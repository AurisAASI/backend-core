import json
import math
import os
import time
import uuid
from pathlib import Path
from typing import Dict, List, Optional

import boto3
import requests
from aws_lambda_powertools import Logger

from src.models.scrappers import BaseScrapper
from src.shared.settings import settings

try:
    from auris_tools.databaseHandlers import DatabaseHandler
except ImportError:
    # Fallback for development/testing without auris_tools
    DatabaseHandler = None

# Configure logger
logger = Logger(service='gmaps-scraper')

# Load company schema template
COMPANY_SCHEMA_PATH = (
    Path(__file__).parent.parent.parent / 'shared' / 'schema' / 'company_schema.json'
)
with open(COMPANY_SCHEMA_PATH, 'r') as f:
    COMPANY_SCHEMA = json.load(f)

# Google Places API (New) constants
PLACES_TEXT_SEARCH_URL = 'https://places.googleapis.com/v1/places:searchText'
PLACES_DETAILS_URL = 'https://places.googleapis.com/v1/places'
# New API pricing (approximate - verify with Google's current pricing)
TEXT_SEARCH_QUOTA_COST = 32  # Text Search (New) Basic
PLACE_DETAILS_QUOTA_COST = 17  # Place Details Basic
DUPLICATE_DISTANCE_THRESHOLD_METERS = 50


class GMapsScrapper(BaseScrapper):
    """
    Creates the AASI company web scrapping to collect general information.
    This class collects AASI company places and public information from Google Places API.

    Args:
        niche (str): The business niche (e.g., 'aasi', 'orl', 'geria', 'audiologist')
        api_key (str): Google Places API key
        daily_quota_limit (int): Daily API quota limit (default: 20000)
    """

    def __init__(self, niche: str, api_key: str, daily_quota_limit: int = 20000):
        """Initialize the GMapsScrapper with niche and API credentials."""
        super().__init__()

        # Set up default boto3 session with explicit region
        region = os.environ.get('AWS_REGION_NAME', settings.region)
        boto3.setup_default_session(region_name=region)
        logger.info(f'boto3 default session configured for region: {region}')

        self.niche = niche.lower()
        self.api_key = api_key
        self.daily_quota_limit = daily_quota_limit
        self.quota_used = 0
        self.ensamble = {
            'places': [],
            'status': 'in_progress',
            'status_reason': '',
            'quota_used': 0,
            'stats': {
                'text_searches': 0,
                'details_fetched': 0,
                'duplicates_by_id': 0,
                'duplicates_by_location': 0,
                'new_places': 0,
                'updated_places': 0,
                'skipped_places': 0,
                'website_tasks_queued': 0,
            },
        }

        # Initialize database handler if available
        if DatabaseHandler:
            try:
                self.db_handler = DatabaseHandler(
                    table_name=settings.get_table_name('places')
                )
                logger.info(
                    f'Database handler initialized for table: {settings.get_table_name("places")}'
                )
            except Exception as e:
                logger.error(f'Failed to initialize database handler: {str(e)}')
                self.db_handler = None
        else:
            self.db_handler = None
            logger.warning(
                'DatabaseHandler not available, database operations will be skipped'
            )

        # Load search terms from JSON file
        self.search_terms = self._load_search_terms()

        # Log configuration
        logger.info(
            f'GMapsScrapper initialized for niche: {self.niche}, '
            f'search terms: {len(self.search_terms)}, '
            f'daily quota limit: {self.daily_quota_limit}, '
            f'stage: {settings.stage}, '
            f'companies_table: {settings.companies_table_name}, '
            f'places_table: {settings.places_table_name}'
        )

    def _load_search_terms(self) -> List[str]:
        """Load search terms from niche_terms.json file."""
        try:
            terms_file = os.path.join(os.path.dirname(__file__), 'niche_terms.json')
            with open(terms_file, 'r', encoding='utf-8') as f:
                terms_data = json.load(f)

            terms = terms_data.get(self.niche, [])
            if not terms:
                logger.warning(f'No search terms found for niche: {self.niche}')
                return []

            logger.info(f'Loaded {len(terms)} search terms for niche: {self.niche}')
            return terms

        except Exception as e:
            logger.error(f'Error loading search terms: {str(e)}')
            return []

    def _convert_place_to_legacy_format(self, place: Dict) -> Dict:
        """
        Convert new Google Places API format to legacy format for compatibility.

        Args:
            place: Place data in new API format

        Returns:
            Place data in legacy format
        """
        legacy_place = {}

        # Place ID - keep as 'id' for consistency
        if 'id' in place:
            legacy_place['id'] = place['id']

        # Name
        if 'displayName' in place:
            if isinstance(place['displayName'], dict):
                legacy_place['name'] = place['displayName'].get('text', '')
            else:
                legacy_place['name'] = place['displayName']

        # Address
        if 'formattedAddress' in place:
            legacy_place['formatted_address'] = place['formattedAddress']

        # Geometry and Location
        if 'location' in place:
            location = place['location']
            legacy_place['geometry'] = {
                'location': {
                    'lat': location.get('latitude'),
                    'lng': location.get('longitude'),
                }
            }

        # Rating
        if 'rating' in place:
            legacy_place['rating'] = place['rating']

        # User ratings total
        if 'userRatingCount' in place:
            legacy_place['user_ratings_total'] = place['userRatingCount']

        # Phone numbers
        if 'nationalPhoneNumber' in place:
            legacy_place['formatted_phone_number'] = place['nationalPhoneNumber']
        if 'internationalPhoneNumber' in place:
            legacy_place['international_phone_number'] = place[
                'internationalPhoneNumber'
            ]

        # Website
        if 'websiteUri' in place:
            legacy_place['website'] = place['websiteUri']

        # Google Maps URL
        if 'googleMapsUri' in place:
            legacy_place['url'] = place['googleMapsUri']

        # Opening hours
        if 'currentOpeningHours' in place:
            opening_hours = place['currentOpeningHours']
            legacy_place['opening_hours'] = {
                'open_now': opening_hours.get('openNow', False),
                'weekday_text': opening_hours.get('weekdayDescriptions', []),
            }
        elif 'regularOpeningHours' in place:
            opening_hours = place['regularOpeningHours']
            legacy_place['opening_hours'] = {
                'weekday_text': opening_hours.get('weekdayDescriptions', []),
            }

        # Business status
        if 'businessStatus' in place:
            legacy_place['business_status'] = place['businessStatus']

        # Types
        if 'types' in place:
            legacy_place['types'] = place['types']

        # Photos
        if 'photos' in place:
            legacy_place['photos'] = place['photos']

        # Reviews
        if 'reviews' in place:
            legacy_place['reviews'] = place['reviews']

        # Price level
        if 'priceLevel' in place:
            legacy_place['price_level'] = place['priceLevel']

        return legacy_place

    def _calculate_distance(
        self, lat1: float, lng1: float, lat2: float, lng2: float
    ) -> float:
        """
        Calculate distance between two coordinates using Haversine formula.

        Args:
            lat1, lng1: First coordinate
            lat2, lng2: Second coordinate

        Returns:
            Distance in meters
        """
        # Earth's radius in meters
        R = 6371000

        # Convert to radians
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lng = math.radians(lng2 - lng1)

        # Haversine formula
        a = (
            math.sin(delta_lat / 2) ** 2
            + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lng / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        distance = R * c
        return distance

    def _is_duplicate_location(
        self, new_lat: float, new_lng: float, existing_places: List[Dict]
    ) -> Optional[Dict]:
        """
        Check if a location is duplicate based on proximity.

        Args:
            new_lat: Latitude of new place
            new_lng: Longitude of new place
            existing_places: List of existing places to check against

        Returns:
            Existing place dict if duplicate found within threshold, None otherwise
        """
        for place in existing_places:
            geometry = place.get('geometry', {})
            location = geometry.get('location', {})
            existing_lat = location.get('lat')
            existing_lng = location.get('lng')

            if existing_lat is None or existing_lng is None:
                continue

            distance = self._calculate_distance(
                new_lat, new_lng, existing_lat, existing_lng
            )

            if distance <= DUPLICATE_DISTANCE_THRESHOLD_METERS:
                logger.info(
                    f'Duplicate location found: {place.get("name")} - '
                    f'Distance: {distance:.2f}m'
                )
                return place

        return None

    def _check_quota(self, required_quota: int) -> bool:
        """
        Check if we have enough quota remaining.

        Args:
            required_quota: Quota units required for next operation

        Returns:
            True if quota available, False otherwise
        """
        if self.quota_used + required_quota > self.daily_quota_limit:
            logger.warning(
                f'Quota limit reached: {self.quota_used}/{self.daily_quota_limit}'
            )
            self.ensamble['status'] = 'partial_quota_exceeded'
            self.ensamble['status_reason'] = (
                f'API quota limit reached at {self.quota_used} units out of '
                f'{self.daily_quota_limit}'
            )
            return False

        # Log warnings at quota thresholds
        quota_percentage = (
            (self.quota_used + required_quota) / self.daily_quota_limit
        ) * 100
        if (
            quota_percentage >= 90
            and (self.quota_used / self.daily_quota_limit) * 100 < 90
        ):
            logger.warning(
                f'API quota at 90%: {self.quota_used + required_quota}/{self.daily_quota_limit}'
            )
        elif (
            quota_percentage >= 80
            and (self.quota_used / self.daily_quota_limit) * 100 < 80
        ):
            logger.info(
                f'API quota at 80%: {self.quota_used + required_quota}/{self.daily_quota_limit}'
            )

        return True

    def _search_places_text_search(
        self, query: str, city: str, state: str
    ) -> List[Dict]:
        """
        Search for places using Google Places API (New) Text Search.

        Args:
            query: Search term
            city: City name
            state: State abbreviation

        Returns:
            List of place dictionaries from API (converted to legacy format)
        """
        all_results = []
        next_page_token = None
        page_count = 0

        full_query = f'{query} em {city}, {state}, Brasil'
        logger.info(f'Starting text search for: {full_query}')

        while True:
            # Check quota before making request
            if not self._check_quota(TEXT_SEARCH_QUOTA_COST):
                logger.warning(
                    f'Stopping text search due to quota limit. '
                    f'Pages processed: {page_count}'
                )
                break

            try:
                headers = {
                    'Content-Type': 'application/json',
                    'X-Goog-Api-Key': self.api_key,
                    'X-Goog-FieldMask': (
                        'places.id,places.displayName,places.formattedAddress,'
                        'places.location,places.rating,places.userRatingCount,'
                        'places.businessStatus,places.types,places.photos,'
                        'places.currentOpeningHours,places.nationalPhoneNumber,'
                        'places.internationalPhoneNumber,places.websiteUri,'
                        'places.googleMapsUri,places.priceLevel,nextPageToken'
                    ),
                }

                body = {
                    'textQuery': full_query,
                    'languageCode': 'pt-BR',
                }

                if next_page_token:
                    body['pageToken'] = next_page_token
                    # Google requires 2-second delay before using page token
                    time.sleep(2)

                response = requests.post(
                    PLACES_TEXT_SEARCH_URL, json=body, headers=headers, timeout=10
                )
                response.raise_for_status()
                data = response.json()

                # Update quota
                self.quota_used += TEXT_SEARCH_QUOTA_COST
                self.ensamble['quota_used'] = self.quota_used
                self.ensamble['stats']['text_searches'] += 1
                page_count += 1

                places = data.get('places', [])
                if not places:
                    logger.info(f'No results found for query: {full_query}')
                    break

                # Convert new API format to legacy format for compatibility
                converted_results = []
                for place in places:
                    converted_place = self._convert_place_to_legacy_format(place)
                    converted_results.append(converted_place)

                all_results.extend(converted_results)
                logger.info(
                    f'Text search page {page_count}: found {len(converted_results)} places '
                    f'(total: {len(all_results)})'
                )

                # Check for next page
                next_page_token = data.get('nextPageToken')
                if not next_page_token:
                    break

            except requests.exceptions.RequestException as e:
                logger.error(f'Request error during text search: {str(e)}')
                self.ensamble['status'] = 'failed_api_error'
                self.ensamble[
                    'status_reason'
                ] = f'Text search API request failed: {str(e)}'
                break
            except Exception as e:
                logger.error(f'Unexpected error during text search: {str(e)}')
                break

        logger.info(
            f'Text search completed for "{query}": {len(all_results)} results, '
            f'{page_count} pages, quota used: {self.quota_used}'
        )
        return all_results

    def _get_place_details(self, place_id: str) -> Optional[Dict]:
        """
        Get detailed information for a place using Google Places API (New) Place Details.

        Args:
            place_id: Google Place ID

        Returns:
            Detailed place information dict (in legacy format) or None on error
        """
        # Check quota before making request
        if not self._check_quota(PLACE_DETAILS_QUOTA_COST):
            logger.warning(f'Skipping place details for {place_id} due to quota limit')
            return None

        try:
            headers = {
                'Content-Type': 'application/json',
                'X-Goog-Api-Key': self.api_key,
                'X-Goog-FieldMask': (
                    'id,displayName,formattedAddress,location,rating,'
                    'userRatingCount,nationalPhoneNumber,internationalPhoneNumber,'
                    'websiteUri,googleMapsUri,currentOpeningHours,regularOpeningHours,'
                    'businessStatus,types,photos,reviews,priceLevel'
                ),
            }

            # New API uses resource name format: places/{PLACE_ID}
            url = f'{PLACES_DETAILS_URL}/{place_id}'

            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            place_data = response.json()

            # Update quota
            self.quota_used += PLACE_DETAILS_QUOTA_COST
            self.ensamble['quota_used'] = self.quota_used
            self.ensamble['stats']['details_fetched'] += 1

            # Convert to legacy format for compatibility
            result = self._convert_place_to_legacy_format(place_data)
            logger.debug(
                f'Fetched details for place: {result.get("name")} (ID: {place_id})'
            )
            return result

        except requests.exceptions.RequestException as e:
            logger.error(
                f'Request error fetching place details for {place_id}: {str(e)}'
            )
            return None
        except Exception as e:
            logger.error(
                f'Unexpected error fetching place details for {place_id}: {str(e)}'
            )
            return None

    def _queue_website_scraping_task(
        self, company_id: str, website: str, city: str, state: str
    ) -> None:
        """
        Send SQS message to queue website scraping task.

        Args:
            company_id: Company UUID
            website: Company website URL
            city: City name
            state: State abbreviation
        """
        try:
            queue_url = settings.website_scraper_task_queue_url
            if not queue_url:
                logger.warning(
                    'WEBSITE_SCRAPER_TASK_QUEUE_URL not configured, skipping website scraping'
                )
                return

            region = os.environ.get('AWS_REGION_NAME', settings.region)
            sqs_client = boto3.client('sqs', region_name=region)
            message_body = json.dumps(
                {
                    'company_id': company_id,
                    'website': website,
                }
            )

            response = sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=message_body,
            )

            self.ensamble['stats']['website_tasks_queued'] += 1
            logger.info(
                f'Queued website scraping task for {website} '
                f'(Company: {company_id}, Message ID: {response["MessageId"]})'
            )

        except Exception as e:
            logger.error(
                f'Failed to queue website scraping task for {company_id}: {str(e)}'
            )
            # Don't fail the entire scraping process if SQS fails

    def _save_to_database(self, city: str, state: str) -> bool:
        """
        Save collected places to DynamoDB tables.

        Args:
            city: City name
            state: State abbreviation

        Returns:
            True if successful, False otherwise
        """
        if not self.db_handler:
            logger.warning('Database handler not available, skipping database save')
            return False

        logger.info(f'Starting database save for {len(self.ensamble["places"])} places')

        try:
            for place in self.ensamble['places']:
                try:
                    place_id = place.get('id')
                    if not place_id:
                        logger.warning('Place missing id, skipping')
                        continue

                    geometry = place.get('geometry', {})
                    location = geometry.get('location', {})
                    lat = location.get('lat')
                    lng = location.get('lng')

                    if lat is None or lng is None:
                        logger.warning(
                            f'Place {place_id} missing coordinates, skipping'
                        )
                        continue

                    # Check if place already exists in database
                    existing_place = None
                    try:
                        existing_place = self.db_handler.get_item(
                            key={'placeID': place_id},
                        )
                    except Exception as e:
                        logger.debug(
                            f'Place {place_id} not found in database: {str(e)}'
                        )

                    if existing_place:
                        # Check if data has changed
                        needs_update = False
                        for key, value in place.items():
                            if existing_place.get(key) != value:
                                needs_update = True
                                break

                        if needs_update:
                            # Update existing place
                            self.db_handler.update_item(
                                key={'placeID': place_id},
                                updates=place,
                                primary_key='placeID',
                            )
                            self.ensamble['stats']['updated_places'] += 1
                            logger.info(
                                f'Updated place: {place.get("name")} (ID: {place_id})'
                            )
                        else:
                            self.ensamble['stats']['skipped_places'] += 1
                            logger.debug(
                                f'No changes for place: {place.get("name")} (ID: {place_id})'
                            )
                        continue

                    # New place - generate company ID and insert
                    company_id = 'company-' + str(uuid.uuid4())

                    # Build company data using schema template
                    company_data = COMPANY_SCHEMA.copy()
                    company_data['companyID'] = company_id
                    company_data['users'] = []
                    company_data['name'] = place.get('name')
                    company_data['city'] = city
                    company_data['state'] = state
                    company_data['niche'] = self.niche
                    company_data['collection_status'] = self.ensamble['status']
                    company_data['collection_reason'] = self.ensamble['status_reason']

                    # Remove empty strings
                    company_data = {k: v for k, v in company_data.items() if v != ''}

                    # Note: This requires a separate DatabaseHandler instance for companies table
                    companies_db = DatabaseHandler(
                        table_name=settings.get_table_name('companies')
                    )
                    companies_db.insert_item(
                        item=company_data,
                        primary_key='companyID',
                    )

                    # Insert place record with companyID link
                    # Exclude 'id' from place data since we store it as 'placeID'
                    place_without_id = {k: v for k, v in place.items() if k != 'id'}
                    place_data = {
                        'placeID': place_id,
                        'companyID': company_id,
                        **place_without_id,
                    }

                    self.db_handler.insert_item(item=place_data, primary_key='placeID')

                    self.ensamble['stats']['new_places'] += 1
                    logger.info(
                        f'Inserted new place: {place.get("name")} '
                        f'(Place ID: {place_id}, Company ID: {company_id})'
                    )

                    # Queue website scraping task if website is present
                    website = place.get('website')
                    if website:
                        self._queue_website_scraping_task(
                            company_id=company_id,
                            website=website,
                            city=city,
                            state=state,
                        )

                except Exception as e:
                    logger.error(
                        f'Error saving individual place {place.get("id")}: {str(e)}'
                    )
                    continue

            logger.info(
                f'Database save completed - New: {self.ensamble["stats"]["new_places"]}, '
                f'Updated: {self.ensamble["stats"]["updated_places"]}, '
                f'Skipped: {self.ensamble["stats"]["skipped_places"]}'
            )
            return True

        except Exception as e:
            logger.error(f'Error during database save: {str(e)}')
            self.ensamble['status'] = 'failed_database_error'
            self.ensamble['status_reason'] = f'Database save failed: {str(e)}'
            return False

    def collect_data(self, city: str, state: str):
        """
        Collects Google API Places list based on the niche.
        For each niche, there are a configured set of search terms.

        This method performs:
        1. Text search for each term with pagination
        2. Enrichment with place details
        3. Deduplication by place_id and location proximity
        4. Save to DynamoDB with quota management

        Args:
            city (str): City name
            state (str): State abbreviation
        """
        logger.info(
            f'Starting place collection for {city}, {state} - '
            f'Niche: {self.niche}, Terms: {len(self.search_terms)}'
        )

        if not self.search_terms:
            logger.error(f'No search terms available for niche: {self.niche}')
            self.ensamble['status'] = 'failed_no_search_terms'
            self.ensamble[
                'status_reason'
            ] = f'No search terms configured for niche: {self.niche}'
            return

        # Track unique place IDs to avoid duplicates
        seen_place_ids = set()

        # Iterate through each search term
        for idx, term in enumerate(self.search_terms, 1):
            logger.info(
                f'Processing search term {idx}/{len(self.search_terms)}: "{term}"'
            )

            # Perform text search
            text_results = self._search_places_text_search(term, city, state)

            # Process each result
            for result in text_results:
                place_id = result.get('id')
                if not place_id:
                    continue

                # Check for id duplicate
                if place_id in seen_place_ids:
                    self.ensamble['stats']['duplicates_by_id'] += 1
                    logger.debug(f'Duplicate place_id: {place_id}')
                    continue

                # Check for location-based duplicate
                geometry = result.get('geometry', {})
                location = geometry.get('location', {})
                lat = location.get('lat')
                lng = location.get('lng')

                if lat is not None and lng is not None:
                    duplicate = self._is_duplicate_location(
                        lat, lng, self.ensamble['places']
                    )
                    if duplicate:
                        self.ensamble['stats']['duplicates_by_location'] += 1
                        continue

                # Mark as seen
                seen_place_ids.add(place_id)

                # Enrich with place details if quota allows
                detailed_info = self._get_place_details(place_id)
                if detailed_info:
                    # Merge text search and detailed info
                    merged_place = {**result, **detailed_info}
                else:
                    # Use text search data only
                    merged_place = result

                # Add to collection
                self.ensamble['places'].append(merged_place)
                logger.debug(
                    f'Added place: {merged_place.get("name")} (ID: {place_id})'
                )

            # Check if we should continue (quota exceeded)
            if self.ensamble['status'] == 'partial_quota_exceeded':
                logger.warning(
                    f'Stopping collection after term {idx}/{len(self.search_terms)} '
                    f'due to quota limit'
                )
                break

            # Rate limiting between search terms
            if idx < len(self.search_terms):
                time.sleep(1)

        # Log collection summary
        logger.info(
            f'Place collection completed - '
            f'Total places: {len(self.ensamble["places"])}, '
            f'Unique place IDs: {len(seen_place_ids)}, '
            f'Duplicates (place_id): {self.ensamble["stats"]["duplicates_by_id"]}, '
            f'Duplicates (location): {self.ensamble["stats"]["duplicates_by_location"]}, '
            f'Quota used: {self.quota_used}/{self.daily_quota_limit}'
        )

        # Save to database
        if self.ensamble['places']:
            save_success = self._save_to_database(city, state)
            if save_success and self.ensamble['status'] == 'in_progress':
                self.ensamble['status'] = 'completed'
                self.ensamble['status_reason'] = 'Collection completed successfully'
        else:
            logger.warning('No places collected to save')
            if self.ensamble['status'] == 'in_progress':
                self.ensamble['status'] = 'completed_no_results'
                self.ensamble['status_reason'] = 'No places found for search terms'

        logger.info(
            f'Final status: {self.ensamble["status"]} - '
            f'{self.ensamble["status_reason"]}'
        )
