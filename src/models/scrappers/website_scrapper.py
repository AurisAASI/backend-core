"""
Website Scrapper for extracting structured company data from websites.

This module provides HTML-only scraping with LLM-powered data extraction
using Google Gemini API to structure information from company websites.
"""

import json
import os
import random
import re
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import boto3
import requests
from auris_tools.databaseHandlers import DatabaseHandler
from auris_tools.geminiHandler import GoogleGeminiHandler
from aws_lambda_powertools import Logger
from bs4 import BeautifulSoup

from src.models.scrappers import BaseScrapper
from src.shared.settings import settings

# Configure logger
logger = Logger(service='website-scraper')

# Constants
MAX_PAGES_PER_SITE = 15
REQUEST_TIMEOUT = 10
USER_AGENT = 'AurisBot/1.0 (+https://auris.com.br/bot)'
MIN_DELAY_SECONDS = 2
MAX_DELAY_SECONDS = 3


class WebsiteScrapper(BaseScrapper):
    """
    Scrapes and extracts structured company data from websites using LLM.

    This class fetches HTML content from company websites, extracts relevant
    information using Google Gemini AI, and saves enriched data to DynamoDB.

    Args:
        company_id (str): UUID of the company in the database
        website (str): Company website URL
        gemini_api_key (str): Google Gemini API key for LLM extraction
        timeout (int): HTTP request timeout in seconds (default: 10)
    """

    def __init__(
        self,
        company_id: str,
        website: str,
        gemini_api_key: str,
        timeout: int = REQUEST_TIMEOUT,
    ):
        """Initialize the WebsiteScrapper."""
        super().__init__()
        self.company_id = company_id
        self.website = self._normalize_url(website)
        self.gemini_api_key = gemini_api_key
        self.timeout = timeout
        self.ensamble = {
            'status': 'in_progress',
            'status_reason': '',
            'pages_fetched': 0,
            'pages_failed': 0,
            'data_extracted': {},
        }

        # Initialize database handler
        try:
            self.db_handler = DatabaseHandler(table_name=settings.get_table_name('companies'))
        except Exception as e:
            logger.warning(f'DatabaseHandler initialization failed: {str(e)}')
            self.db_handler = None

        # Initialize Gemini handler
        try:
            self.gemini_handler = GoogleGeminiHandler(api_key=gemini_api_key)
        except Exception as e:
            logger.error(f'GoogleGeminiHandler initialization failed: {str(e)}')
            raise

        logger.info(
            f'WebsiteScrapper initialized - Company: {company_id}, '
            f'Website: {self.website}, Stage: {settings.stage}'
        )

    def _normalize_url(self, url: str) -> str:
        """
        Normalize and validate URL format.

        Args:
            url: Raw URL string

        Returns:
            Normalized URL with scheme

        Raises:
            ValueError: If URL is invalid
        """
        url = url.strip()

        # Basic validation before processing
        if not url or ' ' in url:
            raise ValueError(f'Invalid URL format: {url}')

        # Add scheme if missing
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url

        # Parse and validate
        parsed = urlparse(url)
        if not parsed.netloc:
            raise ValueError(f'Invalid URL format: {url}')

        return url

    def _check_robots_txt(self, base_url: str) -> bool:
        """
        Check if scraping is allowed by robots.txt.

        Args:
            base_url: Base URL of the website

        Returns:
            True if allowed, False otherwise
        """
        try:
            robots_url = urljoin(base_url, '/robots.txt')
            rp = RobotFileParser()
            rp.set_url(robots_url)

            # Read robots.txt - this fetches it from the URL
            try:
                rp.read()
            except Exception as read_error:
                logger.warning(
                    f'Could not read robots.txt from {robots_url}: {str(read_error)}'
                )
                # If we can't read robots.txt, assume it's okay to proceed
                return True

            # Check if our user agent can fetch the base URL
            can_fetch = rp.can_fetch(USER_AGENT, base_url)
            logger.info(
                f'robots.txt check for {base_url}: {"allowed" if can_fetch else "disallowed"}'
            )
            return can_fetch
        except Exception as e:
            logger.warning(f'Failed to check robots.txt for {base_url}: {str(e)}')
            # If we can't read robots.txt, assume it's okay to proceed
            return True

    def _discover_pages(self, base_url: str) -> List[str]:
        """
        Discover main pages to scrape from the website using hybrid strategy.

        This method:
        1. Collects all available page URLs from multiple strategies
        2. Validates that each URL is actually accessible
        3. Ranks pages by HTML content size and selects top MAX_PAGES_PER_SITE

        Strategies used:
        - Homepage navigation links
        - Sitemap.xml parsing
        - Common path patterns

        Args:
            base_url: Base URL of the website

        Returns:
            List of validated page URLs ranked by content size
        """
        logger.info(f'Starting page discovery for {base_url}')
        
        # Step 1: Collect all candidate URLs from all strategies
        candidate_urls = set()
        
        # Strategy 1: Homepage navigation links
        logger.info('Strategy 1: Discovering pages from homepage navigation')
        homepage_pages = self._discover_pages_from_homepage(base_url)
        candidate_urls.update(homepage_pages)
        logger.info(f'Found {len(homepage_pages)} pages from homepage navigation')
        
        # Strategy 2: Sitemap.xml
        logger.info('Strategy 2: Discovering pages from sitemap.xml')
        sitemap_pages = self._discover_pages_from_sitemap(base_url)
        candidate_urls.update(sitemap_pages)
        logger.info(f'Found {len(sitemap_pages)} pages from sitemap')
        
        # Strategy 3: Common paths
        logger.info('Strategy 3: Discovering pages from common paths')
        common_pages = self._discover_pages_common_paths(base_url)
        candidate_urls.update(common_pages)
        logger.info(f'Found {len(common_pages)} pages from common paths')
        
        logger.info(f'Total unique candidate URLs: {len(candidate_urls)}')
        
        # Step 2: Validate URLs and collect content size
        valid_pages = []
        for idx, url in enumerate(candidate_urls):
            # Rate limiting between validation requests
            if idx > 0 and idx % 5 == 0:
                delay = random.uniform(0.5, 1.5)
                time.sleep(delay)
            
            logger.debug(f'Validating URL ({idx + 1}/{len(candidate_urls)}): {url}')
            html_content = self._fetch_page_content(url)
            
            if html_content:
                content_size = len(html_content)
                valid_pages.append((url, content_size))
                logger.debug(f'Valid URL with {content_size} bytes: {url}')
            else:
                logger.debug(f'Skipping invalid/unreachable URL: {url}')
        
        logger.info(f'Validated {len(valid_pages)} accessible pages out of {len(candidate_urls)} candidates')
        
        # Step 3: Rank by content size and select top MAX_PAGES_PER_SITE
        if not valid_pages:
            logger.warning('No valid pages found')
            return []
        
        # Sort by content size (descending) - pages with more content first
        valid_pages.sort(key=lambda x: x[1], reverse=True)
        
        # Select top MAX_PAGES_PER_SITE pages
        selected_pages = [url for url, size in valid_pages[:MAX_PAGES_PER_SITE]]
        
        logger.info(
            f'Selected top {len(selected_pages)} pages by content size '
            f'(range: {valid_pages[-1][1] if len(valid_pages) > 0 else 0} - '
            f'{valid_pages[0][1]} bytes)'
        )
        
        return selected_pages
        

    def _discover_pages_from_sitemap(self, base_url: str) -> List[str]:
        """
        Discover pages by parsing sitemap.xml.

        Args:
            base_url: Base URL of the website

        Returns:
            List of discovered page URLs, empty if sitemap not found
        """
        sitemap_paths = [
            '/sitemap.xml',
            '/sitemap_index.xml',
            '/sitemap-index.xml',
            '/sitemap1.xml',
        ]

        for sitemap_path in sitemap_paths:
            try:
                sitemap_url = urljoin(base_url, sitemap_path)
                response = requests.get(
                    sitemap_url,
                    headers={'User-Agent': USER_AGENT},
                    timeout=self.timeout,
                    verify=True,
                )

                if response.status_code == 200:
                    # Parse XML sitemap
                    soup = BeautifulSoup(response.content, 'xml')
                    urls = [loc.text.strip() for loc in soup.find_all('loc')]

                    if urls:
                        logger.debug(
                            f'Found sitemap at {sitemap_url} with {len(urls)} URLs'
                        )
                        # Filter and prioritize main pages
                        filtered_urls = self._filter_main_pages(urls)
                        return filtered_urls

            except Exception as e:
                logger.debug(f'Failed to fetch sitemap {sitemap_path}: {str(e)}')
                continue

        return []

    def _filter_main_pages(self, urls: List[str]) -> List[str]:
        """
        Filter sitemap URLs to prioritize main informational pages.

        Args:
            urls: List of URLs from sitemap

        Returns:
            Filtered and prioritized list of URLs
        """
        # Keywords that indicate important pages
        priority_keywords = [
            'about',
            'sobre',
            'quem-somos',
            'contact',
            'contato',
            'fale-conosco',
            'service',
            'servico',
            'servicos',
            'product',
            'produto',
            'produtos',
            'empresa',
            'company',
            'historia',
        ]

        # Patterns to exclude (blog posts, pagination, etc.)
        exclude_patterns = [
            r'/blog/',
            r'/news/',
            r'/noticia/',
            r'/artigo/',
            r'/page/\d+',
            r'/p/\d+',
            r'/\d{4}/\d{2}/',
            r'/category/',
            r'/tag/',
            r'/author/',
            r'\?',
            r'#',  # Query params and anchors
        ]

        filtered = []
        priority_urls = []

        for url in urls:
            # Skip if matches exclude patterns
            if any(
                re.search(pattern, url, re.IGNORECASE) for pattern in exclude_patterns
            ):
                continue

            # Check if it's a priority page
            path = urlparse(url).path.lower()
            if any(kw in path for kw in priority_keywords):
                priority_urls.append(url)
            else:
                filtered.append(url)

        # Return priority pages first, then others
        return priority_urls + filtered

    def _discover_pages_from_homepage(self, base_url: str) -> List[str]:
        """
        Discover pages by crawling homepage navigation links.

        Args:
            base_url: Base URL of the website

        Returns:
            List of discovered page URLs from navigation
        """
        try:
            html = self._fetch_page_content(base_url)
            if not html:
                return []

            soup = BeautifulSoup(html, 'html.parser')
            parsed_base = urlparse(base_url)
            base_domain = f'{parsed_base.scheme}://{parsed_base.netloc}'

            # Find links in navigation, header, footer, and main menu areas
            nav_areas = soup.find_all(['nav', 'header', 'footer', 'menu'])
            # Also check for common nav class names
            nav_areas.extend(
                soup.find_all(class_=re.compile(r'nav|menu|header', re.IGNORECASE))
            )

            links = set()

            for area in nav_areas:
                for a_tag in area.find_all('a', href=True):
                    href = a_tag['href'].strip()

                    # Skip empty, anchor-only, or javascript links
                    if not href or href.startswith(
                        ('#', 'javascript:', 'mailto:', 'tel:')
                    ):
                        continue

                    # Convert relative to absolute URL
                    full_url = urljoin(base_domain, href)
                    parsed_url = urlparse(full_url)

                    # Only include same-domain links
                    if parsed_url.netloc == parsed_base.netloc:
                        # Clean URL (remove anchors and query params for deduplication)
                        clean_url = f'{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}'
                        clean_url = clean_url.rstrip('/')

                        if clean_url and clean_url not in links:
                            links.add(clean_url)

            # Convert to list and prioritize
            links_list = list(links)
            if links_list:
                prioritized = self._prioritize_links(links_list)
                logger.debug(f'Found {len(prioritized)} navigation links from homepage')
                return prioritized

            return []

        except Exception as e:
            logger.warning(f'Failed to crawl homepage navigation: {str(e)}')
            return []

    def _prioritize_links(self, links: List[str]) -> List[str]:
        """
        Sort links by relevance based on keywords in URL path.

        Args:
            links: List of URLs to prioritize

        Returns:
            Sorted list with most relevant URLs first
        """
        priority_keywords = [
            'sobre',
            'about',
            'quem-somos',
            'contato',
            'contact',
            'fale-conosco',
            'servico',
            'service',
            'servicos',
            'produto',
            'product',
            'produtos',
            'empresa',
            'company',
        ]

        def score_url(url):
            path = urlparse(url).path.lower()
            # Count keyword matches
            matches = sum(1 for kw in priority_keywords if kw in path)
            # Prefer shorter paths (closer to root)
            depth_penalty = path.count('/') * 0.1
            return matches - depth_penalty

        return sorted(links, key=score_url, reverse=True)

    def _discover_pages_common_paths(self, base_url: str) -> List[str]:
        """
        Fallback: discover pages using common path patterns.

        Args:
            base_url: Base URL of the website

        Returns:
            List of page URLs based on common patterns
        """
        parsed = urlparse(base_url)
        base_domain = f'{parsed.scheme}://{parsed.netloc}'

        # Common page paths to check
        common_paths = [
            '/',
            '/index.html',
            '/index.php',
            '/sobre',
            '/sobre-nos',
            '/quem-somos',
            '/about',
            '/about-us',
            '/contato',
            '/fale-conosco',
            '/contact',
            '/produtos',
            '/products',
            '/servicos',
            '/services',
            '/ofertas',
            '/offers',
            '/empresa',
            '/company',
        ]

        pages_to_fetch = []
        for path in common_paths:
            url = urljoin(base_domain, path)
            if url not in pages_to_fetch:
                pages_to_fetch.append(url)

        # Limit to MAX_PAGES_PER_SITE
        return pages_to_fetch[:MAX_PAGES_PER_SITE]

    def _fetch_page_content(self, url: str) -> Optional[str]:
        """
        Fetch HTML content from a URL with politeness measures.

        Args:
            url: URL to fetch

        Returns:
            HTML content as string, or None if failed
        """
        try:
            headers = {
                'User-Agent': USER_AGENT,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
                'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            }

            response = requests.get(
                url,
                headers=headers,
                timeout=self.timeout,
                allow_redirects=True,
                verify=True,
            )

            # Check status code
            if response.status_code == 200:
                self.ensamble['pages_fetched'] += 1
                logger.debug(f'Successfully fetched: {url}')
                return response.text
            else:
                logger.warning(f'Failed to fetch {url}: HTTP {response.status_code}')
                self.ensamble['pages_failed'] += 1
                return None

        except requests.exceptions.Timeout:
            logger.warning(f'Timeout fetching {url}')
            self.ensamble['pages_failed'] += 1
            return None
        except requests.exceptions.RequestException as e:
            logger.warning(f'Request error fetching {url}: {str(e)}')
            self.ensamble['pages_failed'] += 1
            return None
        except Exception as e:
            logger.error(f'Unexpected error fetching {url}: {str(e)}')
            self.ensamble['pages_failed'] += 1
            return None

    def _extract_text_from_html(self, html: str) -> str:
        """
        Extract clean text from HTML using BeautifulSoup.

        Args:
            html: Raw HTML content

        Returns:
            Cleaned text content
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')

            # Remove script and style elements
            for script_or_style in soup(['script', 'style', 'noscript']):
                script_or_style.decompose()

            # Get text
            text = soup.get_text(separator='\n', strip=True)

            # Clean up whitespace
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split('  '))
            text = '\n'.join(chunk for chunk in chunks if chunk)

            return text
        except Exception as e:
            logger.error(f'Error extracting text from HTML: {str(e)}')
            return ''

    def _extract_structured_data(self, pages_content: Dict[str, str]) -> Dict:
        """
        Extract structured data from HTML pages using Google Gemini LLM.

        Args:
            pages_content: Dictionary mapping URL to HTML content

        Returns:
            Dictionary with extracted structured data
        """
        logger.info(
            f'Extracting structured data using Gemini LLM from {len(pages_content)} pages'
        )

        # Combine all page texts
        all_text = []
        for url, html in pages_content.items():
            text = self._extract_text_from_html(html)
            if text:
                all_text.append(
                    f'=== Page: {url} ===\n{text[:20000]}'
                )  # Limit per page

        combined_text = '\n\n'.join(all_text)[:300000]  # Limit total to ~300k chars

        if not combined_text.strip():
            logger.warning('No text content extracted from pages')
            return {}

        # Load JSON schema for Gemini response from external file
        schema_path = os.path.join(
            os.path.dirname(__file__), 'website_gemini_schema.json'
        )
        try:
            with open(schema_path, 'r', encoding='utf-8') as f:
                response_schema = json.load(f)
        except FileNotFoundError:
            logger.error(f'Schema file not found: {schema_path}')
            return {}
        except json.JSONDecodeError as e:
            logger.error(f'Invalid JSON in schema file: {str(e)}')
            return {}

        # Construct prompt for Gemini
        prompt = f"""Analyze the following website content and extract structured company information in Brazilian Portuguese context.

Website Content:
{combined_text}

Extract and return ONLY the following information in JSON format (return null for missing fields):

1. **brand_name**: The official company brand name
2. **addresses**: All physical addresses found (parse into structured components: street, number, district, city, state, postal_code)
3. **phones**: All phone numbers (up to 4), classify type as: 'fixed', 'mobile', 'whatsapp', 'fax', or 'other'
4. **history**: Brief company history or about section (max 1500 characters)
5. **products**: List of main products offered
6. **services**: List of main services offered
7. **brands**: List of product brands sold or represented
8. **social_links**: Social media URLs (facebook, instagram, youtube, tiktok, linkedin, twitter, others)
9. **cnpj**: Brazilian company ID (CNPJ) in format XX.XXX.XXX/XXXX-XX if found
10. **offers_summary**: Summary of current offers or promotions (max 1500 characters)

Guidelines:
- Extract information in Brazilian Portuguese
- For addresses: identify street type (Rua, Av., etc.), number, district, city, state abbreviation (SP, RJ, etc.), CEP
- For phones: detect type based on context (WhatsApp, Celular, Fixo, etc.) or digit count (9 digits = mobile)
- Social links: extract full URLs
- Return null for any field that cannot be found
- Be concise and accurate

Return ONLY valid JSON following the schema provided."""

        try:
            # Use Gemini to extract structured data with JSON schema
            logger.info('Calling Gemini API for structured extraction...')
            self.gemini_handler = GoogleGeminiHandler(
                api_key=self.gemini_api_key,
                temperature=0.0,
                response_mime_type='application/json',
                response_schema=response_schema,
                )
            result = self.gemini_handler.generate_output(
                prompt=prompt
            )

            # Parse JSON response
            extracted_data = json.loads(result.text)

            # Log extraction stats
            self.ensamble['data_extracted'] = {
                'brand_name': bool(extracted_data.get('brand_name')),
                'addresses_count': len(extracted_data.get('addresses', [])),
                'phones_count': len(extracted_data.get('phones', [])),
                'has_history': bool(extracted_data.get('history')),
                'products_count': len(extracted_data.get('products', [])),
                'services_count': len(extracted_data.get('services', [])),
                'brands_count': len(extracted_data.get('brands', [])),
                'social_links_count': len(
                    [v for v in extracted_data.get('social_links', {}).values() if v]
                ),
                'has_cnpj': bool(extracted_data.get('cnpj')),
                'has_offers': bool(extracted_data.get('offers_summary')),
            }

            logger.info(
                f'Structured data extracted successfully: {self.ensamble["data_extracted"]}'
            )
            return extracted_data

        except json.JSONDecodeError as e:
            logger.error(f'Failed to parse Gemini JSON response: {str(e)}')
            return {}
        except Exception as e:
            logger.error(f'Error during LLM extraction: {str(e)}')
            return {}

    def _save_to_database(self, website_data: Dict) -> bool:
        """
        Save extracted website data to DynamoDB companies table.

        Args:
            website_data: Extracted structured data

        Returns:
            True if successful, False otherwise
        """
        if not self.db_handler:
            logger.warning('Database handler not available, skipping database save')
            return False

        try:
            logger.info(f'Saving website data for company {self.company_id}')

            # Prepare update data
            update_data = {
                'website_data': website_data,
                'website_scraping_status': self.ensamble['status'],
                'website_scraping_reason': self.ensamble['status_reason'],
                'website_scraped_at': datetime.now(timezone.utc).isoformat(),
            }

            # Update company record
            self.db_handler.update_item(
                key={'companyID': self.company_id},
                updates=update_data,
                primary_key='companyID',
            )

            logger.info(
                f'Successfully saved website data for company {self.company_id}'
            )
            return True

        except Exception as e:
            logger.error(f'Error saving to database: {str(e)}')
            self.ensamble['status'] = 'failed'
            self.ensamble['status_reason'] = f'Database save failed: {str(e)}'
            return False

    def _queue_federal_scraping_task(self, company_id: str, cnpj: str) -> None:
        """
        Send SQS message to queue federal company data scraping task.

        Args:
            company_id: Company UUID
            cnpj: Clean CNPJ number (digits only)
        """
        try:
            queue_url = settings.company_federal_scraper_task_queue_url
            if not queue_url:
                logger.warning(
                    'COMPANY_FEDERAL_SCRAPER_TASK_QUEUE_URL not configured, '
                    'skipping federal scraping'
                )
                return

            region = os.environ.get('AWS_REGION_NAME', settings.region)
            sqs_client = boto3.client('sqs', region_name=region)
            message_body = json.dumps(
                {
                    'company_id': company_id,
                    'cnpj': cnpj,
                }
            )

            response = sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=message_body,
            )

            logger.info(
                f'Queued federal scraping task for CNPJ {cnpj} '
                f'(Company: {company_id}, Message ID: {response["MessageId"]})'
            )

        except Exception as e:
            logger.error(
                f'Failed to queue federal scraping task for {company_id}: {str(e)}'
            )
            # Don't fail the entire scraping process if SQS fails

    def collect_data(self) -> None:
        """
        Main method to orchestrate website scraping and data extraction.

        This method:
        1. Validates the website URL
        2. Checks robots.txt for permission
        3. Discovers and fetches relevant pages
        4. Extracts structured data using LLM
        5. Saves enriched data to database
        """
        logger.info(
            f'Starting website scraping for company {self.company_id}: {self.website}'
        )

        try:
            # Validate URL
            if not self.website:
                raise ValueError('Invalid or empty website URL')

            # Check robots.txt
            if not self._check_robots_txt(self.website):
                logger.warning(f'Scraping disallowed by robots.txt: {self.website}')
                self.ensamble['status'] = 'completed'
                self.ensamble['status_reason'] = 'Scraping disallowed by robots.txt'
                # Still save status to DB
                self._save_to_database({})
                return

            # Discover pages to scrape
            pages_to_fetch = self._discover_pages(self.website)
            logger.info(f'Discovered {len(pages_to_fetch)} pages to fetch')

            # Fetch page contents with rate limiting
            pages_content = {}
            for idx, url in enumerate(pages_to_fetch):
                # Rate limiting: delay between requests
                if idx > 0:
                    delay = random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
                    logger.debug(f'Rate limiting: sleeping {delay:.2f}s')
                    time.sleep(delay)

                html = self._fetch_page_content(url)
                if html:
                    pages_content[url] = html

            logger.info(
                f'Fetched {self.ensamble["pages_fetched"]} pages successfully, '
                f'{self.ensamble["pages_failed"]} failed'
            )

            # Check if we got any content
            if not pages_content:
                logger.warning('No pages could be fetched')
                self.ensamble['status'] = 'partial'
                self.ensamble[
                    'status_reason'
                ] = f'Failed to fetch any pages (tried {len(pages_to_fetch)})'
                self._save_to_database({})
                return

            # Extract structured data using LLM
            website_data = self._extract_structured_data(pages_content)

            # Determine final status
            if website_data:
                if self.ensamble['pages_failed'] > 0:
                    self.ensamble['status'] = 'partial'
                    self.ensamble['status_reason'] = (
                        f'Data extracted from {self.ensamble["pages_fetched"]} pages, '
                        f'{self.ensamble["pages_failed"]} pages failed'
                    )
                else:
                    self.ensamble['status'] = 'completed'
                    self.ensamble[
                        'status_reason'
                    ] = f'Successfully scraped {self.ensamble["pages_fetched"]} pages'
            else:
                self.ensamble['status'] = 'failed'
                self.ensamble[
                    'status_reason'
                ] = 'Failed to extract structured data from pages'

            # Save to database
            save_success = self._save_to_database(website_data)

            if not save_success:
                # Status already updated in _save_to_database
                pass

            # Queue federal scraping if CNPJ was extracted
            if website_data and website_data.get('cnpj'):
                from src.shared.utils import clean_cnpj, validate_cnpj
                
                extracted_cnpj = website_data.get('cnpj')
                cleaned_cnpj = clean_cnpj(extracted_cnpj)
                
                if cleaned_cnpj and validate_cnpj(cleaned_cnpj):
                    logger.info(f'Valid CNPJ found: {extracted_cnpj}, queueing federal scraping')
                    self._queue_federal_scraping_task(self.company_id, cleaned_cnpj)
                else:
                    logger.warning(f'Invalid CNPJ extracted: {extracted_cnpj}, skipping federal scraping')

            logger.info(
                f'Website scraping completed - Status: {self.ensamble["status"]}, '
                f'Reason: {self.ensamble["status_reason"]}'
            )

        except ValueError as e:
            logger.error(f'Validation error: {str(e)}')
            self.ensamble['status'] = 'failed'
            self.ensamble['status_reason'] = f'Invalid URL: {str(e)}'
            self._save_to_database({})
        except Exception as e:
            logger.error(
                f'Unexpected error during website scraping: {str(e)}', exc_info=True
            )
            self.ensamble['status'] = 'failed'
            self.ensamble['status_reason'] = f'Scraping error: {str(e)}'
            self._save_to_database({})
