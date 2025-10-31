from src.models.scrappers.aasi_scrapper import BaseScrapper


class AASIScrapper(BaseScrapper):
    """
    Creates the AASI company web scrapping to collect general information.

    Args:
        BaseScrapper: Abstract class for scrappers
    """

    def collect_places(self, city: str, state: str):
        """Collects Google API Places list based on the niche.
        For each niche, there are a configured set of search terms.
        """
        # Placeholder implementation
        places = [
            {'name': 'AASI Place 1', 'city': city, 'state': state},
            {'name': 'AASI Place 2', 'city': city, 'state': state},
        ]
        self.ensamble['places'] = places

    def collect_details(self):
        """Collects detailed information for each place."""
        if 'places' not in self.ensamble:
            raise ValueError('No places collected. Call collect_places first.')

        detailed_places = []
        for place in self.ensamble['places']:
            # Placeholder for detailed info collection
            detailed_info = {
                'name': place['name'],
                'city': place['city'],
                'state': place['state'],
                'address': '123 Main St',
                'phone': '555-1234',
            }
            detailed_places.append(detailed_info)

        self.places = detailed_places
