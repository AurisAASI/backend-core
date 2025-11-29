from src.models.scrappers import BaseScrapper


class CompanyFederalScrapper(BaseScrapper):
    """Scrapper for federal company data."""

    def __init__(self, state: str, niche: str):
        super().__init__(state, niche)
        # Additional initialization if needed

    def collect_data(self, **kwargs):
        return super().collect_data(**kwargs)
