from abc import ABC, abstractmethod


class BaseScrapper(ABC):
    def __init__(self):
        self.ensamble = {}

    @abstractmethod
    def collect_places(self, city: str, state: str):
        """Collects Google API Places list based on the niche.
        For each niche, there are a configured set of search terms.
        """
        pass

    @abstractmethod
    def collect_details(self):
        """Collects detailed information for each place."""
        pass
