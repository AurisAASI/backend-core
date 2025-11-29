from abc import ABC, abstractmethod


class BaseScrapper(ABC):
    def __init__(self):
        self.ensamble = {}

    @abstractmethod
    def collect_data(self, **kwargs):
        pass
