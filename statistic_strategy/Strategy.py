from abc import ABC, abstractmethod

"""
Abstract class for Strategy pattern
"""


class Strategy(ABC):
    @abstractmethod
    def test(self, *args, **kwargs):
        pass

    @property
    @abstractmethod
    def name(self):
        pass
