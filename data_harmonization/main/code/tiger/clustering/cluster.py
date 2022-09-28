from abc import ABC, abstractmethod
from typing import Optional
import pandas as pd


class Blocking(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def prepare_data(self, data: Optional[pd.DataFrame] = None):
        pass

    @abstractmethod
    def do_blocking(self, docs: dict):
        pass
