from abc import ABC, abstractmethod

class BasePipeline(ABC):
    def __init__(self, source_connector, target_connector):
        self.source = source_connector
        self.target = target_connector

    @abstractmethod
    def validate(self):
        pass