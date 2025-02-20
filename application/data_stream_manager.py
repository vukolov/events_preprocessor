from abc import ABCMeta, abstractmethod
from typing import Any
from application.storages.abstract_source import AbstractSource
from application.storages.abstract_destination import AbstractDestination
from application.data_stream import DataStream
from application.data_frame import DataFrame


class AbstractDataStreamManager(metaclass=ABCMeta):
    @abstractmethod
    def subscribe_on_source(self, stream_source: AbstractSource) -> DataStream:
        ...

    @abstractmethod
    def aggregate_metrics(self, data_stream: DataStream, interval_config: DataFrame, metrics_in_groups: DataFrame) -> DataStream:
        ...

    @abstractmethod
    def get_aggregation_intervals(self, active_metric_groups: dict[str, Any]) -> DataFrame:
        ...

    @abstractmethod
    def get_active_metrics_in_groups(self, active_metric_groups: dict[str, Any]) -> DataFrame:
        ...

    @abstractmethod
    def normalize_metrics_values(self, data_stream: DataStream, normalization_model_path: str) -> DataStream:
        ...

    @abstractmethod
    def fill_empty_metrics_and_sort(self, data_stream: DataStream) -> DataStream:
        ...

    @abstractmethod
    def set_checkpoint_destination(self, data_stream: DataStream, checkpoint_destination: str) -> DataStream:
        ...

    @abstractmethod
    def set_destination(self, data_stream: DataStream, stream_destination: AbstractDestination) -> DataStream:
        ...

    @abstractmethod
    def start(self, data_stream: DataStream):
        ...

    @abstractmethod
    def stop(self):
        ...
