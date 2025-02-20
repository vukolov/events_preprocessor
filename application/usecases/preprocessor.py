from application.data_stream_manager import AbstractDataStreamManager
from application.storages.abstract_source import AbstractSource
from application.storages.abstract_destination import AbstractDestination


class Preprocessor:

    DEFAULT_VALUE_FOR_EMPTY_METRIC = float(-999999)

    def __init__(self,
                 data_stream_manager: AbstractDataStreamManager,
                 data_source: AbstractSource,
                 data_destination: AbstractDestination,
                 checkpoints_path: str):
        self._data_stream_manager = data_stream_manager
        self._data_source = data_source
        self._data_destination = data_destination
        self._checkpoints_path = checkpoints_path

    def run_sequence_agg_by_time_with_normalization(self, normalization_model_path: str):
        active_metric_groups = self._get_active_metric_groups_with_agg_intervals()
        aggregation_intervals_df = self._data_stream_manager.get_aggregation_intervals(active_metric_groups)
        active_metrics_in_groups = self._data_stream_manager.get_active_metrics_in_groups(active_metric_groups)
        data_stream = self._data_stream_manager.subscribe_on_source(self._data_source)
        data_stream = self._data_stream_manager.aggregate_metrics(data_stream, aggregation_intervals_df, active_metrics_in_groups)
        data_stream = self._data_stream_manager.normalize_metrics_values(data_stream, normalization_model_path)
        data_stream = self._data_stream_manager.fill_empty_metrics_and_sort(data_stream)
        data_stream = self._data_stream_manager.set_destination(data_stream, self._data_destination)
        data_stream = self._data_stream_manager.set_checkpoint_destination(data_stream, self._checkpoints_path)
        self._data_stream_manager.start(data_stream)

    def stop(self):
        self._data_stream_manager.stop()

    def _get_active_metric_groups_with_agg_intervals(self) -> dict:
        # todo: request using API
        return {
            "group_1": {
                "metrics": ["metric_101", "metric_102", "metric_103"],
                "agg_interval_ms": 5000
            },
            "group_2": {
                "metrics": ["metric_201", "metric_202"],
                "agg_interval_ms": 5000
            },
            "group_3": {
                "metrics": ["metric_301", "metric_302"],
                "agg_interval_ms": 5000
            }
        }
