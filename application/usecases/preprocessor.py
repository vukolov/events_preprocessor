from application.data_stream_manager import AbstractDataStreamManager
from application.storages.abstract_source import AbstractSource
from application.storages.abstract_destination import AbstractDestination
import utils


class Preprocessor:
    def __init__(self,
                 data_stream_manager: AbstractDataStreamManager,
                 data_source: AbstractSource,
                 data_destination: AbstractDestination):
        self._data_stream_manager = data_stream_manager
        self._data_source = data_source
        self._data_destination = data_destination

    def run(self):
        active_metric_groups = self._get_active_metric_groups_with_agg_intervals()
        aggregation_intervals_df = self._data_stream_manager.get_aggregation_intervals(active_metric_groups)
        normalization_model_path = self._get_normalization_model_path()
        checkpoint_destination = self._get_checkpoint_destination()
        data_stream = self._data_stream_manager.subscribe_on_source(self._data_source)
        data_stream = self._data_stream_manager.aggregate_metrics(data_stream, aggregation_intervals_df)
        data_stream = self._data_stream_manager.normalize_metrics_values(data_stream, normalization_model_path)
        data_stream = self._data_stream_manager.set_destination(data_stream, self._data_destination)
        data_stream = self._data_stream_manager.set_checkpoint_destination(data_stream, checkpoint_destination)
        self._data_stream_manager.start(data_stream)

    def stop(self):
        self._data_stream_manager.stop()

    def _get_active_metric_groups_with_agg_intervals(self) -> list:
        return [
            ("group_1", 5),
            ("group_2", 10),
            ("group_3", 15)
        ]

    def _get_normalization_model_path(self) -> str:
        project_root = str(utils.get_project_root())
        return project_root + "/tests/data/norm_model"

    def _get_checkpoint_destination(self) -> str:
        project_root = str(utils.get_project_root())
        return project_root + "/tests/data/checkpoint"

