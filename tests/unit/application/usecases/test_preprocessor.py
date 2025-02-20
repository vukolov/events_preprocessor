from unittest import TestCase
import utils
import shutil
import os
import json
from application.usecases.preprocessor import Preprocessor
from infrastructure.data_stream_managers.spark import Spark
from infrastructure.storages.sources.file import File as SourceFile
from infrastructure.storages.destinations.console import Console as DestinationConsole
from infrastructure.storages.destinations.file import File as DestinationFile


class TestPreprocessor(TestCase):
    def setUp(self):
        project_root = str(utils.get_project_root())
        shutil.rmtree(project_root + '/tests/data/checkpoint', ignore_errors=True)

        self._project_root = project_root
        self._data_source = SourceFile(
            project_root + '/tests/data',
            'input_messages.json',
            project_root + '/infrastructure/storages/sources/schemas/metrics/sequences/message.json',
            'json')
        self._checkpoints_path = project_root + "/tests/data/checkpoint"

    def test_visualise(self):
        """Just visualise the output. Always Success"""
        normalization_model_path = self._project_root + "/tests/data/norm_model"
        console_destination = DestinationConsole('_',
                                                 '',
                                                 '',
                                                 'json')
        preprocessor = Preprocessor(Spark(),
                                    self._data_source,
                                    console_destination,
                                    self._checkpoints_path)
        preprocessor.run_sequence_agg_by_time_with_normalization(normalization_model_path)
        self.assertTrue(True)

    def test_run_sequence_agg_by_time_with_normalization(self):
        """Checks that the output messages have the necessary fields"""
        files_dir = self._project_root + '/tests/data/test_preprocessor_output_data'
        normalization_model_path = self._project_root + "/tests/data/norm_model"
        shutil.rmtree(files_dir, ignore_errors=True)

        file_destination = DestinationFile(files_dir, '', '', 'json')
        preprocessor = Preprocessor(Spark(),
                                    self._data_source,
                                    file_destination,
                                    self._checkpoints_path)
        preprocessor.run_sequence_agg_by_time_with_normalization(normalization_model_path)
        preprocessor.stop()

        data = []
        for filename in os.listdir(files_dir):
            if filename.endswith('.json'):
                with open(os.path.join(files_dir, filename), 'r') as f:
                    file_content = f.read()
                    if file_content:
                        message = json.loads(file_content)
                        data.append(message)
                        self.assertIn('metric_group_uid', message)
                        self.assertIn('window', message)
                        self.assertIn('sorted_metric_ids', message)
                        self.assertIn('sum_scaled_prepared', message)
                        self.assertIn('avg_scaled_prepared', message)

        self.assertGreaterEqual(len(data), 1)
