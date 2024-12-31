from application.storages.abstract_source import AbstractSource


class File(AbstractSource):
    def __init__(self, file_dir: str, file_name: str, message_schema_path: str, message_format: str):
        super().__init__(file_dir, file_name, message_schema_path, message_format)
