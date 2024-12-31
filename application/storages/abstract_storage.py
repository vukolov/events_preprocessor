from abc import ABCMeta, abstractmethod


class AbstractStorage(metaclass=ABCMeta):
    def __init__(self, storage_address: str, storage_name: str, message_schema_path: str, message_format: str):
        self._storage_address = storage_address
        self._storage_name = storage_name
        self._schema_path = message_schema_path
        self._message_format = message_format

    def get_address(self) -> str:
        return self._storage_address

    def get_message_schema(self) -> str:
        with open(self._schema_path, 'r') as file:
            return file.read()

    def get_message_format(self) -> str:
        return self._message_format

    def get_topic_name(self) -> str:
        return self._storage_name
