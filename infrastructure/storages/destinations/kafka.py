from application.storages.abstract_destination import AbstractDestination


class Kafka(AbstractDestination):
    def __init__(self, storage_address: str, message_schema_path: str, message_format: str):
        self._broker_address = storage_address
        self._schema_path = message_schema_path
        self._message_format = message_format

    def get_address(self) -> str:
        return self._broker_address

    def get_message_schema(self) -> str:
        with open(self._schema_path, 'r') as file:
            return file.read()

    def get_message_format(self) -> str:
        return self._message_format
