from application.storages.abstract_source import AbstractSource


class Kafka(AbstractSource):
    def __init__(self, broker_address: str, topic_name: str, message_schema_path: str, message_format: str):
        super().__init__(broker_address, topic_name, message_schema_path, message_format)
