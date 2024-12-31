from application.storages.abstract_destination import AbstractDestination


class Console(AbstractDestination):
    def get_address(self) -> str:
        return ''

    def get_topic_name(self) -> str:
        return ''
