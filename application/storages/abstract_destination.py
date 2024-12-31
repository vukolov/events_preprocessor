from application.storages.abstract_storage import AbstractStorage
from abc import ABCMeta


class AbstractDestination(AbstractStorage, metaclass=ABCMeta):
    ...
