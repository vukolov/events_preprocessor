from application.storages.abstract_storage import AbstractStorage
from abc import ABCMeta


class AbstractSource(AbstractStorage, metaclass=ABCMeta):
    ...
