import abc
import json
import os
from typing import Any, Optional


class BaseStorage:

    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        pass


class JsonFileStorage(BaseStorage):

    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path

    def save_state(self, state: dict) -> None:
        with open(self.file_path, 'w') as file:
            json.dump(state, file)

    def retrieve_state(self) -> dict:
        if self.file_path is not None and os.path.isfile(self.file_path):
            with open(self.file_path, 'r') as file:
                state = json.load(file)
            return state
        else:
            return {}


class State:

    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        cur_state = self.storage.retrieve_state()
        cur_state[key] = value
        self.storage.save_state(cur_state)

    def get_state(self, key: str) -> Any:
        cur_state = self.storage.retrieve_state()
        if key in cur_state:
            return cur_state[key]

