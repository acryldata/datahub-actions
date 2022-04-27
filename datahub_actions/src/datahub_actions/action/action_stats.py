import json


# Class that stores running statistics for a single Action.
# TODO: Invocation time tracking.
class ActionStats:
    # The number of exception raised by the Action.
    exception_count: int = 0

    # The number of events that were actually submitted to the Action
    success_count: int = 0

    def increment_exception_count(self) -> None:
        self.exception_count = self.exception_count + 1

    def get_exception_count(self) -> int:
        return self.exception_count

    def increment_success_count(self) -> None:
        self.success_count = self.success_count + 1

    def get_success_count(self) -> int:
        return self.success_count

    def as_string(self) -> str:
        return json.dumps(self.__dict__, indent=4, sort_keys=True)
