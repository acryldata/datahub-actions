import json


# Class that stores running statistics for a single Actions Transformer.
class TransformerStats:
    # The number of exceptions raised by the Transformer.
    exception_count: int = 0

    # The total number of events that were received by the transformer.
    processed_count: int = 0

    # The number of events filtered by the Transformer. The total transformed count is equal to processed count - filtered count.
    filtered_count: int = 0

    def increment_exception_count(self) -> None:
        self.exception_count = self.exception_count + 1

    def increment_processed_count(self) -> None:
        self.processed_count = self.processed_count + 1

    def increment_filtered_count(self) -> None:
        self.filtered_count = self.filtered_count + 1

    def get_exception_count(self) -> int:
        return self.exception_count

    def get_processed_count(self) -> int:
        return self.processed_count

    def get_filtered_count(self) -> int:
        return self.filtered_count

    def as_string(self) -> str:
        return json.dumps(self.__dict__, indent=4, sort_keys=True)
