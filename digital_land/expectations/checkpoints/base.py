from abc import ABC, abstractmethod


class BaseCheckpoint(ABC):
    @abstractmethod
    def load():
        """filled in by child classes, ensures a config is loaded correctly should raise error if not"""
        pass

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def save(self, output_dir, format="csv"):
        """filled in by child classes, uses save functions to save the data. could add default behaviour at somepoint"""
        pass

    # Needs to be reinstated on child class
    # def act_on_critical_error(self, failed_expectation_with_error_severity=None):
    #     if failed_expectation_with_error_severity is None:
    #         getattr(self, "failed_expectation_with_error_severity", None)

    #     if failed_expectation_with_error_severity:
    #         if failed_expectation_with_error_severity > 0:
    #             raise DataQualityException(
    #                 "One or more expectations with severity RaiseError failed, see results for more details"
    #             )
