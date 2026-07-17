import datetime
import ipaddress
import re
import json
from src.base.log_config import get_logger
from src.base.utils import setup_config

logger = get_logger()

CONFIG = setup_config()

REQUIRED_FIELDS = ["ts", "src_ip"]
FORBIDDEN_FIELD_NAMES = [
    "logline_id",
    "batch_id",
]  # field names that are used internally


class FieldType:
    """Base class for all field validation types in the logline format configuration

    Provides the common interface for field validation. All specific field types inherit
    from this class and implement their own validation logic in the :meth:`validate` method.
    """

    def __init__(self, name: str):
        self.name = name

    def validate(self, value) -> bool:
        """Validates the input value according to the field type's rules.

        This method must be implemented by all inheriting field type classes.
        Each implementation defines specific validation logic appropriate for the field type.

        Args:
            value: The value to be validated.

        Returns:
            True if the value is valid according to the field type's rules, False otherwise.

        Raises:
            NotImplementedError: This base method must be overridden by subclasses.
        """
        raise NotImplementedError


class RegEx(FieldType):
    """Field type for regular expression pattern validation

    Validates field values against a specified regular expression pattern.
    Useful for validating structured text fields like domain names, sizes, or custom formats.
    """

    def __init__(self, name: str, pattern: str):
        super().__init__(name)
        self.pattern = re.compile(pattern)

    def validate(self, value) -> bool:
        """Validates the input value against the configured regular expression pattern.

        Args:
            value: The value to be validated against the regex pattern.

        Returns:
            True if the value matches the pattern, False otherwise.
        """
        return bool(self.pattern.match(str(value)))


class Timestamp(FieldType):
    """Field type for timestamp validation and parsing

    Validates timestamp fields according to a specified format string and provides
    functionality to convert valid timestamps to ISO format for internal processing.
    """

    def __init__(self, name: str, timestamp_format: str):
        super().__init__(name)
        self.timestamp_format = timestamp_format

    def validate(self, value) -> bool:
        """Validates the input value against the configured timestamp format.

        Args:
            value: The timestamp string to be validated.

        Returns:
            True if the value matches the timestamp format, False otherwise.
        """
        try:
            datetime.datetime.strptime(value, self.timestamp_format)
        except ValueError:
            return False

        return True

    def get_timestamp_as_str(self, value) -> str:
        """Converts a valid timestamp to ISO format string.

        Args:
            value: Correctly formatted timestamp according to self.timestamp_format.

        Returns:
            ISO formatted timestamp string for internal processing.
        """
        return str(datetime.datetime.strptime(value, self.timestamp_format).isoformat())


class IpAddress(FieldType):
    """Field type for IP address validation

    Validates both IPv4 and IPv6 addresses using :mod:`ipaddress`.
    No additional configuration parameters are required beyond the field name.
    """

    def __init__(self, name):
        super().__init__(name)

    def validate(self, value) -> bool:
        """Validates the input value as a valid IP address.

        Args:
            value: The IP address string to be validated.

        Returns:
            True if the value is a valid IPv4 or IPv6 address, False otherwise.
        """
        try:
            ipaddress.ip_address(value)
        except ValueError:
            return False

        return True


class ListItem(FieldType):
    """Field type for list-based validation with optional relevance filtering

    Validates field values against an allowed list and optionally defines which values
    are considered relevant for filtering in later pipeline stages. The allowed_list
    contains all valid values, while the optional relevant_list defines a subset
    used for relevance-based filtering in the Log Filtering stage.
    """

    def __init__(self, name: str, allowed_list: list, relevant_list: list):
        super().__init__(name)
        self.allowed_list = allowed_list

        if relevant_list and not all(e in allowed_list for e in relevant_list):
            raise ValueError("Relevant types are not allowed types")

        self.relevant_list = relevant_list

    def validate(self, value) -> bool:
        """Validates the input value against the allowed list.

        Args:
            value: The value to be validated.

        Returns:
            True if the value is in the allowed_list, False otherwise.
        """
        return value in self.allowed_list


class RelevanceHandler:
    """
    Handler class to check the relevance of a given logline. Loads the appropriate child method by the name, configured
    in the config.yaml at the ``log_filtering`` section from the ``relevance_method`` attribute.
    """

    def __init__(self, log_configuration_instances):
        self.log_configuration_instances = log_configuration_instances

    def check_relevance(self, function_name: str, logline_dict: dict) -> bool:
        """
        wrapper function to get the appropriate relevance function by name.

        Args:
            function_name (str): The name of the relevance_method to import
            logline_dict (dict): The dictionary version of a logline

        Returns:
            True, if the logline is relevant according to the relevance function, else False
        """
        is_relevant = False
        try:
            is_relevant = getattr(self, function_name)(logline_dict)
        except AttributeError as e:
            logger.error(f"Function {function_name} is not implemented!")
            raise Exception(f"Function {function_name} is not implemented!")
        return is_relevant

    def check_dga_relevance(self, logline_dict: dict) -> bool:
        """
        Method to check if a given logline is relevant for a dga analysis.

        Args:
            logline_dict (dict): The dictionary version of a logline

        Returns:
            True, if the logline is relevant according to the relevance function, else False
        """
        relevant = True
        for _, instance_configuartion in self.log_configuration_instances.items():
            if isinstance(instance_configuartion, ListItem):
                if instance_configuartion.relevant_list:
                    relevant = (
                        logline_dict[instance_configuartion.name]
                        in instance_configuartion.relevant_list
                    )
                    if not relevant:
                        return relevant
        return relevant

    def no_relevance_check(self, logline_dict: dict) -> bool:
        """
            Skip the relevance check by always returning True

        Args:
            logline_dict (dict): The dictionary version of a logline

        Returns:
            Always returns True (all lines are relevant)
        """
        return True


class LoglineHandler:
    """Main handler for logline validation and processing

    Manages the configuration-based validation of loglines according to the format
    specified in the configuration file. Provides validation, field extraction,
    and relevance checking functionality for the log processing pipeline.
    """

    def __init__(self, validation_config: list):
        """
        Check all existing log configurations for validity.

        Args:
            validation_config (list): A list containing the configured attributes a given logline needs to hold. Otherwise it gets discarded
        """
        self.logformats = validation_config
        log_configuration_instances = {}
        if not validation_config:
            raise ValueError("No fields configured")
        for log_config_item in validation_config:
            instance = self._create_instance_from_list_entry(log_config_item)
            if instance.name in FORBIDDEN_FIELD_NAMES:
                raise ValueError(
                    f"Forbidden field name included. These fields are used internally "
                    f"and cannot be used as names: {FORBIDDEN_FIELD_NAMES}"
                )
            if log_configuration_instances.get(instance.name):
                raise ValueError("Multiple fields with same name")
            else:
                log_configuration_instances[instance.name] = instance
        for required_field in REQUIRED_FIELDS:

            if required_field not in log_configuration_instances.keys():
                raise ValueError("Not all needed fields are set in the configuration")

        self.relvance_handler = RelevanceHandler(
            log_configuration_instances=log_configuration_instances
        )

    def validate_logline(self, logline: str) -> bool:
        """
        Validates the given input logline by checking if the fields presented are corresponding to a given logformat of a protocol.
        Calls the :meth:`validate` method for each field. If the logline is incorrect, it shows an error with the
        incorrect fields being highlighted.

        Args:
            logline (str): Logline string to be validated.

        Returns:
            True if the logline contains correct fields in the configured format, False otherwise.
        """
        logline = json.loads(logline)
        valid_values = []
        invalid_value_names = []
        for log_config_item in self.logformats:
            # by convention the first item is always the key present in a logline
            log_line_property_key = log_config_item[0]
            instance = self._create_instance_from_list_entry(log_config_item)
            try:
                is_value_valid = instance.validate(logline.get(log_line_property_key))
                valid_values.append(is_value_valid)
                if not is_value_valid:
                    invalid_value_names.append(log_line_property_key)
            except:
                logger.error(
                    f"line {logline} does not contain the specified field of {log_line_property_key}"
                )
        if all(valid_values):
            return True
        return False

    def __get_fields_as_json(self, logline: str) -> dict:
        """Extracts fields from a logline and returns them as a dictionary.

        Parses the logline into individual fields and creates a dictionary with
        field names as keys and field values as values. Handles timestamp conversion
        to ISO format for internal processing. Does not perform validation.

        Args:
            logline (str): Logline to extract fields from.

        Returns:
            Dictionary with field names as keys and field values as values.
        """
        return json.loads(logline)

    def validate_logline_and_get_fields_as_json(self, logline: str) -> dict:
        """Validates a logline and returns the fields as a dictionary.

        Combines validation and field extraction in a single operation.
        First validates the logline format, then extracts and returns the fields.

        Args:
            logline (dict): Logline parsed from zeek

        Returns:
            Dictionary with field names as keys and field values as values.

        Raises:
            ValueError: If logline validation fails.
        """
        if not self.validate_logline(logline):
            raise ValueError("Incorrect logline, validation unsuccessful")
        return self.__get_fields_as_json(logline)

    def check_relevance(self, logline_dict: dict, function_name: str) -> bool:
        """
        Checks if the given logline is relevant.

        Args:
            logline_dict (dict): Logline parts to be checked for relevance as dictionary
            function_name (str): A string that points to the relevance function to use

        Returns:
            Propagates the bool from the subordinate relevance method
        """
        return self.relvance_handler.check_relevance(
            function_name=function_name, logline_dict=logline_dict
        )

    @staticmethod
    def _create_instance_from_list_entry(field_list: list):
        """Creates a field type instance from configuration list entry.

        Parses the configuration format and creates the appropriate field type instance
        based on the specified class name and parameters. Supports RegEx, Timestamp,
        ListItem, and IpAddress field types with their respective parameter requirements.

        Args:
            field_list (list): Configuration list containing field name, type, and parameters.

        Returns:
            Field type instance configured according to the specification.

        Raises:
            ValueError: If the field configuration is invalid or unsupported.
        """
        len_of_field_list = len(field_list)

        if len_of_field_list < 2 or not isinstance(field_list[0], str):
            raise ValueError("Invalid field list or field name")

        name, cls_name = field_list[0], field_list[1]

        try:
            cls = globals()[cls_name]
        except KeyError:
            raise ValueError(f"Class '{cls_name}' not found")

        if cls_name == "RegEx":
            if len_of_field_list != 3 or not isinstance(field_list[2], str):
                raise ValueError("Invalid RegEx parameters")
            instance = cls(name=name, pattern=field_list[2])

        elif cls_name == "Timestamp":
            if len_of_field_list != 3 or not isinstance(field_list[2], str):
                raise ValueError("Invalid Timestamp parameters")
            instance = cls(name=name, timestamp_format=field_list[2])

        elif cls_name == "ListItem":
            if len_of_field_list not in [3, 4] or not isinstance(field_list[2], list):
                raise ValueError("Invalid ListItem parameters")

            relevant_list = field_list[3] if len_of_field_list == 4 else None
            instance = cls(
                name=name, allowed_list=field_list[2], relevant_list=relevant_list
            )

        elif cls_name == "IpAddress":
            if len_of_field_list != 2:
                raise ValueError("Invalid IpAddress parameters")

            instance = cls(name=name)

        else:
            raise ValueError(f"Unsupported class '{cls_name}'")

        return instance
