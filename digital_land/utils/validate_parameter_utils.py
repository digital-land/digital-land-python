def positive_int(value, plugin_name, parameter_name):
    if not isinstance(value, int) or value <= 0:
        raise ValueError(
            f"{plugin_name} parameter '{parameter_name}' must be a positive integer"
        )
    return value


def non_negative_int(value, plugin_name, parameter_name):
    if not isinstance(value, int) or value < 0:
        raise ValueError(
            f"{plugin_name} parameter '{parameter_name}' must be a non-negative integer"
        )
    return value


def validate_plugin_parameters(parameters, plugin_name, defaults, validators):
    if parameters is None:
        parameters = {}

    if not isinstance(parameters, dict):
        raise ValueError(f"{plugin_name} parameters must be a dictionary")

    allowed_parameters = set(validators)
    unknown = set(parameters) - allowed_parameters
    if unknown:
        raise ValueError(
            f"Unsupported {plugin_name} parameters: {sorted(unknown)}. "
            f"Allowed parameters: {sorted(allowed_parameters)}"
        )

    validated = defaults.copy()

    for parameter_name, validator in validators.items():
        if parameter_name in parameters:
            validated[parameter_name] = validator(
                parameters[parameter_name], plugin_name, parameter_name
            )

    return validated
