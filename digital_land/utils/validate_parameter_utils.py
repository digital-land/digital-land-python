def validate_parameters(parameters):
    if parameters is None:
        parameters = {}

    if not isinstance(parameters, dict):
        raise ValueError("ArcGIS parameters must be a dictionary")

    unknown = set(parameters) - ALLOWED_ARCGIS_PARAMETERS
    if unknown:
        raise ValueError(
            f"Unsupported ArcGIS parameters: {sorted(unknown)}. "
            f"Allowed parameters: {sorted(ALLOWED_ARCGIS_PARAMETERS)}"
        )

    validated = {
        "timeout": DEFAULT_TIMEOUT,
        "retries": DEFAULT_RETRIES,
        "retry_backoff_seconds": DEFAULT_RETRY_BACKOFF_SECONDS,
    }

    if "max_page_size" in parameters:
        value = parameters["max_page_size"]
        if not isinstance(value, int) or value <= 0:
            raise ValueError("ArcGIS parameter 'max_page_size' must be a positive integer")
        validated["max_page_size"] = value

    if "timeout" in parameters:
        value = parameters["timeout"]
        if not isinstance(value, int) or value <= 0:
            raise ValueError("ArcGIS parameter 'timeout' must be a positive integer")
        validated["timeout"] = value

    if "retries" in parameters:
        value = parameters["retries"]
        if not isinstance(value, int) or value < 0:
            raise ValueError("ArcGIS parameter 'retries' must be a non-negative integer")
        validated["retries"] = value

    if "retry_backoff_seconds" in parameters:
        value = parameters["retry_backoff_seconds"]
        if not isinstance(value, int) or value <= 0:
            raise ValueError(
                "ArcGIS parameter 'retry_backoff_seconds' must be a positive integer"
            )
        validated["retry_backoff_seconds"] = value

    return validated
