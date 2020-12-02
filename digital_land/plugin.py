import inspect
import logging
import sys

import pluggy

from digital_land import hookspecs


def get_plugin_manager():
    pm = pluggy.PluginManager("digital-land")

    # Gather all of the classes from hookspecs
    plugin_classes = inspect.getmembers(hookspecs, inspect.isclass)

    for name, class_ in plugin_classes:
        logging.debug("Adding hookspecs from %s", class_)
        pm.add_hookspecs(class_)

    pm.load_setuptools_entrypoints("digital_land")

    try:
        # Warn if unrecognised hook implementations have been defined
        pm.check_pending()
    except pluggy.PluginValidationError as e:
        logging.error(e)
        sys.exit(2)

    return pm
