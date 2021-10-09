import os
import logging
import json


class DataConfig:
    """Retrieve information from the configuration files located in conf directory."""

    def __init__(self):
        """Initialize data configuration."""
        # Filepaths
        input_path = os.path.join(os.environ["CONF"], "data")
        self._perimeter_filepath = os.path.join(input_path, "perimeter.json")

        # Prepare configs
        self._prepare_perimeter_configuration()

    def _prepare_perimeter_configuration(self):
        """Load the project perimeter."""
        logging.info("  - Loading 'perimeter' attributes")
        with open(self._perimeter_filepath) as json_file:
            perimeter = json.load(json_file)
        self.vhls_perimeter = perimeter["vehicles"]
