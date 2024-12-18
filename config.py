import json
import logging


def load_config(config_file='config.json'):
    """
    Load configuration from a JSON file.
    
    :param config_file: Path to the configuration file.
    :return: Dictionary containing configuration.
    """
    logging.info(f"Loading configuration from {config_file}")
    try:
        with open(config_file, 'r') as file:
            config = json.load(file)
        logging.info("Configuration loaded successfully")
    except FileNotFoundError as e:
        logging.error(f"Configuration file not found: {config_file}")
        raise e
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding the configuration file: {config_file}")
        raise e

    return config
