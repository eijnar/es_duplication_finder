import logging

from config import load_config
from es_client import initialize_es_client
from scroll_search import scroll_search_incremental
from logging_config import setup_logging
from redis_utils import get_redis_connection


def run_scroll_and_save_duplicates(aliases, config):
    """
    Main function to orchestrate an incremental scroll search on one or multiple Elasticsearch aliases
    and handle saving duplicates to a CSV file.
    
    :param aliases: List of aliases (or a single alias as a string) to search for duplicates.
    :param config: Configuration dictionary.
    """
    if isinstance(aliases, str):
        aliases = [aliases]

    es = initialize_es_client(config)

    for alias_name in aliases:
        logging.info(f"Starting duplicate detection process for alias: {alias_name}")
        scroll_search_incremental(es, alias_name, config['scroll_time'], config['page_size'], config['output_file'])
        logging.info(f"Completed duplicate detection for alias: {alias_name}")

    logging.info("Completed processing for all aliases")


def run_check_for_duplicates(config_file='config.json'):
    """
    Load configuration and check for duplicates in multiple aliases.
    
    :param config_file: Path to the configuration file.
    """
    logging.info("Starting the process to check for duplicates")

    config = load_config(config_file)
    run_scroll_and_save_duplicates(config['aliases'], config)

    logging.info("Completed processing for all aliases")


if __name__ == '__main__':
    redis_conn = get_redis_connection()
    redis_conn.flushdb()
    setup_logging()
    run_check_for_duplicates('config.json')