import logging

from elasticsearch import Elasticsearch


def initialize_es_client(config):
    """
    Initialize the Elasticsearch client using API key authentication.
    
    :param config: Configuration dictionary.
    :return: Elasticsearch client instance.
    """
    logging.info("Initializing Elasticsearch client")
    return Elasticsearch(
        hosts=config['elasticsearch']['hosts'],
        api_key=config['elasticsearch']['api_key']
    )