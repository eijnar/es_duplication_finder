import logging

def setup_logging(log_file='elasticsearch_duplicates.log', level=logging.INFO):
    """
    Set up logging configuration to ensure consistent logging across all modules.
    
    :param log_file: The path to the log file.
    :param level: The logging level for the file logger.
    """
    
    # Create a FileHandler for logging to a file
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(level)  # Use the provided logging level for the file
    
    # Create a StreamHandler for logging to the console
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.ERROR)  # Only show errors or above in the console

    # Set the logging format
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)
    
    # Set up the basic logging configuration
    logging.basicConfig(
        level=level,  # Base logging level for the entire logger
        handlers=[file_handler, stream_handler]  # Use both file and stream handlers
    )
    
    # Optional: Silence Elasticsearch logs below WARNING level
    es_logger = logging.getLogger('elastic_transport.transport')
    es_logger.setLevel(logging.WARNING)