import logging

from tqdm import tqdm
from elasticsearch import AuthenticationException, ConnectionError

from utils import save_duplicates_to_csv
from redis_utils import check_duplicate_redis, get_redis_connection, is_written_redis, mark_as_written_redis, update_id_count_redis


redis_conn = get_redis_connection()

def scroll_search_incremental(es, alias_name, scroll_time, page_size, output_file):
    """
    Perform an incremental scroll search on a given alias and check for duplicates as you go,
    fetching only the _id and not the full source of the documents.
    
    :param es: Elasticsearch client instance.
    :param alias_name: The alias or index name to search.
    :param scroll_time: The duration for which the scroll context is kept alive.
    :param page_size: The number of documents to fetch per scroll batch.
    :param output_file: File to save duplicates.
    """
    logging.info(f"Starting scroll search for alias: {alias_name} with page_size={page_size} and scroll_time={scroll_time}")
    
    total_processed = 0
    scroll_id = None

    try:
        response = es.search(
            index=alias_name,
            body={
                "query": {
                    "match_all": {}
                },
                "_source": False,  # Only fetch the _id, omit the full document source
                "size": page_size  # Limit the number of documents fetched per scroll request
            },
            scroll=scroll_time  # Scroll context remains open for this duration
        )

        scroll_id = response['_scroll_id']
        total_hits = response['hits']['total']['value']
        logging.info(f"Total documents to process: {total_hits} for alias {alias_name}")

        progress_bar = tqdm(total=total_hits, desc=f"Processing alias: {alias_name}", unit="docs")

        while True:
            hits = response['hits']['hits']
            if not hits:
                break
            
            # Process each document
            for hit in hits:
                doc_id = hit['_id']
                update_id_count_redis(redis_conn, doc_id)
                total_processed += 1

            # Check for duplicates and save to CSV
            duplicates = []
            for hit in hits:
                doc_id = hit['_id']
                if check_duplicate_redis(redis_conn, doc_id) and not is_written_redis(redis_conn, doc_id):
                    duplicates.append((doc_id, redis_conn.hget('id_counts', doc_id)))
            
            if duplicates:
                # Save duplicates to CSV and mark them as written
                save_duplicates_to_csv(duplicates, alias_name, output_file)
                for doc_id, _ in duplicates:
                    mark_as_written_redis(redis_conn, doc_id)

            # Fetch next batch
            response = es.scroll(scroll_id=scroll_id, scroll=scroll_time)

            progress_bar.update(len(hits))

        progress_bar.close()
        logging.info(f"Completed scroll search for alias: {alias_name}. Total documents processed: {total_processed}")

    except AuthenticationException:
        logging.error("Authentication failed: Please check your API key")
    except ConnectionError:
        logging.error("Connection error: Unable to reach Elasticsearch.")
    finally:
        if scroll_id:
            try:
                es.clear_scroll(scroll_id=scroll_id)
                logging.info(f"Cleared scroll context for alias: {alias_name}")
            except Exception as clear_err:
                logging.error(f"Failed to clear scroll context: {clear_err}")
