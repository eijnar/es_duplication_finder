import csv
import os

from redis_utils import is_written_redis, mark_as_written_redis


def save_duplicates_to_csv(redis_conn, duplicates, alias_name, output_file):
    """
    Save duplicates to a CSV file if they haven't already been written, using Redis to track written IDs.
    
    :param redis_conn: Redis connection object.
    :param duplicates: List of tuples containing (doc_id, count).
    :param alias_name: The alias name to save in the CSV.
    :param output_file: The file to append duplicate entries to.
    """
    
    # Open the file in append mode and write new duplicates
    with open(output_file, 'a', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)

        # If the file is new, write the header
        if os.stat(output_file).st_size == 0:
            csvwriter.writerow(['_id', 'count', 'alias'])  # Write the header

        for doc_id, count in duplicates:
            if not is_written_redis(redis_conn, doc_id):  # Use Redis to check if the _id has been written
                # Write to CSV if the ID hasn't been written
                csvwriter.writerow([doc_id, count, alias_name])
                mark_as_written_redis(redis_conn, doc_id)  # Mark the ID as written in Redis
