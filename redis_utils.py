import redis


def get_redis_connection(host='localhost', port=6379, db=0):
    """
    Get a Redis connection.
    
    :param host: Redis server host.
    :param port: Redis server port.
    :param db: Redis database number.
    :return: Redis connection object.
    """
    return redis.Redis(host=host, port=port, db=db)


def update_id_count_redis(redis_conn, doc_id):
    """
    Update the count of a given _id in Redis.
    
    :param redis_conn: Redis connection object.
    :param doc_id: The document ID to increment the count for.
    """
    redis_conn.hincrby('id_counts', doc_id, 1)


def check_duplicate_redis(redis_conn, doc_id):
    """
    Check if the count of a given _id in Redis is greater than 1.
    
    :param redis_conn: Redis connection object.
    :param doc_id: The document ID to check.
    :return: True if the _id is a duplicate, False otherwise.
    """
    count = redis_conn.hget('id_counts', doc_id)
    if count and int(count) > 1:
        return True
    return False


def mark_as_written_redis(redis_conn, doc_id):
    """
    Mark an _id as written to prevent writing it again.
    
    :param redis_conn: Redis connection object.
    :param doc_id: The document ID to mark as written.
    """
    redis_conn.sadd('written_ids', doc_id)


def is_written_redis(redis_conn, doc_id):
    """
    Check if an _id has already been written to the CSV.
    
    :param redis_conn: Redis connection object.
    :param doc_id: The document ID to check.
    :return: True if the _id has been written, False otherwise.
    """
    return redis_conn.sismember('written_ids', doc_id)