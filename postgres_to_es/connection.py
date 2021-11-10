import psycopg2
from postgres_to_es.utils import backoff


@backoff()
def connect_postgres(config):
    connection = psycopg2.connect(**dict(config.film_work_pg.dsn))
    return connection