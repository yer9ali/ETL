import psycopg2

from utils import backoff


@backoff()
def connect_postgres(config):
    connection = psycopg2.connect(**dict(config.film_work_pg.dsn))
    return connection