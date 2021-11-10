from datetime import datetime
from postgres_to_es.db_loader import LoadMovies
from postgres_to_es.es_loader import ESLoader
from postgres_to_es.models import Config
from postgres_to_es.state import JsonFileStorage, State


def create_index(config):
    ESLoader(config).create_index(config.es_settings.schema_movies_path, 'movies')
    ESLoader(config).create_index(config.es_settings.schema_genre_path, 'genre')
    ESLoader(config).create_index(config.es_settings.schema_person_path, 'person')


def load_data(config):
    ESLoader(config).load(LoadMovies(config, 'state').loader_movies(), 'movies')
    ESLoader(config).load(LoadMovies(config, 'state').loader_movies(), 'genre')
    ESLoader(config).load(LoadMovies(config, 'state').loader_movies(), 'person')


def save_state(config):
    State(JsonFileStorage(config.film_work_pg.state_file_path)).set_state(str('state'), value=str(datetime.now()))


if __name__ == '__main__':
    config = Config.parse_file('config.json')

    create_index(config)
    load_data(config)
    save_state(config)









