from datetime import datetime
from loguru import logger
from postgres_to_es.db_loader import LoadMovies, LoadGenre, LoadPerson
from postgres_to_es.es_saver import ESSaver
from postgres_to_es.models import Config
from postgres_to_es.state import JsonFileStorage, State


def create_index(config, index_name):
    ESSaver(config).create_index(config.es_settings.schema_movies_path, index_name)


def load_data(config, state, index_name):
    try:
        ESSaver(config).load(LoadMovies(config, state).loader_movies(), index_name)
        ESSaver(config).load(LoadGenre(config, state).loader_genre(), index_name)
        ESSaver(config).load(LoadPerson(config, state).loader_person(), index_name)
    except Exception:
        logger.error('Ошибка при загрузке данных')


def save_state(config, state):
    State(JsonFileStorage(config.film_work_pg.state_file_path)).set_state(state, value=str(datetime.now()))


if __name__ == '__main__':
    state = 'state'
    index_name = 'movies'
    config = Config.parse_file('config.json')

    create_index(config, index_name)
    load_data(config, state, index_name)
    save_state(config, state)









