from datetime import datetime
from time import sleep

from loguru import logger

from db_loader import LoadMovies, LoadGenre, LoadPerson
from es_saver import ESSaver
from models import Config
from state import JsonFileStorage, State

state = 'state'
index_movies = 'movies'
index_person = 'persons'
index_genre = 'genres'
config = Config.parse_file('config.json')


def create_index():
    ESSaver(config).create_index(config.es_settings.schema_movies_path, index_movies)
    ESSaver(config).create_index(config.es_settings.schema_person_path, index_person)
    ESSaver(config).create_index(config.es_settings.schema_genre_path, index_genre)


def load_data_film_work():
    """Первый загрузчик загрузжает film_work, остальные загрузчики проверяют
    genre, person по дате если есть новые данные то обновляет соответствующих фильмов.
    Если index count не больше 1, это означает что внутри индекса отсутствуют данные
    и не вкючается обновления персоны и жанров"""
    try:
        index_count = ESSaver(config).get_count_index(index_movies)
        ESSaver(config).load(LoadMovies(config, state).loader_movies(), index_movies)
        if index_count > 1:
            ESSaver(config).load(LoadGenre(config, state).loader_genre_film_work(), index_movies)
            ESSaver(config).load(LoadPerson(config, state).loader_person_film_work(), index_movies)
    except Exception:
        logger.error('Error loading movies')


def load_data_person():
    try:
        ESSaver(config).load(LoadPerson(config, state).loader_person(), index_person)
    except Exception:
        logger.error('Error loading person')


def load_data_genre():
    try:
        ESSaver(config).load(LoadGenre(config, state).loader_genre(), index_genre)
    except Exception:
        logger.error('Error loading genres')


def save_state(config, state):
    State(JsonFileStorage(config.film_work_pg.state_file_path)).set_state(state, value=str(datetime.now()))


if __name__ == '__main__':

    create_index()

    while True:
        load_data_film_work()
        load_data_person()
        load_data_genre()

        save_state(config, state)

        sleep(10)
