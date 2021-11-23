from datetime import datetime
from time import sleep

from loguru import logger

from db_loader import LoadMovies, LoadGenre, LoadPerson
from es_saver import ESSaver
from models import Config
from state import JsonFileStorage, State


def create_index(config, path, index_name):
    ESSaver(config).create_index(path, index_name)


def load_data_film_work(config, state, index_name):
    """Первый загрузчик загрузжает film_work, остальные загрузчики проверяют
    genre, person по дате если есть новые данные то обновляет соответствующих фильмов.
    Если index count не больше 1, это означает что внутри индекса отсутствуют данные
    и не вкючается обновления персоны и жанров"""
    try:
        index_count = ESSaver(config).get_count_index(index_name)
        ESSaver(config).load(LoadMovies(config, state).loader_movies(), index_name)
        if index_count > 1:
            ESSaver(config).load(LoadGenre(config, state).loader_genre_film_work(), index_name)
            ESSaver(config).load(LoadPerson(config, state).loader_person_film_work(), index_name)
    except Exception:
        logger.error('Ошибка при загрузке фильмов')


def load_data_person(config, state, index_name):
    try:
        ESSaver(config).load(LoadPerson(config, state).loader_person(), index_name)
    except Exception:
        logger.error('Ошибка при загрузке персоны')


def load_data_genre(config, state, index_name):
    try:
        ESSaver(config).load(LoadGenre(config, state).loader_genre(), index_name)
    except Exception:
        logger.error('Ошибка при загрузке жанров')


def save_state(config, state):
    State(JsonFileStorage(config.film_work_pg.state_file_path)).set_state(state, value=str(datetime.now()))


if __name__ == '__main__':
    state = 'state'
    index_movies = 'movies'
    index_person = 'persons'
    index_genre = 'genres'
    config = Config.parse_file('config.json')

    while True:
        create_index(config, config.es_settings.schema_movies_path, index_movies)
        load_data_film_work(config, state, index_movies)
        create_index(config, config.es_settings.schema_person_path, index_person)
        load_data_person(config, state, index_person)
        create_index(config, config.es_settings.schema_genre_path, index_genre)
        load_data_genre(config, state, index_genre)

        save_state(config, state)

        sleep(10)
