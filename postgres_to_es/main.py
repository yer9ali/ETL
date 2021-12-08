from datetime import datetime
from time import sleep

from loguru import logger

from db_loader import LoadMovies, LoadGenre, LoadPerson, UpdateSomeMovieData
from es import ESLoader, ESCreator, ESState
from models import Config
from state import JsonFileStorage, State

state = 'state'
index_movies = 'movies'
index_person = 'persons'
index_genre = 'genres'
config = Config.parse_file('config.json')


def create_index():
    ESCreator(config).create_index(config.es_settings.schema_movies_path, index_movies)
    ESCreator(config).create_index(config.es_settings.schema_person_path, index_person)
    ESCreator(config).create_index(config.es_settings.schema_genre_path, index_genre)


def load_data_film_work():
    """Первый загрузчик загрузжает film_work, остальные загрузчики проверяют
    genre, person по дате если есть новые данные то обновляет соответствующих фильмов.
    Если index count не больше 1, это означает что внутри индекса отсутствуют данные
    и не вкючается обновления персоны и жанров"""
    try:
        index_count = ESState(config).get_count_index(index_movies)
        ESLoader(config).load(LoadMovies(config, state).load_movies(), index_movies)
        if index_count > 1:
            ESLoader(config).load(UpdateSomeMovieData(config, state).update_genres_on_film_work(), index_movies)
            ESLoader(config).load(UpdateSomeMovieData(config, state).update_person_on_film_work(), index_movies)
    except Exception:
        logger.error('Error loading movies')


def load_data_person():
    try:
        ESLoader(config).load(LoadPerson(config, state).loader_person(), index_person)
    except Exception:
        logger.error('Error loading person')


def load_data_genre():
    try:
        ESLoader(config).load(LoadGenre(config, state).loader_genre(), index_genre)
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
