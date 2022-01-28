import psycopg2
from psycopg2.extras import DictCursor

from query import load_genre_by_date, load_genre_from_film_work_id, load_person_by_date, \
    load_person_from_film_work_id, load_film_work_by_date, load_film_work_by_id, load_genre, load_person
from state import State, JsonFileStorage
from utils import replace_none, backoff


class DBLoader:

    def __init__(self, config, state):
        self.conn = self.connect_postgres(config)
        self.cursor = self.conn.cursor(cursor_factory=DictCursor)
        self.batch_size = config.film_work_pg.limit
        self.state_key = State(JsonFileStorage(config.film_work_pg.state_file_path)).get_state(state)
        self.data = []

    @backoff()
    def connect_postgres(self, config):
        connection = psycopg2.connect(**dict(config.film_work_pg.dsn))
        return connection


class MoviesLoader(DBLoader):
    def load_film_work(self) -> str:
        """Загрузка фильмов по дате"""
        return load_film_work_by_date % self.state_key

    def load_data_film_work(self) -> list:
        while True:
            rows = self.cursor.fetchmany(self.batch_size)
            if not rows:
                break

            for row in rows:
                d = {
                    "uuid": dict(row).get('id'),
                    "imdb_rating": dict(row).get('rating'),
                    "genre": dict(row).get('genre'),
                    "title": dict(row).get('title'),
                    "description": dict(row).get('description'),
                    "directors": dict(row).get('director'),
                    "directors_names": dict(row).get('directors_names'),
                    "actors_names": dict(row).get('actors_names'),
                    "writers_names": dict(row).get('writers_names'),
                    "actors": dict(row).get('actors'),
                    "writers": dict(row).get('writers'),
                    "subscribers_only": dict(row).get('subscribers_only')
                }
                replace_none(d)
                self.data.append(d)
        return self.data

    def load_from_film_work(self, query_by_date: str, load_from_work_id: str) -> str:
        self.cursor.execute(query_by_date)
        list_id_and_date = self.cursor.fetchall()
        if not list_id_and_date: return query_by_date

        list_by_date = [i[0] for i in list_id_and_date]
        query_by_id = load_from_work_id % str(list_by_date)[1:-1]

        self.cursor.execute(query_by_id)
        list_by_id = [i[0] for i in self.cursor.fetchall()]

        query_update = load_film_work_by_id % str(list_by_id)[1:-1]
        return query_update


class LoadMovies(MoviesLoader):
    def load_movies(self) -> list:
        self.cursor.execute(self.load_film_work())
        return self.load_data_film_work()


class LoaderSomeMovieData(MoviesLoader):
    def load_genre_for_film_work(self) -> str:
        """Загрузка жанров по дате и обновление соответствующих фильмов"""
        query_by_date = load_genre_by_date % self.state_key
        return self.load_from_film_work(query_by_date, load_genre_from_film_work_id)

    def load_person_film_work(self) -> str:
        """Загрузка персоны по дате и обновление соответствующих фильмов"""
        query_by_date = load_person_by_date % self.state_key
        return self.load_from_film_work(query_by_date, load_person_from_film_work_id)


class UpdateSomeMovieData(LoaderSomeMovieData):

    def update_genres_on_film_work(self) -> list:
        self.cursor.execute(self.load_genre_for_film_work())

        return self.load_data_film_work()

    def update_person_on_film_work(self) -> list:
        self.cursor.execute(self.load_person_film_work())

        return self.load_data_film_work()


class GenreLoader(DBLoader):

    def load_genre(self) -> str:
        return load_genre % self.state_key

    def load_data_genre(self) -> list:
        while True:
            rows = self.cursor.fetchmany(self.batch_size)
            if not rows:
                break

            for row in rows:
                d = {
                    "uuid": dict(row).get('id'),
                    "name": dict(row).get('name'),
                    "description": dict(row).get('description'),
                }
                replace_none(d)
                self.data.append(d)
        return self.data


class LoadGenre(GenreLoader):

    def loader_genre(self) -> list:
        self.cursor.execute(self.load_genre())
        return self.load_data_genre()


class PersonLoader(DBLoader):

    def load_data_person(self) -> list:
        while True:
            rows = self.cursor.fetchmany(self.batch_size)
            if not rows:
                break

            for row in rows:
                d = {
                    "uuid": dict(row).get('id'),
                    "full_name": dict(row).get('full_name'),
                    "birth_date": dict(row).get('birth_date'),
                    "role": dict(row).get('role'),
                    "film_ids": [i.get('id') for i in dict(row).get('film_ids')],
                }
                replace_none(d)
                self.data.append(d)
        return self.data

    def load_person(self) -> str:
        return load_person % self.state_key


class LoadPerson(PersonLoader):

    def loader_person(self) -> list:
        self.cursor.execute(self.load_person())

        return self.load_data_person()
