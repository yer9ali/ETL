import re
from psycopg2.extras import DictCursor
from postgres_to_es.connection import connect_postgres
from postgres_to_es.query import load_film_id, load_person_q, full_load, query_all_genre, load_person_role
from postgres_to_es.state import State, JsonFileStorage


class DBLoader:
    def __init__(self, config, state):
        self.conn = connect_postgres(config)
        self.cursor = self.conn.cursor(cursor_factory=DictCursor)
        self.batch_size = config.film_work_pg.limit
        self.state_key = State(JsonFileStorage(config.film_work_pg.state_file_path)).get_state(state)
        self.data = []

    def load_film_work_id(self) -> str:
        query = load_film_id % load_person_q
        if self.state_key is None:
            return query
        inx = query.rfind(
            f'WHERE pfw.person_id IN ({load_person_q})'
        )
        return f"{query[:inx]} AND updated_at > '{self.state_key}' {query[inx:]}"

    def load_all_film_work_person(self) -> str:
        return full_load % self.load_film_work_id()

    def load_genre(self) -> str:
        if self.state_key is None:
            return query_all_genre
        inx = re.search('FROM content.genre', query_all_genre).end()
        return f"{query_all_genre[:inx]} WHERE updated_at > '{self.state_key}' {query_all_genre[inx:]}"

    def load_person(self) -> str:
        query = load_person_role
        if self.state_key is None:
            return query
        inx = re.search('ON p.id = pfw.person_id', query).end()
        return f"{query[:inx]} WHERE updated_at > '{self.state_key}' {query[inx:]}"


class LoadMovies(DBLoader):
    def loader_movies(self) -> list:
        self.cursor.execute(self.load_all_film_work_person())

        while True:
            rows = self.cursor.fetchmany(self.batch_size)
            if not rows:
                break

            for row in rows:
                d = {
                    "id": dict(row).get('id'),
                    "imdb_rating": dict(row).get('rating'),
                    "genre": dict(row).get('genre'),
                    "title": dict(row).get('title'),
                    "description": dict(row).get('description'),
                    "director": dict(row).get('director'),
                    "actors_names": dict(row).get('actors_names'),
                    "writers_names": dict(row).get('writers_names'),
                    "actors": dict(row).get('actors'),
                    "writers": dict(row).get('writers'),
                }
                self.data.append(d)
        return self.data


class LoadGenre(DBLoader):
    def loader_genre(self) -> list:
        self.cursor.execute(self.load_genre())

        while True:
            rows = self.cursor.fetchmany(self.batch_size)
            if not rows:
                break

            for row in rows:
                d = {
                    "id": dict(row).get('id'),
                    "name": dict(row).get('name'),
                    "description": dict(row).get('description'),
                }
                self.data.append(d)
        return self.data


class LoadPerson(DBLoader):
    def loader_person(self) -> list:
        self.cursor.execute(self.load_person())

        while True:
            rows = self.cursor.fetchmany(self.batch_size)
            if not rows:
                break

            for row in rows:
                d = {
                    "id": dict(row).get('id'),
                    "full_name": dict(row).get('full_name'),
                    "birth_date": dict(row).get('birth_date'),
                    "role": dict(row).get('role').replace('{', '').replace('}', ''),
                    "film_ids": [dict(row).get('film_ids').replace('{', '').replace('}', '')]
                }
                self.data.append(d)
        return self.data