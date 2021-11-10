from typing import Optional, List
from pydantic import BaseModel


class DSNSettings(BaseModel):
    host: str
    port: int
    dbname: str
    password: str
    user: str
    options: str


class ESSettings(BaseModel):
    host: str
    port: int
    schema_movies_path: str
    schema_genre_path: str
    schema_person_path: str


class PostgresSettings(BaseModel):
    dsn: DSNSettings
    limit: Optional[int]
    order_field: List[str]
    state_field: List[str]
    fetch_delay: Optional[float]
    state_file_path: Optional[str]
    sql_query: str


class Config(BaseModel):
    film_work_pg: PostgresSettings
    es_settings: ESSettings