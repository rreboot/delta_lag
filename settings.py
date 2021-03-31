from pydantic import BaseSettings


class Settings(BaseSettings):
    server_host: str = '127.0.0.1'
    server_port: int = 8000
    db_user: str
    db_password: str
    db_host: str = '172.17.0.2'
    db_port: int = 5432
    db_name: str = 'test_db'


settings = Settings(
    _env_file='.env',
    _env_file_encoding='utf-8'
)
