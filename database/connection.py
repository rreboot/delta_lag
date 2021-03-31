from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from settings import settings


db_user = settings.db_user
db_password = settings.db_password
db_host = settings.db_host
db_port = settings.db_port
db_name = settings.db_name

# postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]
db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
engine = create_engine(db_url)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db() -> SessionLocal:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
