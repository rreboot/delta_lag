from sqlalchemy import Column, Integer, Date, Float
from sqlalchemy.ext.declarative import declarative_base

from database.connection import engine

Base = declarative_base()


class Model(Base):
    __tablename__ = 'models'

    id = Column(Integer, primary_key=True, autoincrement=True)
    rep_dt = Column(Date)
    delta = Column(Float)

    def __repr__(self):
        return f'<Model(rep_dt={self.rep_dt}, delta={self.delta})>'


def create_table() -> None:
    Model.__table__.create(bind=engine, checkfirst=True)


def create_view(lag_num: int) -> None:
    """ Создание представления. Запросы заточены под Postgres. В 3-х вариантах.

    """
    q1 = """CREATE OR REPLACE VIEW delta_lag as
     select t1.id, t1.rep_dt, t1.delta, t2.delta as delta_lag 
     from
        (select id, rep_dt, delta, rep_dt - interval '{}' month as new_dt 
         from models) as t1
     left join models as t2
     on date_trunc('month', t1.new_dt) = date_trunc('month', t2.rep_dt) 
     order by t1.rep_dt desc
     """

    q2 = """CREATE OR REPLACE VIEW delta_lag as
    select t1.id, t1.rep_dt, t1.delta,
        (select delta as delta_lag 
         from models 
         where date_trunc('month', rep_dt) = date_trunc('month', t1.rep_dt - interval '{}' month)) as delta_lag
    from models t1
    order by rep_dt desc
    """

    q3 = """CREATE OR REPLACE VIEW delta_lag as
    select id, rep_dt, delta, lag(delta, {}, 0::double precision) over(order by rep_dt) delta_lag
    from models
    order by rep_dt desc
    """

    with engine.connect() as con:
        con.execute(q3.format(lag_num))
