from typing import List

import pandas as pd
from pandas import DataFrame
from sqlalchemy import MetaData
from sqlalchemy.orm import Session

from database.connection import engine
from database.models import Model, create_view


def validate_df(df: DataFrame) -> DataFrame:
    df['rep_dt'] = pd.to_datetime(df['rep_dt'])
    df['delta'] = df['delta'].apply(lambda x: float(x.replace(',', '.')))

    return df


def load_xlsx(db: Session):
    """ Загрузка файла в БД.

    """

    db.query(Model).delete()
    db.commit()

    filename = 'files/testData.xlsx'

    try:
        df = pd.read_excel(filename, dtype=str, keep_default_na=False)
        df.columns = map(str.lower, df.columns)
        df = validate_df(df)
        df.to_sql(name=Model.__tablename__,
                  con=engine,
                  if_exists='append',
                  index=False)
        return {'result': 'OK'}

    except Exception as e:
        return {'result': f'error: {e}'}


def get_delta_sql(db: Session, lag_num: int) -> List:
    """ Создание представления и выгрузка данных из представления.

    """
    create_view(lag_num)

    meta = MetaData(engine)
    meta.reflect(views=True)
    view = meta.tables['delta_lag']

    table = db.query(view)

    return table.all()


def get_delta_pandas(lag_num: int) -> List:
    """ Поиск смещения с помощью Pandas .

    """
    df = pd.read_sql_table('models', con=engine)
    df = df.sort_values('rep_dt')
    df['delta_lag'] = df.delta.shift(lag_num)
    df.dropna(inplace=True)

    return df.to_dict('records')
