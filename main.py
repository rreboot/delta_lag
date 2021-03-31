from typing import Optional

import uvicorn
from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session

import utils
from database.connection import get_db
from database.models import create_table
from settings import settings

app = FastAPI()


@app.get('/export/sql')
def export_sql(db: Session = Depends(get_db), lag_num: Optional[int] = 1):
    records = utils.get_delta_sql(db=db, lag_num=lag_num)

    return records


@app.get('/export/pandas')
def export_pandas(lag_num: Optional[int] = 1):
    records = utils.get_delta_pandas(lag_num=lag_num)

    return records


@app.get('/import/xlsx', dependencies=[Depends(create_table)])
def import_xlsx(db: Session = Depends(get_db)):
    return utils.load_xlsx(db)


if __name__ == '__main__':
    uvicorn.run(
        'main:app',
        host=settings.server_host,
        port=settings.server_port,
        reload=True,
    )
