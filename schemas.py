from datetime import date

from pydantic import BaseModel


class Model(BaseModel):
    rep_dt: date
    delta: float

    class Config:
        orm_mode = True


class ModelCreate(Model):
    delta_lag: float
