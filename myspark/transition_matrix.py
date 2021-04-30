from pyspark.sql.types import (StringType, StructType, StructField, FloatType, ArrayType,
                               IntegerType, TimestampType)
import pyspark.sql.dataframe as Dataset
from pyspark.sql import Window, functions as F
from pyspark.ml.feature import StringIndexer


USER_ID = "user_id"
EVENT_NAME = "event_name"
EVENT_TIME = "event_time"
STATE = "state"
STATE_INDEX = f"{STATE}_index"

schema = StructType([
    StructField(USER_ID, IntegerType(), True),
    StructField(EVENT_NAME, StringType(), True),
    StructField(STATE, StringType(), True),
    StructField(EVENT_TIME, TimestampType(), True),
])


CARDIOLOGY = 'cardiology'
WAITING = "waiting"
IN_PATIENT = "inpatient"
OUT_PATIENT = "outpatient"
THEATRE = "theatre"
DIAGNOSTICS = "diagnostics"


def index_state(df):
    indexer = StringIndexer(inputCol=STATE, outputCol=STATE_INDEX, stringOrderType="alphabetDesc")
    return indexer.fit(df).transform(df)


def pairs(xs) -> list:
    if len(xs) < 2:
        return []
    last = xs[0]
    tuples = []
    for i in range(1, len(xs)):
        current = xs[i]
        tuples.append([last, current])
        last = current
    return tuples


def to_transition_matrix(df) -> Dataset:
    df = index_state(df)
    user_window = Window().partitionBy(USER_ID)
    w = user_window.orderBy(F.col(EVENT_TIME).asc())
    df = df.withColumn("path", F.collect_list(STATE_INDEX).over(w))
    last_date_col = "last_date"
    df = df.withColumn(last_date_col, F.max(EVENT_TIME).over(user_window))
    df = df.where(df[EVENT_TIME] == df[last_date_col])
    return df
