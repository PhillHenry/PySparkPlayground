from pyspark.sql.types import (StringType, StructType, StructField, FloatType, ArrayType,
                               IntegerType, TimestampType)
import pyspark.sql.dataframe as Dataset
from pyspark.sql import Window, functions as F


USER_ID = "user_id"
EVENT_NAME = "event_name"
EVENT_TIME = "event_time"
STATE = "state"

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


def to_transition_matrix(df) -> Dataset:
    user_window = Window().partitionBy(USER_ID)
    w = user_window.orderBy(F.col(EVENT_TIME).asc())
    df = df.withColumn("path", F.collect_list(STATE).over(w))
    last_date_col = "last_date"
    df = df.withColumn(last_date_col, F.max(EVENT_TIME).over(user_window))
    df = df.where(df[EVENT_TIME] == df[last_date_col])
    return df
