from pyspark.sql.types import (StringType, StructType, StructField, FloatType, ArrayType,
                               IntegerType, TimestampType)

USER_ID = "user_id"
EVENT_NAME = "event_name"
EVENT_TIME = "event_time"

schema = StructType([
    StructField(USER_ID, IntegerType(), True),
    StructField(EVENT_NAME, StringType(), True),
    StructField(EVENT_TIME, TimestampType(), True),
])


CARDIOLOGY = 'cardiology'