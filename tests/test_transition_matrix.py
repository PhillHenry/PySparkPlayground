import pytest
import myspark.transition_matrix as t
from dateutil.parser import parse
from datetime import datetime


pytestmark = pytest.mark.usefixtures("spark_context", "spark_session")


def test_my_app(spark_context, spark_session):
    test_input = [
        {t.USER_ID: 1, t.EVENT_NAME: t.CARDIOLOGY, t.EVENT_TIME: parse("2015-02-24T13:00:00-08:00")},
    ]

    rdd = spark_context.parallelize(test_input, 1)
    df = spark_session.createDataFrame(test_input, schema=t.schema)

