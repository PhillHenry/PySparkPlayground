import pytest
import myspark.transition_matrix as t
from dateutil.parser import parse
from datetime import datetime


pytestmark = pytest.mark.usefixtures("spark_context", "spark_session")

feb_24 = parse("2015-02-24T13:00:00-08:00")
feb_25 = parse("2015-02-25T13:00:00-08:00")
feb_26 = parse("2015-02-26T13:00:00-08:00")
feb_27 = parse("2015-02-27T13:00:00-08:00")
feb_28 = parse("2015-02-28T13:00:00-08:00")
test_input = [
    {t.USER_ID: 1, t.EVENT_NAME: t.CARDIOLOGY, t.STATE: t.WAITING, t.EVENT_TIME: feb_24},
    {t.USER_ID: 1, t.EVENT_NAME: t.CARDIOLOGY, t.STATE: t.IN_PATIENT, t.EVENT_TIME: feb_25},
    {t.USER_ID: 1, t.EVENT_NAME: t.CARDIOLOGY, t.STATE: t.THEATRE, t.EVENT_TIME: feb_26},
    {t.USER_ID: 1, t.EVENT_NAME: t.CARDIOLOGY, t.STATE: t.IN_PATIENT, t.EVENT_TIME: feb_27},
    {t.USER_ID: 1, t.EVENT_NAME: t.CARDIOLOGY, t.STATE: t.OUT_PATIENT, t.EVENT_TIME: feb_28},
    {t.USER_ID: 2, t.EVENT_NAME: t.CARDIOLOGY, t.STATE: t.WAITING, t.EVENT_TIME: feb_24},
    {t.USER_ID: 2, t.EVENT_NAME: t.CARDIOLOGY, t.STATE: t.WAITING, t.EVENT_TIME: feb_25},
    {t.USER_ID: 2, t.EVENT_NAME: t.CARDIOLOGY, t.STATE: t.DIAGNOSTICS, t.EVENT_TIME: feb_26},
]


def test_transitions(spark_context, spark_session):
    df = spark_session.createDataFrame(test_input, schema=t.schema)

    paths = t.add_paths(df)
    paired = t.to_transitions(paths)

    assert paired.count() == len(test_input) - 2  # users


def test_paths(spark_context, spark_session):
    df = spark_session.createDataFrame(test_input, schema=t.schema)

    paths = t.add_paths(df)
    paths.show()
    assert paths.count() == 2  # patients



