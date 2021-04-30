import pytest
import myspark.pi as to_test

pytestmark = pytest.mark.usefixtures("spark_context")

def test_my_app(spark_context):
    pi = to_test.calculate_pi(spark_context)
    assert pi == pytest.approx(3.14, 0.01)
