import factory

import pytest
from pytest_factoryboy import register, LazyFixture


@pytest.fixture
def spark_session():
    class SparkSession:
        def createDataframe(self, data, schema):
            return {"spark_session": self, "data": data, "schema": schema}

    return SparkSession()


class DataFrame:
    pass


class DataFrameFactory(factory.Factory):
    class Meta:
        model = DataFrame

    spark_session = LazyFixture("spark_session")
    data = None
    schema = None


    @classmethod
    def _create(self, model_class, spark_session, data, schema):
        return spark_session.createDataframe(data, schema)


class FunctionCall:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

        self.other_df = {}

        for k, v in kwargs.items():
            setattr(self, k, v)

    def __call__(self):
        return {"df": self.df, "other_df": self.other_df}

    def _as_expected(self):
        result = self()

        return result["df"]["data"]


class FunctionCallFactory(factory.Factory):
    class Meta:
        model = FunctionCall

    spark_session = LazyFixture("spark_session")

    df_data = None
    df_schema = None
    df = factory.LazyAttribute(lambda o: o.spark_session.createDataframe(o.df_data, o.df_schema))

    other_df_data = None
    other_df = factory.LazyAttribute(lambda o: o.spark_session.createDataframe(o.other_df_data, None))


register(FunctionCallFactory, "fc")


@pytest.mark.parametrize("fc__df_data,other_df__data", [
    ([1, 1], ["a", "b"]),
    (1, "a"),
])
def test_function_call(fc, spark_session, fc__df_data, other_df__data):
    assert fc.df["spark_session"] is spark_session

    assert fc._as_expected() == fc__df_data
