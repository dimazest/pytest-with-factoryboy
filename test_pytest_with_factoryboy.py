import factory

import pytest
import pytest_factoryboy


@pytest.fixture
def spark_session():
    class SparkSession:
        def createDataframe(self, data, schema):
            assert data is not None

            return {"spark_session": self, "data": data, "schema": schema}

    return SparkSession()


class DataFrame:
    pass


class DataFrameFactory(factory.Factory):
    class Meta:
        model = DataFrame

    spark_session = pytest_factoryboy.LazyFixture("spark_session")
    data = None
    schema = None


    @classmethod
    def _create(self, model_class, spark_session, data, schema):
        return spark_session.createDataframe(data, schema)


pytest_factoryboy.register(DataFrameFactory)


def test_data_frame(data_frame, spark_session):
    assert data_frame["spark_session"] is spark_session


class FunctionCall:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

        for k, v in kwargs.items():
            setattr(self, k, v)


    def __call__(self):
        return {"df": self.df, "other_df": self.other_df}


class FunctionCallFactory(factory.Factory):
    class Meta:
        model = FunctionCall

    df = factory.SubFactory(DataFrameFactory)
    other_df = factory.SubFactory(DataFrameFactory)


pytest_factoryboy.register(FunctionCallFactory, "fc")


@pytest.mark.parametrize("fc__df__data,fc__other_df__data", [
    ([1, 1], ["a", "b"]),
    (1, "a"),
])
def test_function_call(fc, spark_session, fc__df__data, fc__other_df__data):
    assert fc.df["spark_session"] is spark_session

    result = fc()

    assert result["df"]["data"] == fc__df__data
