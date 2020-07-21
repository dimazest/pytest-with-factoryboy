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


import inspect
import dataclasses

from factory.base import FactoryMetaClass


class BaseFabric:
    pass


class DataFrame(BaseFabric):

    def __init__(self, data="some_data", schema="some_schema"):
        self.data = data
        self.schema = schema

    def attrs(self, name):
        return {
            f"{name}_data": self.data,
            f"{name}_schema": self.schema,
            name: factory.LazyAttribute(lambda o: o.spark_session.createDataframe(
                data=getattr(o, f"{name}_data"),
                schema=getattr(o, f"{name}_schema")
            )),
        }


class DFFactoryMetaClass(FactoryMetaClass):

    def __new__(mcs, class_name, bases, attrs):

        for attr, annotation in attrs.get("__annotations__", {}).items():
            if isinstance(annotation, BaseFabric):
                attrs.update(annotation.attrs(attr))

        return super().__new__(mcs, class_name, bases, attrs)


class DFFactory(factory.Factory, metaclass=DFFactoryMetaClass):
    pass



class FunctionCall():
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

        for k, v in kwargs.items():
            setattr(self, k, v)

    def __call__(self):
        return {"df": self.df, "other_df": self.other_df}

    def _as_expected(self):
        result = self()

        return result["df"]["data"]

    def create(self, *args, **kwargs):
        return super()._create(args, kwargs)


class FunctionCallFactory(DFFactory):
    class Meta:
        model = FunctionCall

    spark_session = LazyFixture("spark_session")

    df: DataFrame()
    other_df: DataFrame(schema=["id", "key", "value"])

    # df_data = None
    # df_schema = None
    # df = factory.LazyAttribute(lambda o: o.spark_session.createDataframe(o.df_data, o.df_schema))


register(FunctionCallFactory, "fc")


@pytest.mark.parametrize("fc__df_data,other_df__data", [
    ([1, 1], ["a", "b"]),
    (1, "a"),
])
def test_function_call(fc, spark_session, fc__df_data, other_df__data):
    assert fc.df["spark_session"] is spark_session

    assert fc._as_expected() == fc__df_data
    assert fc.other_df_schema == ["id", "key", "value"]
