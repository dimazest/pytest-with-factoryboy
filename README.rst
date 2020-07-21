Pytest, factoryboy and spark
============================

Test session
------------

::

  pytest test_pytest_with_factoryboy.py -vlx
  ================================================ test session starts ================================================
  platform darwin -- Python 3.6.8, pytest-4.0.2, py-1.7.0, pluggy-0.8.0 -- /Users/dimazest/miniconda3/bin/python
  cachedir: .pytest_cache
  rootdir: /Users/dimazest/pytest-with-factoryboy, inifile:
  plugins: remotedata-0.3.1, openfiles-0.3.1, doctestplus-0.1.3, arraydiff-0.3, Faker-4.1.1, factoryboy-2.0.3
  collected 3 items

  test_pytest_with_factoryboy.py::test_data_frame ERROR                                                         [ 33%]

  ====================================================== ERRORS =======================================================
  _________________________________________ ERROR at setup of test_data_frame _________________________________________

  self = <class 'pytest_factoryboy.fixture.model_fixture.<locals>.Factory'>
  model_class = <class 'test_pytest_with_factoryboy.DataFrame'>
  spark_session = <test_pytest_with_factoryboy.spark_session.<locals>.SparkSession object at 0x10674e748>, data = None
  schema = None

      @classmethod
      def _create(self, model_class, spark_session, data, schema):
  >       return spark_session.createDataframe(data, schema)

  data       = None
  model_class = <class 'test_pytest_with_factoryboy.DataFrame'>
  schema     = None
  self       = <class 'pytest_factoryboy.fixture.model_fixture.<locals>.Factory'>
  spark_session = <test_pytest_with_factoryboy.spark_session.<locals>.SparkSession object at 0x10674e748>

  test_pytest_with_factoryboy.py:33:
  _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _

  self = <test_pytest_with_factoryboy.spark_session.<locals>.SparkSession object at 0x10674e748>, data = None
  schema = None

      def createDataframe(self, data, schema):
  >       assert data is not None
  E       assert None is not None

  data       = None
  schema     = None
  self       = <test_pytest_with_factoryboy.spark_session.<locals>.SparkSession object at 0x10674e748>

  test_pytest_with_factoryboy.py:11: AssertionError
  ============================================== 1 error in 0.12 seconds ==============================================

Fixtures
--------

::

  data_frame
      <string>:3: no docstring available
  data_frame__data
      <string>:3: no docstring available
  data_frame__schema
      <string>:3: no docstring available
  data_frame__spark_session
      <string>:3: no docstring available
  data_frame_factory
      <string>:3: no docstring available
  fc
      <string>:3: no docstring available
  fc__df
      <string>:3: no docstring available
  fc__other_df
      <string>:3: no docstring available
  function_call_factory
      <string>:3: no docstring available
  spark_session
      test_pytest_with_factoryboy.py:8: no docstring available
