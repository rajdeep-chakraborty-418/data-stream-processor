"""
Pyspark Transformer Module Interface
"""
import types
from typing import (
    List,
)
from pyspark.sql import (
    SparkSession,
    DataFrame,
)

from src.main.utils.constants import (
    PYSPARK_RUN_METHOD,
)


class InterfacePysparkScriptExecutor:
    """
    Interface For Pyspark Transformer Module
    """
    def __init__(self, spark: SparkSession, script: str, table_names: List[str] = None):
        """
        Initialize the PysparkScriptExecutor with Spark session, script string, and optional table names.
        :param spark:
        :param script:
        :param table_names:
        """
        self.spark = spark
        self.script = script
        self.pyspark_run_method = PYSPARK_RUN_METHOD
        self.table_names = table_names or []
        self.module = self.__load_module_with_dataframe_bindings()
        self.validate_methods = [self.pyspark_run_method]
        self.__validate_required_functions(self.validate_methods)

    def __load_module_with_dataframe_bindings(self) -> types.ModuleType:
        """
        Load Module from Spark String and register spark tables
        :return:
        """
        module = types.ModuleType("user_module")
        """
        Register Spark DataFrames as tables of the module in memory
        """
        for table_name in self.table_names:
            df = self.spark.read.table(table_name)
            setattr(module, table_name, df)
        """
        Create a Custom Context and Load Methods into the Module
        """
        context_namespace = {"spark": self.spark, **module.__dict__}
        exec(self.script, context_namespace)

        for name, value in context_namespace.items():
            if callable(value):
                setattr(module, name, value)

        return module

    def __validate_required_functions(self, validate_methods: list):
        """
        Validate that the required methods are defined in the module
        :param validate_methods:
        :return:
        """
        for method_name in validate_methods:
            if not hasattr(self.module, method_name):
                raise AttributeError(f"""The script must define a {method_name} function""")
            method = getattr(self.module, method_name)
            if not callable(method):
                raise TypeError(f"""{method_name} must be callable""")

    def run(self) -> DataFrame:
        """
        Execute the transform method from the loaded module
        :return:
        """
        transform = getattr(self.module, self.pyspark_run_method)
        return transform(self.spark)
