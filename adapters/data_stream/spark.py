from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.streaming import DataStreamWriter as SparkDataStreamWriter
from application.data_stream import DataStream


class Spark:
    @staticmethod
    def to_data_stream(spark_stream_object: SparkDataFrame | SparkDataStreamWriter) -> DataStream:
        return DataStream(spark_stream_object)

    @staticmethod
    def to_spark_stream(data_stream: DataStream) -> SparkDataFrame | SparkDataStreamWriter:
        return data_stream.get_stream_object()
