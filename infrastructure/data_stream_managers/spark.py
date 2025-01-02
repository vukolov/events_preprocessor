from pyspark.sql.functions import col, window, avg, udf, expr, sum, schema_of_json, from_json, lit, to_timestamp
from pyspark.ml.feature import StandardScalerModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, window, mean, sum
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, IntegerType
from pyspark.ml.functions import vector_to_array
from pyspark.ml.feature import VectorAssembler
import findspark
import os
from typing import List, Tuple
from application.data_stream_manager import AbstractDataStreamManager
from application.storages.abstract_source import AbstractSource
from application.storages.abstract_destination import AbstractDestination
from application.data_stream import DataStream
from application.data_frame import DataFrame
from adapters.data_stream.spark import Spark as SparkDataStreamAdapter
from infrastructure.storages.sources.kafka import Kafka as SourceKafka
from infrastructure.storages.sources.file import File as SourceFile
from infrastructure.storages.destinations.kafka import Kafka as DestinationKafka
from infrastructure.storages.destinations.console import Console as DestinationConsole
from infrastructure.storages.destinations.memory import Memory as DestinationMemory
from infrastructure.storages.destinations.file import File as DestinationFile


class Spark(AbstractDataStreamManager):
    def __init__(self):
        os.environ['SPARK_HOME'] = '/opt/homebrew/Cellar/apache-spark/3.5.3/libexec'
        os.environ['JAVA_HOME'] = '/opt/homebrew/opt/openjdk@17'
        os.environ['PATH'] = f"{os.environ['SPARK_HOME']}/bin:" + f"{os.environ['JAVA_HOME']}/bin:" + os.environ['PATH']
        findspark.init()
        # self._frequency_seconds = 2
        self._plan = None
        self._session = (SparkSession.builder
                         .appName("NestedAggregationExample")
                         .master("local[*]")
                         .config("spark.executor.memory", "2g")
                         .getOrCreate())
        self._session.conf.set("spark.sql.debug.maxToStringFields", 1000)
        # self._destination = None

    def subscribe_on_source(self, stream_source: AbstractSource) -> DataStream:
        read_stream = self._session.readStream
        if isinstance(stream_source, SourceKafka):
            read_stream = read_stream.format("kafka") \
                .option("kafka.bootstrap.servers", stream_source.get_address()) \
                .option("subscribe", stream_source.get_topic_name()) \
                .load()
        elif isinstance(stream_source, SourceFile):
            read_stream = read_stream.format("text") \
                .option("maxFilesPerTrigger", 1) \
                .load(stream_source.get_address())

        read_stream = read_stream.selectExpr("CAST(value AS STRING) as string_repr")
        if stream_source.get_message_format() == 'avro':
            read_stream.selectExpr(f"from_avro(string_repr, '{stream_source.get_message_schema()}') as data")
        elif stream_source.get_message_format() == 'json':
            schema = schema_of_json(lit(stream_source.get_message_schema()))
            read_stream = read_stream.select(from_json("string_repr", schema).alias("data"))

        read_stream = read_stream \
            .select("data.metric_group_uid", "data.metric_uid", "data.metric_value", "data.event_time") \
            .withColumn(
                "event_time", to_timestamp("event_time", "yyyy-MM-dd'T'HH:mm:ssX")
            )
        # .withColumn("event_time", col("event_time").cast(TimestampType()))
        return SparkDataStreamAdapter.to_data_stream(read_stream)

    def get_aggregation_intervals(self, aggregation_intervals: List[Tuple]) -> DataFrame:
        interval_schema = StructType([
            StructField("metric_group_uid", StringType(), True),
            StructField("window_interval_seconds", IntegerType(), True)
        ])
        return self._session.createDataFrame(aggregation_intervals, interval_schema)

    def aggregate_metrics(self, data_stream: DataStream, interval_config: DataFrame) -> DataStream:
        parsed_stream = SparkDataStreamAdapter.to_spark_stream(data_stream)
        # joined_stream = parsed_stream.join(interval_config, "metric_group_uid")

        aggregated = parsed_stream \
            .withWatermark("event_time", "30 seconds") \
            .groupBy(
                col("metric_group_uid"),
                col("metric_uid"),
                window(col("event_time"), "5 seconds", "5 seconds")
            ).agg(
                mean("metric_value").alias("avg_metric_value"),
                sum("metric_value").alias("sum_metric_value")
            )
        return SparkDataStreamAdapter.to_data_stream(aggregated)

    def normalize_metrics_values(self, data_stream: DataStream, normalization_model_path: str) -> DataStream:
        aggregated = SparkDataStreamAdapter.to_spark_stream(data_stream)

        scaler_model = StandardScalerModel.load(normalization_model_path)

        assembler = VectorAssembler(inputCols=["sum_metric_value", "avg_metric_value"], outputCol="features")
        assembled_df = assembler.transform(aggregated)

        scaled_df = scaler_model.transform(assembled_df) \
            .withColumn("sum_scaled", vector_to_array("scaled_features")[0]) \
            .withColumn("avg_scaled", vector_to_array("scaled_features")[1]) \
            .select(
            "metric_group_uid",
            "metric_uid",
            "window",
            "sum_metric_value",
            "avg_metric_value",
            "sum_scaled",
            "avg_scaled"
        )
        return SparkDataStreamAdapter.to_data_stream(scaled_df)

    def set_checkpoint_destination(self, data_stream: DataStream, checkpoint_destination: str) -> DataStream:
        stream = SparkDataStreamAdapter.to_spark_stream(data_stream)
        stream = stream.option("checkpointLocation", checkpoint_destination)
        return SparkDataStreamAdapter.to_data_stream(stream)

    def set_destination(self, data_stream: DataStream, stream_destination: AbstractDestination) -> DataStream:
        stream = SparkDataStreamAdapter.to_spark_stream(data_stream)
        stream = stream.writeStream
        if isinstance(stream_destination, DestinationKafka):
            stream.format("kafka") \
                .option("kafka.bootstrap.servers", stream_destination.get_address()) \
                .option("topic", stream_destination.get_topic_name())
        elif isinstance(stream_destination, DestinationConsole):
            stream.format("console") \
                .option("truncate", "false")
        elif isinstance(stream_destination, DestinationMemory):
            stream.format("memory") \
                .queryName(stream_destination.get_topic_name())
        elif isinstance(stream_destination, DestinationFile):
            stream.format("json") \
                .option("path", stream_destination.get_address()) \
                .trigger(availableNow=True)
        return SparkDataStreamAdapter.to_data_stream(stream)

    def start(self, data_stream: DataStream):
        stream = SparkDataStreamAdapter.to_spark_stream(data_stream)
        stream.start().awaitTermination()

    def stop(self):
        self._session.stop()
