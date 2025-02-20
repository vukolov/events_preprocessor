from pyspark.sql.functions import schema_of_json, lit, to_timestamp, col, from_json, window, mean, sum, collect_list, collect_set, first, udf, array_sort
from pyspark.ml.feature import StandardScalerModel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType
from pyspark.ml.functions import vector_to_array
from pyspark.ml.feature import VectorAssembler
import findspark
import os
from typing import Any
from application.data_stream_manager import AbstractDataStreamManager
from application.storages.abstract_source import AbstractSource
from application.storages.abstract_destination import AbstractDestination
from application.data_stream import DataStream
from application.data_frame import DataFrame
from adapters.data_stream.spark import Spark as SparkDataStreamAdapter
from application.usecases.preprocessor import Preprocessor
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
        self._plan = None
        self._session = (SparkSession.builder
                         .appName("NestedAggregationExample")
                         .master("local[*]")
                         .config("spark.executor.memory", "2g")
                         # .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") # we can lose some late messages with this option
                         .getOrCreate())
        self._session.conf.set("spark.sql.debug.maxToStringFields", 1000)

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
        return SparkDataStreamAdapter.to_data_stream(read_stream)

    def get_aggregation_intervals(self, active_metric_groups: dict[str, Any]) -> DataFrame:
        interval_schema = StructType([
            StructField("metric_group_uid", StringType(), True),
            StructField("window_interval_seconds", IntegerType(), True)
        ])
        return DataFrame(self._session.createDataFrame(
            [(group, conf["agg_interval_ms"]) for group, conf in active_metric_groups.items()],
            interval_schema
        ))

    def get_active_metrics_in_groups(self, active_metric_groups: dict[str, Any]) -> DataFrame:
        active_metrics_schema = StructType([
            StructField("metric_group_uid", StringType(), True),
            StructField("active_metric_uid", StringType(), True)
        ])
        return DataFrame(self._session.createDataFrame(
            [(group, metric) for group, conf in active_metric_groups.items() for metric in conf["metrics"]],
            active_metrics_schema
        ))

    def aggregate_metrics(self, data_stream: DataStream, interval_config: DataFrame, metrics_in_groups: DataFrame) -> DataStream:
        parsed_stream = SparkDataStreamAdapter.to_spark_stream(data_stream)
        metrics_in_groups_df = metrics_in_groups.get_frame_object()
        joined_stream = parsed_stream \
            .join(metrics_in_groups_df, parsed_stream.metric_group_uid == metrics_in_groups_df.metric_group_uid) \
            .select(parsed_stream.metric_group_uid,
                    parsed_stream.metric_uid,
                    parsed_stream.metric_value,
                    parsed_stream.event_time,
                    metrics_in_groups_df.active_metric_uid.alias("active_metric_uid"))

        aggregated = joined_stream \
            .withWatermark("event_time", "30 seconds") \
            .groupBy(
                col("metric_group_uid"),
                col("metric_uid"),
                window(col("event_time"), "5 seconds", "5 seconds")
            ).agg(
                mean("metric_value").alias("avg_metric_value"),
                sum("metric_value").alias("sum_metric_value"),
                array_sort(collect_set("active_metric_uid")).alias("indicators")
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
            "avg_scaled",
            "indicators"
        )
        aggregated_df = scaled_df.groupBy("metric_group_uid", "window") \
            .agg(
                collect_list("metric_uid").alias("metric_ids"),
                collect_list("sum_metric_value").alias("sum_metric_values"),
                collect_list("avg_metric_value").alias("avg_metric_values"),
                collect_list("sum_scaled").alias("sum_scaled"),
                collect_list("avg_scaled").alias("avg_scaled"),
                first("indicators").alias("sorted_metric_ids")
            )

        return SparkDataStreamAdapter.to_data_stream(aggregated_df)

    def fill_empty_metrics_and_sort(self, data_stream: DataStream) -> DataStream:
        ds = SparkDataStreamAdapter.to_spark_stream(data_stream)

        def sort_and_fill_empty(arr_to_modify: list, existed_metrics: list, target_metrics: list):
            res_arr = []
            for i in range(len(target_metrics)):
                if target_metrics[i] in existed_metrics:
                    res_arr.append(arr_to_modify[existed_metrics.index(target_metrics[i])])
                else:
                    res_arr.append(Preprocessor.DEFAULT_VALUE_FOR_EMPTY_METRIC)
            return res_arr

        sort_and_fill_empty_udf = udf(sort_and_fill_empty, ArrayType(DoubleType()))

        ds = ds \
            .withColumn("sum_scaled_prepared", sort_and_fill_empty_udf(col("sum_scaled"), col("metric_ids"), col("sorted_metric_ids"))) \
            .withColumn("avg_scaled_prepared", sort_and_fill_empty_udf(col("avg_scaled"), col("metric_ids"), col("sorted_metric_ids")))

        # you can comment the following string to see more fields in the output (for debug)
        ds = ds.select("metric_group_uid", "window", "sorted_metric_ids", "sum_scaled_prepared", "avg_scaled_prepared")

        return SparkDataStreamAdapter.to_data_stream(ds)


    def set_checkpoint_destination(self, data_stream: DataStream, checkpoint_destination: str) -> DataStream:
        stream = SparkDataStreamAdapter.to_spark_stream(data_stream)
        stream = stream.option("checkpointLocation", checkpoint_destination)
        return SparkDataStreamAdapter.to_data_stream(stream)

    def set_destination(self, data_stream: DataStream, stream_destination: AbstractDestination) -> DataStream:
        stream = SparkDataStreamAdapter.to_spark_stream(data_stream)
        stream = stream.writeStream
        # stream.outputMode("update")
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
