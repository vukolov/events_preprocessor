# Events Preprocessor

## Overview
The **Events Preprocessor** is a microservice designed to handle the initial processing of incoming messages. It reads raw messages from a data source, aggregates them based on time windows, normalizes and standardizes metric values, and writes the enriched and aggregated messages back to the next storage of the data pipeline.

## Features
- **Message Aggregation**: Collects and aggregates raw messages by specified time intervals.  
- **Normalization and Standardization**: Ensures metric values conform to predefined formats and ranges.  
- **Data Enrichment**: Augments aggregated data with normalized values.  
- **Efficient Processing**: Handles high-throughput data streams with minimal latency.  

## Architecture
1. **Input**: Raw messages are ingested from a data source (e.g., Kafka, S3, etc.).
2. **Processing**: Messages are aggregated by time window and normalized using configured rules.  
3. **Output**: The processed messages are stored in the target data source.  

## Messages format
Format of the input message is the following:
```json
{
  "metric_group_uid": "group_1",
  "metric_uid": "metric_101",
  "event_time": "2024-12-01T10:15:30Z",
  "metric_value": 45.7
}
```
Format of the output message:
```
+----------------+------------------------------------------+------------------------------------+----------------------------------------------------------+--------------------------------------------------------------+
|metric_group_uid|window                                    |sorted_metric_ids                   |sum_scaled_prepared                                       |avg_scaled_prepared                                           |
+----------------+------------------------------------------+------------------------------------+----------------------------------------------------------+--------------------------------------------------------------+
|group_2         |{2024-12-24 08:05:00, 2024-12-24 08:05:05}|[metric_201, metric_202]            |[-999999.0, 5.0192277459266235]                           |[-999999.0, 1.1845406531046625]                               |
|group_3         |{2024-12-24 07:12:40, 2024-12-24 07:12:45}|[metric_301, metric_302]            |[2.085111106721942, -999999.0]                            |[-0.2825176664976787, -999999.0]                              |
|group_1         |{2024-12-24 07:15:30, 2024-12-24 07:15:35}|[metric_101, metric_102, metric_103]|[2.8840042510598503, 3.546359294365461, 2.807746269100323]|[-1.7277879912544407, -1.617395484036839, -0.8308488701114252]|
|group_2         |{2024-12-24 08:00:00, 2024-12-24 08:00:05}|[metric_201, metric_202]            |[4.489053204684194, -999999.0]                            |[0.9194533824834474, -999999.0]                               |
+----------------+------------------------------------------+------------------------------------+----------------------------------------------------------+--------------------------------------------------------------+

```
Here -999999.0 is a placeholder for missing values. You can change this value in Processor.DEFAULT_VALUE_FOR_EMPTY_METRIC constant.
## Requirements
...