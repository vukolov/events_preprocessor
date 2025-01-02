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
```json
{
  "metric_group_uid":"group_1",
  "metric_uid":"metric_101",
  "window":{
    "start":"2024-12-24T07:15:30.000+01:00",
    "end":"2024-12-24T07:15:35.000+01:00"
  },
  "sum_metric_value":45.7,
  "avg_metric_value":45.7,
  "sum_scaled":-0.9906274989790068,
  "avg_scaled":-0.9906274989790068
}

```
## Requirements
...