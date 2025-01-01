# Events Preprocessor

## Overview
The **Events Preprocessor** is a microservice designed to handle the initial processing of incoming messages. It reads raw messages from a data source, aggregates them based on time windows, normalizes and standardizes metric values, and writes the enriched and aggregated messages back to the data source.

## Features
- **Message Aggregation**: Collects and aggregates raw messages by specified time intervals.  
- **Normalization and Standardization**: Ensures metric values conform to predefined formats and ranges.  
- **Data Enrichment**: Augments aggregated data with normalized values.  
- **Efficient Processing**: Handles high-throughput data streams with minimal latency.  

## Architecture
1. **Input**: Raw messages are ingested from a data source (e.g., Kafka, S3, etc.).  
2. **Processing**: Messages are aggregated by time window and normalized using configured rules.  
3. **Output**: The processed messages are stored in the target data source.  

## Requirements
...