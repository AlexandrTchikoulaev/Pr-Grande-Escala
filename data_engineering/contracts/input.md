# data_engineering — Input Contracts

Inputs consumed from `data_sources`. See [data_sources/contracts/README.md](../../data_sources/contracts/README.md).

| Source | Type | Detail |
|--------|------|--------|
| Kafka topic `clickstream_events` | Semi-structured JSON | Consumed by `ingestion/consumer.py` |
| Kafka topic `debezium.public.simulated_orders` | CDC JSON (Debezium) | Consumed by `ingestion/cdc_consumer.py` |
| MinIO bucket `raw-reviews/` | Unstructured `.txt` files | Polled by `ingestion/file_watcher.py` |
