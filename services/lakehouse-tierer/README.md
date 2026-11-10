# Lakehouse Tierer

Long-term storage lifecycle service. This service will tier ClickHouse data
older than 30 days into compressed Apache Parquet files and publish them to S3
using Apache Iceberg table schemas.
