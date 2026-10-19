"""The stream job: Redpanda → Spark Structured Streaming → Iceberg.

Runs as `python -m wikistream.streaming run` in the Spark image. Needs the
`spark` dependency group (PySpark) and, at runtime, the Kafka, Avro and
Iceberg connector jars listed in `session.PACKAGES`.
"""
