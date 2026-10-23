"""The recorded events as DataFrame rows, in the shape `transform.decode` gives."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from pyspark.sql import types as T

from wikistream.model import decode, is_canary, schema_str, to_record
from wikistream.sse import SseEvent

_AVRO_TO_SPARK: dict[str, T.DataType] = {
    "string": T.StringType(),
    "int": T.IntegerType(),
    "long": T.LongType(),
    "boolean": T.BooleanType(),
}


def _spark_type(avro_type: Any) -> tuple[T.DataType, bool]:
    """Spark type and nullability that from_avro gives an Avro field type."""
    if isinstance(avro_type, list):
        (inner,) = [t for t in avro_type if t != "null"]
        return _spark_type(inner)[0], True
    if isinstance(avro_type, dict):
        if avro_type.get("logicalType") == "timestamp-millis":
            return T.TimestampType(), False
        return _spark_type(avro_type["type"])
    return _AVRO_TO_SPARK[avro_type], False


def decoded_schema() -> T.StructType:
    """What `transform.decode` produces: the Avro fields, then Kafka's, then
    whether the record decoded. Every Avro field is nullable: from_avro's
    PERMISSIVE mode returns nulls for a record it can't read."""
    fields = [
        T.StructField(f["name"], _spark_type(f["type"])[0], True)
        for f in json.loads(schema_str())["fields"]
    ]
    fields += [
        T.StructField("kafka_partition", T.IntegerType()),
        T.StructField("kafka_offset", T.LongType()),
        T.StructField("kafka_timestamp", T.TimestampType()),
        T.StructField("decoded", T.BooleanType()),
        T.StructField("raw_value", T.BinaryType()),
    ]
    return T.StructType(fields)


def undecodable(value: bytes | None, partition: int, offset: int) -> dict[str, Any]:
    """A record from_avro couldn't read: every field null, the bytes kept."""
    row: dict[str, Any] = {field.name: None for field in decoded_schema().fields}
    return {
        **row,
        "kafka_partition": partition,
        "kafka_offset": offset,
        "kafka_timestamp": datetime(2026, 10, 22, 18, 0),
        "decoded": False,
        "raw_value": value,
    }


def _ts(millis: int) -> datetime:
    return datetime.fromtimestamp(millis / 1000, tz=UTC).replace(tzinfo=None)


def decoded_rows(events: list[SseEvent]) -> list[dict[str, Any]]:
    """The recorded events as the job sees them after decoding."""
    rows = []
    for offset, sse in enumerate(e for e in events if e.event == "message"):
        event = decode(sse.data)
        if is_canary(event):
            continue
        row = to_record(event, sse.data)
        row["event_time"] = _ts(row["event_time"])
        row["stream_time"] = _ts(row["stream_time"])
        row.update(
            kafka_partition=offset % 3,
            kafka_offset=offset,
            kafka_timestamp=row["stream_time"],
            decoded=True,
            raw_value=None,
        )
        rows.append(row)
    return rows
