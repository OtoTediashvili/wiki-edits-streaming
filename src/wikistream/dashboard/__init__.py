"""The live dashboard: Streamlit over the Iceberg tables the stream job writes.

Runs as `python -m wikistream.dashboard` in its own image, with only the
`dashboard` dependency group: PyIceberg, DuckDB and Streamlit, no Spark.
"""
