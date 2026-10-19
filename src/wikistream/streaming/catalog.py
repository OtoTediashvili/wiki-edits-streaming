"""The lake's Iceberg REST catalog, for Python readers: PyIceberg, no Spark.

`make peek`, the dashboard and the end-to-end test read the tables through
this; the Spark job has its own connection (session.spark_conf), with the
same settings.
"""

from __future__ import annotations

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog

from wikistream.streaming.settings import LakeSettings


def rest_catalog(settings: LakeSettings) -> Catalog:
    secret = settings.lake_secret_key.get_secret_value()
    return RestCatalog(
        "lake",
        **{
            "uri": settings.lake_catalog_uri,
            "warehouse": settings.lake_warehouse,
            "credential": f"{settings.lake_access_key}:{secret}",
            "oauth2-server-uri": f"{settings.lake_catalog_uri}/v1/oauth/tokens",
            "s3.endpoint": settings.lake_s3_endpoint,
            "s3.access-key-id": settings.lake_access_key,
            "s3.secret-access-key": secret,
            "s3.region": settings.lake_region,
        },
    )
