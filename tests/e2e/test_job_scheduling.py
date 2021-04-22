import pytest as pytest

from feast import Entity, Feature, FeatureTable, FileSource, ValueType
from feast.data_format import ParquetFormat
from feast_spark import Client as SparkClient


@pytest.mark.env("k8s")
def test_schedule_batch_ingestion_jobs(
    pytestconfig, feast_spark_client: SparkClient
):
    entity = Entity(name="s2id", description="S2id", value_type=ValueType.INT64,)
    batch_source = FileSource(
        file_format=ParquetFormat(),
        file_url="gs://example/feast/*",
        event_timestamp_column="datetime_col",
        created_timestamp_column="timestamp",
        date_partition_column="datetime",
    )
    feature_table = FeatureTable(
        name="drivers",
        entities=["s2id"],
        features=[Feature("unique_drivers", ValueType.INT64)],
        batch_source=batch_source,
    )

    feast_spark_client.schedule_offline_to_online_ingestion(feature_table, 1, "0 0 * * *")
    feast_spark_client.unschedule_offline_to_online_ingestion(feature_table, feast_spark_client._feast.project)
