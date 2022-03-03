import tempfile

from feast import Client
from feast_spark import Client as JobClient
from feast_spark.job_service import JobServiceServicer
from feast_spark.pyspark.abc import SparkJobType


def test_feature_table_default_whitelist():
    feast_client = Client()
    job_client = JobClient(feast_client)
    job_servicer = JobServiceServicer(job_client)
    assert job_servicer.is_feature_table_whitelisted("some project", "some table")


def test_feature_table_whitelist():
    with tempfile.NamedTemporaryFile() as tmp:
        tmp.writelines([b"project1:table1\n", b"project1:table2"])
        tmp.seek(0)
        feast_client = Client(whitelisted_feature_tables_path=tmp.name)
        job_client = JobClient(feast_client)
        job_servicer = JobServiceServicer(job_client)
        assert not job_servicer.is_feature_table_whitelisted("project2", "table1")
        assert job_servicer.is_feature_table_whitelisted("project1", "table1")


def test_job_type_default_whitelist():
    feast_client = Client()
    job_client = JobClient(feast_client)
    job_servicer = JobServiceServicer(job_client)
    assert job_servicer.is_job_type_whitelisted(SparkJobType.STREAM_INGESTION)


def test_job_type_whitelist():
    feast_client = Client(whitelisted_job_types="STREAM_INGESTION,BATCH_INGESTION")
    job_client = JobClient(feast_client)
    job_servicer = JobServiceServicer(job_client)
    assert job_servicer.is_job_type_whitelisted(SparkJobType.STREAM_INGESTION)
    assert job_servicer.is_job_type_whitelisted(SparkJobType.BATCH_INGESTION)
    assert not job_servicer.is_job_type_whitelisted(SparkJobType.HISTORICAL_RETRIEVAL)
    assert not job_servicer.is_job_type_whitelisted(
        SparkJobType.SCHEDULED_BATCH_INGESTION
    )
