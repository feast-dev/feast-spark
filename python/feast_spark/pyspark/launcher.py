import json
import os
import tempfile
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Dict, List, Optional, Union
from urllib.parse import urlparse, urlunparse

from feast.config import Config
from feast.data_format import ParquetFormat
from feast.data_source import BigQuerySource, DataSource, FileSource, KafkaSource
from feast.feature_table import FeatureTable
from feast.staging.storage_client import get_staging_client
from feast.value_type import ValueType
from feast_spark.constants import ConfigOptions as opt
from feast_spark.pyspark.abc import (
    BatchIngestionJob,
    BatchIngestionJobParameters,
    JobLauncher,
    RetrievalJob,
    RetrievalJobParameters,
    ScheduledBatchIngestionJobParameters,
    SparkJob,
    StreamIngestionJob,
    StreamIngestionJobParameters,
)

if TYPE_CHECKING:
    from feast_spark.client import Client


def _standalone_launcher(config: Config) -> JobLauncher:
    from feast_spark.pyspark.launchers import standalone

    return standalone.StandaloneClusterLauncher(
        config.get(opt.SPARK_STANDALONE_MASTER), config.get(opt.SPARK_HOME),
    )


def _dataproc_launcher(config: Config) -> JobLauncher:
    from feast_spark.pyspark.launchers import gcloud

    return gcloud.DataprocClusterLauncher(
        cluster_name=config.get(opt.DATAPROC_CLUSTER_NAME),
        staging_location=config.get(opt.SPARK_STAGING_LOCATION),
        region=config.get(opt.DATAPROC_REGION),
        project_id=config.get(opt.DATAPROC_PROJECT),
        executor_instances=config.get(opt.DATAPROC_EXECUTOR_INSTANCES),
        executor_cores=config.get(opt.DATAPROC_EXECUTOR_CORES),
        executor_memory=config.get(opt.DATAPROC_EXECUTOR_MEMORY),
    )


def _emr_launcher(config: Config) -> JobLauncher:
    from feast_spark.pyspark.launchers import aws

    def _get_optional(option):
        if config.exists(option):
            return config.get(option)

    return aws.EmrClusterLauncher(
        region=config.get(opt.EMR_REGION),
        existing_cluster_id=_get_optional(opt.EMR_CLUSTER_ID),
        new_cluster_template_path=_get_optional(opt.EMR_CLUSTER_TEMPLATE_PATH),
        staging_location=config.get(opt.SPARK_STAGING_LOCATION),
        emr_log_location=config.get(opt.EMR_LOG_LOCATION),
    )


def _k8s_launcher(config: Config) -> JobLauncher:
    from feast_spark.pyspark.launchers import k8s

    staging_location = config.get(opt.SPARK_STAGING_LOCATION)
    staging_uri = urlparse(staging_location)

    return k8s.KubernetesJobLauncher(
        namespace=config.get(opt.SPARK_K8S_NAMESPACE),
        generic_resource_template_path=config.get(opt.SPARK_K8S_JOB_TEMPLATE_PATH),
        batch_ingestion_resource_template_path=config.get(
            opt.SPARK_K8S_BATCH_INGESTION_TEMPLATE_PATH, None
        ),
        stream_ingestion_resource_template_path=config.get(
            opt.SPARK_K8S_STREAM_INGESTION_TEMPLATE_PATH, None
        ),
        historical_retrieval_resource_template_path=config.get(
            opt.SPARK_K8S_HISTORICAL_RETRIEVAL_TEMPLATE_PATH, None
        ),
        staging_location=staging_location,
        incluster=config.getboolean(opt.SPARK_K8S_USE_INCLUSTER_CONFIG),
        staging_client=get_staging_client(staging_uri.scheme, config),
    )


_launchers = {
    "standalone": _standalone_launcher,
    "dataproc": _dataproc_launcher,
    "emr": _emr_launcher,
    "k8s": _k8s_launcher,
}


def resolve_launcher(config: Config) -> JobLauncher:
    return _launchers[config.get(opt.SPARK_LAUNCHER)](config)


def _source_to_argument(source: DataSource, config: Config):
    common_properties = {
        "field_mapping": dict(source.field_mapping),
        "event_timestamp_column": source.event_timestamp_column,
        "created_timestamp_column": source.created_timestamp_column,
        "date_partition_column": source.date_partition_column,
    }

    properties = {**common_properties}

    if isinstance(source, FileSource):
        properties["path"] = source.file_options.file_url
        properties["format"] = dict(
            json_class=source.file_options.file_format.__class__.__name__
        )
        return {"file": properties}

    if isinstance(source, BigQuerySource):
        project, dataset_and_table = source.bigquery_options.table_ref.split(":")
        dataset, table = dataset_and_table.split(".")
        properties["project"] = project
        properties["dataset"] = dataset
        properties["table"] = table
        if config.exists(opt.SPARK_BQ_MATERIALIZATION_PROJECT) and config.exists(
            opt.SPARK_BQ_MATERIALIZATION_DATASET
        ):
            properties["materialization"] = dict(
                project=config.get(opt.SPARK_BQ_MATERIALIZATION_PROJECT),
                dataset=config.get(opt.SPARK_BQ_MATERIALIZATION_DATASET),
            )

        return {"bq": properties}

    if isinstance(source, KafkaSource):
        properties["bootstrap_servers"] = source.kafka_options.bootstrap_servers
        properties["topic"] = source.kafka_options.topic
        properties["format"] = {
            **source.kafka_options.message_format.__dict__,
            "json_class": source.kafka_options.message_format.__class__.__name__,
        }
        return {"kafka": properties}

    raise NotImplementedError(f"Unsupported Datasource: {type(source)}")


def _feature_table_to_argument(
    client: "Client", project: str, feature_table: FeatureTable, use_gc_threshold=True,
):
    max_age = feature_table.max_age.ToSeconds() if feature_table.max_age else None
    if use_gc_threshold:
        try:
            gc_threshold = int(feature_table.labels["gcThresholdSec"])
        except (KeyError, ValueError, TypeError):
            pass
        else:
            max_age = max(max_age or 0, gc_threshold)

    return {
        "features": [
            {"name": f.name, "type": ValueType(f.dtype).name}
            for f in feature_table.features
        ],
        "project": project,
        "name": feature_table.name,
        "entities": [
            {
                "name": n,
                "type": client.feature_store.get_entity(n, project=project).value_type,
            }
            for n in feature_table.entities
        ],
        "max_age": max_age,
        "labels": dict(feature_table.labels),
    }


def start_historical_feature_retrieval_spark_session(
    client: "Client",
    project: str,
    entity_source: Union[FileSource, BigQuerySource],
    feature_tables: List[FeatureTable],
):
    from pyspark.sql import SparkSession

    from feast_spark.pyspark.historical_feature_retrieval_job import (
        retrieve_historical_features,
    )

    spark_session = SparkSession.builder.getOrCreate()
    return retrieve_historical_features(
        spark=spark_session,
        entity_source_conf=_source_to_argument(entity_source, client.config),
        feature_tables_sources_conf=[
            _source_to_argument(feature_table.batch_source, client.config)
            for feature_table in feature_tables
        ],
        feature_tables_conf=[
            _feature_table_to_argument(
                client, project, feature_table, use_gc_threshold=False
            )
            for feature_table in feature_tables
        ],
    )


def start_historical_feature_retrieval_job(
    client: "Client",
    project: str,
    entity_source: Union[FileSource, BigQuerySource],
    feature_tables: List[FeatureTable],
    output_format: str,
    output_path: str,
) -> RetrievalJob:
    launcher = resolve_launcher(client.config)
    feature_sources = [
        _source_to_argument(feature_table.batch_source, client.config,)
        for feature_table in feature_tables
    ]

    return launcher.historical_feature_retrieval(
        RetrievalJobParameters(
            project=project,
            entity_source=_source_to_argument(entity_source, client.config),
            feature_tables_sources=feature_sources,
            feature_tables=[
                _feature_table_to_argument(
                    client, project, feature_table, use_gc_threshold=False
                )
                for feature_table in feature_tables
            ],
            destination={"format": output_format, "path": output_path},
            checkpoint_path=client.config.get(opt.CHECKPOINT_PATH),
        )
    )


def table_reference_from_string(table_ref: str):
    """
    Parses reference string with format "{project}:{dataset}.{table}" into bigquery.TableReference
    """
    from google.cloud import bigquery

    project, dataset_and_table = table_ref.split(":")
    dataset, table_id = dataset_and_table.split(".")
    return bigquery.TableReference(
        bigquery.DatasetReference(project, dataset), table_id
    )


def start_offline_to_online_ingestion(
    client: "Client",
    project: str,
    feature_table: FeatureTable,
    start: datetime,
    end: datetime,
) -> BatchIngestionJob:

    launcher = resolve_launcher(client.config)

    return launcher.offline_to_online_ingestion(
        BatchIngestionJobParameters(
            jar=client.config.get(opt.SPARK_INGESTION_JAR),
            source=_source_to_argument(feature_table.batch_source, client.config),
            feature_table=_feature_table_to_argument(client, project, feature_table),
            start=start,
            end=end,
            redis_host=client.config.get(opt.REDIS_HOST),
            redis_port=bool(client.config.get(opt.REDIS_HOST))
            and client.config.getint(opt.REDIS_PORT),
            redis_password=client.config.get(opt.REDIS_PASSWORD),
            redis_ssl=client.config.getboolean(opt.REDIS_SSL),
            bigtable_project=client.config.get(opt.BIGTABLE_PROJECT),
            bigtable_instance=client.config.get(opt.BIGTABLE_INSTANCE),
            cassandra_host=client.config.get(opt.CASSANDRA_HOST),
            cassandra_port=bool(client.config.get(opt.CASSANDRA_HOST))
            and client.config.getint(opt.CASSANDRA_PORT),
            statsd_host=(
                client.config.getboolean(opt.STATSD_ENABLED)
                and client.config.get(opt.STATSD_HOST)
            ),
            statsd_port=(
                client.config.getboolean(opt.STATSD_ENABLED)
                and client.config.getint(opt.STATSD_PORT)
            ),
            deadletter_path=client.config.get(opt.DEADLETTER_PATH),
            stencil_url=client.config.get(opt.STENCIL_URL),
        )
    )


def schedule_offline_to_online_ingestion(
    client: "Client",
    project: str,
    feature_table: FeatureTable,
    ingestion_timespan: int,
    cron_schedule: str,
):

    launcher = resolve_launcher(client.config)

    launcher.schedule_offline_to_online_ingestion(
        ScheduledBatchIngestionJobParameters(
            jar=client.config.get(opt.SPARK_INGESTION_JAR),
            source=_source_to_argument(feature_table.batch_source, client.config),
            feature_table=_feature_table_to_argument(client, project, feature_table),
            ingestion_timespan=ingestion_timespan,
            cron_schedule=cron_schedule,
            redis_host=client.config.get(opt.REDIS_HOST),
            redis_port=bool(client.config.get(opt.REDIS_HOST))
            and client.config.getint(opt.REDIS_PORT),
            redis_password=client.config.get(opt.REDIS_PASSWORD),
            redis_ssl=client.config.getboolean(opt.REDIS_SSL),
            bigtable_project=client.config.get(opt.BIGTABLE_PROJECT),
            bigtable_instance=client.config.get(opt.BIGTABLE_INSTANCE),
            cassandra_host=client.config.get(opt.CASSANDRA_HOST),
            cassandra_port=bool(client.config.get(opt.CASSANDRA_HOST))
            and client.config.getint(opt.CASSANDRA_PORT),
            statsd_host=(
                client.config.getboolean(opt.STATSD_ENABLED)
                and client.config.get(opt.STATSD_HOST)
            ),
            statsd_port=(
                client.config.getboolean(opt.STATSD_ENABLED)
                and client.config.getint(opt.STATSD_PORT)
            ),
            deadletter_path=client.config.get(opt.DEADLETTER_PATH),
            stencil_url=client.config.get(opt.STENCIL_URL),
        )
    )


def unschedule_offline_to_online_ingestion(
    client: "Client", project: str, feature_table: FeatureTable,
):

    launcher = resolve_launcher(client.config)
    launcher.unschedule_offline_to_online_ingestion(project, feature_table.name)


def get_stream_to_online_ingestion_params(
    client: "Client", project: str, feature_table: FeatureTable, extra_jars: List[str]
) -> StreamIngestionJobParameters:
    return StreamIngestionJobParameters(
        jar=client.config.get(opt.SPARK_INGESTION_JAR),
        extra_jars=extra_jars,
        source=_source_to_argument(feature_table.stream_source, client.config),
        feature_table=_feature_table_to_argument(client, project, feature_table),
        redis_host=client.config.get(opt.REDIS_HOST),
        redis_port=bool(client.config.get(opt.REDIS_HOST))
        and client.config.getint(opt.REDIS_PORT),
        redis_password=client.config.get(opt.REDIS_PASSWORD),
        redis_ssl=client.config.getboolean(opt.REDIS_SSL),
        bigtable_project=client.config.get(opt.BIGTABLE_PROJECT),
        bigtable_instance=client.config.get(opt.BIGTABLE_INSTANCE),
        statsd_host=client.config.getboolean(opt.STATSD_ENABLED)
        and client.config.get(opt.STATSD_HOST),
        statsd_port=client.config.getboolean(opt.STATSD_ENABLED)
        and client.config.getint(opt.STATSD_PORT),
        deadletter_path=client.config.get(opt.DEADLETTER_PATH),
        checkpoint_path=client.config.get(opt.CHECKPOINT_PATH),
        stencil_url=client.config.get(opt.STENCIL_URL),
        drop_invalid_rows=client.config.get(opt.INGESTION_DROP_INVALID_ROWS),
        triggering_interval=client.config.getint(
            opt.SPARK_STREAMING_TRIGGERING_INTERVAL, default=None
        ),
    )


def start_stream_to_online_ingestion(
    client: "Client", project: str, feature_table: FeatureTable, extra_jars: List[str]
) -> StreamIngestionJob:

    launcher = resolve_launcher(client.config)

    return launcher.start_stream_to_online_ingestion(
        get_stream_to_online_ingestion_params(
            client, project, feature_table, extra_jars
        )
    )


def list_jobs(
    include_terminated: bool,
    client: "Client",
    project: Optional[str] = None,
    table_name: Optional[str] = None,
) -> List[SparkJob]:
    launcher = resolve_launcher(client.config)
    return launcher.list_jobs(
        include_terminated=include_terminated, table_name=table_name, project=project
    )


def get_job_by_id(job_id: str, client: "Client") -> SparkJob:
    launcher = resolve_launcher(client.config)
    return launcher.get_job_by_id(job_id)


def get_health_metrics(
    client: "Client", project: str, table_names: List[str],
) -> Dict[str, List[str]]:
    all_redis_keys = [f"{project}:{table}" for table in table_names]
    metrics = client.metrics_redis.mget(all_redis_keys)

    passed_feature_tables = []
    failed_feature_tables = []

    for metric, name in zip(metrics, table_names):
        feature_table = client.feature_store.get_feature_table(
            project=project, name=name
        )
        max_age = feature_table.max_age
        # Only perform ingestion health checks for Feature tables with max_age
        if not max_age:
            passed_feature_tables.append(name)
            continue

        # If there are missing metrics in Redis; None is returned if there is no such key
        if not metric:
            passed_feature_tables.append(name)
            continue

        # Ensure ingestion times are in epoch timings
        last_ingestion_time = json.loads(metric)["last_processed_event_timestamp"][
            "value"
        ]
        valid_ingestion_time = datetime.timestamp(
            datetime.now() - timedelta(seconds=max_age.ToSeconds())
        )

        # Check if latest ingestion timestamp > cur_time - max_age
        if valid_ingestion_time > last_ingestion_time:
            failed_feature_tables.append(name)
        else:
            passed_feature_tables.append(name)

    return {"passed": passed_feature_tables, "failed": failed_feature_tables}


def stage_dataframe(df, event_timestamp_column: str, config: Config) -> FileSource:
    """
    Helper function to upload a pandas dataframe in parquet format to a temporary location (under
    SPARK_STAGING_LOCATION) and return it wrapped in a FileSource.

    Args:
        event_timestamp_column(str): the name of the timestamp column in the dataframe.
        config(Config): feast config.
    """
    staging_location = config.get(opt.SPARK_STAGING_LOCATION)
    staging_uri = urlparse(staging_location)

    with tempfile.NamedTemporaryFile() as f:
        df.to_parquet(f)

        file_url = urlunparse(
            get_staging_client(staging_uri.scheme, config).upload_fileobj(
                f,
                f.name,
                remote_path_prefix=os.path.join(staging_location, "dataframes"),
                remote_path_suffix=".parquet",
            )
        )

    return FileSource(
        event_timestamp_column=event_timestamp_column,
        file_format=ParquetFormat(),
        file_url=file_url,
    )
