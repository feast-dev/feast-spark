from typing import Optional

from feast.constants import ConfigMeta


class ConfigOptions(metaclass=ConfigMeta):
    #: Default Feast Job Service URL
    JOB_SERVICE_URL: Optional[str] = None

    #: Enable or disable TLS/SSL to Feast Job Service
    JOB_SERVICE_ENABLE_SSL: str = "False"

    #: Path to certificate(s) to secure connection to Feast Job Service
    JOB_SERVICE_SERVER_SSL_CERT: str = ""

    #: Enable or disable control loop for Feast Job Service
    JOB_SERVICE_ENABLE_CONTROL_LOOP: str = "False"

    #: If set to True, Control Loop will try to start failed streaming jobss
    JOB_SERVICE_RETRY_FAILED_JOBS: str = "False"

    #: Pause in seconds between starting new jobs in Control Loop
    JOB_SERVICE_PAUSE_BETWEEN_JOBS: str = "5"

    #: Port for which Prometheus metric server will be running on
    JOB_SERVICE_PROMETHEUS_METRIC_PORT: int = 8080

    #: Endpoint URL for S3 storage_client
    S3_ENDPOINT_URL: Optional[str] = None

    #: Spark Job launcher. The choice of storage is connected to the choice of SPARK_LAUNCHER.
    #:
    #: Options: "standalone", "dataproc", "emr"
    SPARK_LAUNCHER: Optional[str] = None

    #: Feast Spark Job ingestion jobs staging location. The choice of storage is connected to the choice of SPARK_LAUNCHER.
    #:
    #: Eg. gs://some-bucket/output/, s3://some-bucket/output/, file:///data/subfolder/
    SPARK_STAGING_LOCATION: Optional[str] = None

    #: Feast Spark Job ingestion jar file. The choice of storage is connected to the choice of SPARK_LAUNCHER.
    #:
    #: Eg. "dataproc" (http and gs), "emr" (http and s3), "standalone" (http and file)
    SPARK_INGESTION_JAR: str = "https://storage.googleapis.com/feast-jobs/spark/ingestion/feast-ingestion-spark-develop.jar"

    #: Spark resource manager master url
    SPARK_STANDALONE_MASTER: str = "local[*]"

    #: Directory where Spark is installed
    SPARK_HOME: Optional[str] = None

    #: The project id where the materialized view of BigQuerySource is going to be created
    #: by default, use the same project where view is located
    SPARK_BQ_MATERIALIZATION_PROJECT: Optional[str] = None

    #: The dataset id where the materialized view of BigQuerySource is going to be created
    #: by default, use the same dataset where view is located
    SPARK_BQ_MATERIALIZATION_DATASET: Optional[str] = None

    #: Dataproc cluster to run Feast Spark Jobs in
    DATAPROC_CLUSTER_NAME: Optional[str] = None

    #: Project of Dataproc cluster
    DATAPROC_PROJECT: Optional[str] = None

    #: Region of Dataproc cluster
    DATAPROC_REGION: Optional[str] = None

    #: No. of executor instances for Dataproc cluster
    DATAPROC_EXECUTOR_INSTANCES = "2"

    #: No. of executor cores for Dataproc cluster
    DATAPROC_EXECUTOR_CORES = "2"

    #: No. of executor memory for Dataproc cluster
    DATAPROC_EXECUTOR_MEMORY = "2g"

    # namespace to use for Spark jobs launched using k8s spark operator
    SPARK_K8S_NAMESPACE = "default"

    # expect k8s spark operator to be running in the same cluster as Feast
    SPARK_K8S_USE_INCLUSTER_CONFIG = "True"

    # SparkApplication resource template
    SPARK_K8S_JOB_TEMPLATE_PATH = None

    # SparkApplication resource template for Batch Ingestion Jobs
    SPARK_K8S_BATCH_INGESTION_TEMPLATE_PATH: Optional[str] = ""

    # SparkApplication resource template for Stream Ingestion Jobs
    SPARK_K8S_STREAM_INGESTION_TEMPLATE_PATH: Optional[str] = ""

    # SparkApplication resource template for Historical Retrieval Jobs
    SPARK_K8S_HISTORICAL_RETRIEVAL_TEMPLATE_PATH: Optional[str] = ""

    #: Default Redis host to Redis Instance which stores Spark Ingestion Job metrics
    SPARK_METRICS_REDIS_HOST: Optional[str] = None

    #: Default Redis port to Redis Instance which stores Spark Ingestion Job metrics
    SPARK_METRICS_REDIS_PORT: Optional[str] = None

    #: Host to Redis Instance which stores locks for job management
    LOCK_MGR_REDIS_HOST: Optional[str] = None

    #: Port to Redis Instance which stores locks for job management
    LOCK_MGR_REDIS_PORT: Optional[str] = None

    #: TTL for locks for job management
    LOCK_EXPIRY: Optional[str] = "60"

    #: File format of historical retrieval features
    HISTORICAL_FEATURE_OUTPUT_FORMAT: str = "parquet"

    #: File location of historical retrieval features
    HISTORICAL_FEATURE_OUTPUT_LOCATION: Optional[str] = None

    #: Default Redis host
    REDIS_HOST: Optional[str] = ""

    #: Default Redis port
    REDIS_PORT: Optional[str] = ""

    #: Redis credentials
    REDIS_PASSWORD: Optional[str] = ""

    #: Enable or disable TLS/SSL to Redis
    REDIS_SSL: Optional[str] = "False"

    #: BigTable Project ID
    BIGTABLE_PROJECT: Optional[str] = ""

    #: BigTable Instance ID
    BIGTABLE_INSTANCE: Optional[str] = ""

    #: Cassandra host. Can be a comma separated string
    CASSANDRA_HOST: Optional[str] = ""

    #: Cassandra port
    CASSANDRA_PORT: Optional[str] = ""

    #: Enable or disable StatsD
    STATSD_ENABLED: str = "False"

    #: Default StatsD port
    STATSD_HOST: Optional[str] = None

    #: Default StatsD port
    STATSD_PORT: Optional[str] = None

    #: Ingestion Job DeadLetter Destination. The choice of storage is connected to the choice of SPARK_LAUNCHER.
    #:
    #: Eg. gs://some-bucket/output/, s3://some-bucket/output/, file:///data/subfolder/
    DEADLETTER_PATH: str = ""

    #: Ingestion Job Checkpoint Location. Format same as for DeadLetter path
    CHECKPOINT_PATH: str = ""

    #: ProtoRegistry Address (currently only Stencil Server is supported as registry)
    #: https://github.com/gojekfarm/stencil
    STENCIL_URL: str = ""

    #: If set to true rows that do not pass custom validation (see feast.contrib.validation)
    #: won't be saved to Online Storage
    INGESTION_DROP_INVALID_ROWS: str = "False"

    #: EMR cluster to run Feast Spark Jobs in
    EMR_CLUSTER_ID: Optional[str] = None

    #: Region of EMR cluster
    EMR_REGION: Optional[str] = None

    #: Template path of EMR cluster
    EMR_CLUSTER_TEMPLATE_PATH: Optional[str] = None

    #: Log path of EMR cluster
    EMR_LOG_LOCATION: Optional[str] = None

    #: Whitelisted Feast Job Types
    WHITELISTED_JOB_TYPES: Optional[str] = None

    #: Whitelisted Feast projects
    WHITELISTED_PROJECTS: Optional[str] = None

    #: File path to a whitelist containing all the feature tables allowed for ingestion.
    #: Each line in the file should be in the format of <project>:<feature table>
    WHITELISTED_FEATURE_TABLES_PATH: Optional[str] = None

    #: If set - streaming ingestion job will be consuming incoming rows not continuously,
    #: but periodically with configured interval (in seconds).
    #: That may help to control amount of write requests to storage
    SPARK_STREAMING_TRIGGERING_INTERVAL: Optional[str] = None

    #: GCP project of the BigQuery dataset used to stage the entities during historical
    #: feature retrieval. If not set, the GCP project of the feature table batch source
    #: will be used instead.
    BQ_STAGING_PROJECT: Optional[str] = None

    #: BigQuery dataset used to stage the entities during historical feature retrieval.
    #  If not set, the BigQuery dataset of the batch source will be used
    #: instead.
    BQ_STAGING_DATASET: Optional[str] = None

    def defaults(self):
        return {
            k: getattr(self, k)
            for k in self.__config_keys__
            if getattr(self, k) is not None
        }
