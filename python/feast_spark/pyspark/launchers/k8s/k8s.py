import hashlib
import random
import string
import time
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, cast
from urllib.parse import urlparse, urlunparse

import yaml
from kubernetes.client.api import CustomObjectsApi

from feast.staging.storage_client import AbstractStagingClient
from feast_spark.pyspark.abc import (
    BatchIngestionJob,
    BatchIngestionJobParameters,
    JobLauncher,
    RetrievalJob,
    RetrievalJobParameters,
    ScheduledBatchIngestionJobParameters,
    SparkJob,
    SparkJobFailure,
    SparkJobStatus,
    StreamIngestionJob,
    StreamIngestionJobParameters,
)

from .k8s_utils import (
    DEFAULT_JOB_TEMPLATE,
    DEFAULT_SCHEDULED_JOB_TEMPLATE,
    HISTORICAL_RETRIEVAL_JOB_TYPE,
    LABEL_FEATURE_TABLE,
    LABEL_FEATURE_TABLE_HASH,
    LABEL_PROJECT,
    METADATA_JOBHASH,
    METADATA_OUTPUT_URI,
    OFFLINE_TO_ONLINE_JOB_TYPE,
    STREAM_TO_ONLINE_JOB_TYPE,
    JobInfo,
    _cancel_job_by_id,
    _generate_project_table_hash,
    _get_api,
    _get_job_by_id,
    _list_jobs,
    _prepare_job_resource,
    _prepare_scheduled_job_resource,
    _submit_job,
    _submit_scheduled_job,
    _unschedule_job,
)


def _load_resource_template(job_template_path: Optional[str]) -> Dict[str, Any]:
    if not job_template_path or not Path(job_template_path).exists():
        return {}

    with open(job_template_path, "rt") as f:
        return yaml.safe_load(f)


def _generate_job_id() -> str:
    return "feast-" + "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(8)
    )


def _generate_scheduled_job_id(project: str, feature_table_name: str) -> str:
    job_hash = hashlib.md5(f"{project}-{feature_table_name}".encode()).hexdigest()
    return f"feast-{job_hash}"


def _truncate_label(label: str) -> str:
    return label[:63]


class JobNotFoundException(Exception):
    pass


class KubernetesJobMixin:
    def __init__(self, api: CustomObjectsApi, namespace: str, job_id: str):
        self._api = api
        self._job_id = job_id
        self._namespace = namespace

    def get_id(self) -> str:
        return self._job_id

    def get_error_message(self) -> str:
        job = _get_job_by_id(self._api, self._namespace, self._job_id)
        if job is None:
            raise JobNotFoundException()
        return job.job_error_message

    def get_status(self) -> SparkJobStatus:
        job = _get_job_by_id(self._api, self._namespace, self._job_id)
        if job is None:
            raise JobNotFoundException
        return job.state

    def get_start_time(self) -> datetime:
        job = _get_job_by_id(self._api, self._namespace, self._job_id)
        if job is None:
            raise JobNotFoundException
        return job.start_time

    def cancel(self):
        _cancel_job_by_id(self._api, self._namespace, self._job_id)

    def _wait_for_complete(self, timeout_seconds: Optional[float]) -> bool:
        """ Returns true if the job completed successfully """
        start_time = time.time()
        while (timeout_seconds is None) or (time.time() - start_time < timeout_seconds):
            status = self.get_status()
            if status == SparkJobStatus.COMPLETED:
                return True
            elif status == SparkJobStatus.FAILED:
                return False
            else:
                time.sleep(1)
        else:
            raise TimeoutError("Timeout waiting for job to complete")


class KubernetesRetrievalJob(KubernetesJobMixin, RetrievalJob):
    """
    Historical feature retrieval job result for a k8s cluster
    """

    def __init__(
        self, api: CustomObjectsApi, namespace: str, job_id: str, output_file_uri: str
    ):
        """
        This is the job object representing the historical retrieval job, returned by KubernetesClusterLauncher.

        Args:
            output_file_uri (str): Uri to the historical feature retrieval job output file.
        """
        super().__init__(api, namespace, job_id)
        self._output_file_uri = output_file_uri

    def get_output_file_uri(self, timeout_sec=None, block=True):
        if not block:
            return self._output_file_uri

        if self._wait_for_complete(timeout_sec):
            return self._output_file_uri
        else:
            raise SparkJobFailure("Spark job failed")


class KubernetesBatchIngestionJob(KubernetesJobMixin, BatchIngestionJob):
    """
    Ingestion job result for a k8s cluster
    """

    def __init__(
        self, api: CustomObjectsApi, namespace: str, job_id: str, feature_table: str
    ):
        super().__init__(api, namespace, job_id)
        self._feature_table = feature_table

    def get_feature_table(self) -> str:
        return self._feature_table


class KubernetesStreamIngestionJob(KubernetesJobMixin, StreamIngestionJob):
    """
    Ingestion streaming job for a k8s cluster
    """

    def __init__(
        self,
        api: CustomObjectsApi,
        namespace: str,
        job_id: str,
        job_hash: str,
        feature_table: str,
    ):
        super().__init__(api, namespace, job_id)
        self._job_hash = job_hash
        self._feature_table = feature_table

    def get_hash(self) -> str:
        return self._job_hash

    def get_feature_table(self) -> str:
        return self._feature_table


class KubernetesJobLauncher(JobLauncher):
    """
    Submits spark jobs to a spark cluster. Currently supports only historical feature retrieval jobs.
    """

    def __init__(
        self,
        namespace: str,
        incluster: bool,
        staging_location: str,
        generic_resource_template_path: Optional[str],
        batch_ingestion_resource_template_path: Optional[str],
        stream_ingestion_resource_template_path: Optional[str],
        historical_retrieval_resource_template_path: Optional[str],
        staging_client: AbstractStagingClient,
    ):
        self._namespace = namespace
        self._api = _get_api(incluster=incluster)
        self._staging_location = staging_location
        self._staging_client = staging_client

        generic_template = _load_resource_template(
            generic_resource_template_path
        ) or yaml.safe_load(DEFAULT_JOB_TEMPLATE)

        self._batch_ingestion_template = (
            _load_resource_template(batch_ingestion_resource_template_path)
            or generic_template
        )

        self._stream_ingestion_template = (
            _load_resource_template(stream_ingestion_resource_template_path)
            or generic_template
        )

        self._historical_retrieval_template = (
            _load_resource_template(historical_retrieval_resource_template_path)
            or generic_template
        )

        self._scheduled_resource_template = yaml.safe_load(
            DEFAULT_SCHEDULED_JOB_TEMPLATE
        )

    def _job_from_job_info(self, job_info: JobInfo) -> SparkJob:
        if job_info.job_type == HISTORICAL_RETRIEVAL_JOB_TYPE:
            assert METADATA_OUTPUT_URI in job_info.extra_metadata
            return KubernetesRetrievalJob(
                api=self._api,
                namespace=job_info.namespace,
                job_id=job_info.job_id,
                output_file_uri=job_info.extra_metadata[METADATA_OUTPUT_URI],
            )
        elif job_info.job_type == OFFLINE_TO_ONLINE_JOB_TYPE:
            return KubernetesBatchIngestionJob(
                api=self._api,
                namespace=job_info.namespace,
                job_id=job_info.job_id,
                feature_table=job_info.labels.get(LABEL_FEATURE_TABLE, ""),
            )
        elif job_info.job_type == STREAM_TO_ONLINE_JOB_TYPE:
            # job_hash must not be None for stream ingestion jobs
            assert METADATA_JOBHASH in job_info.extra_metadata
            return KubernetesStreamIngestionJob(
                api=self._api,
                namespace=job_info.namespace,
                job_id=job_info.job_id,
                job_hash=job_info.extra_metadata[METADATA_JOBHASH],
                feature_table=job_info.labels.get(LABEL_FEATURE_TABLE, ""),
            )
        else:
            # We should never get here
            raise ValueError(f"Unknown job type {job_info.job_type}")

    def historical_feature_retrieval(
        self, job_params: RetrievalJobParameters
    ) -> RetrievalJob:
        """
        Submits a historical feature retrieval job to a Spark cluster.

        Raises:
            SparkJobFailure: The spark job submission failed, encountered error
                during execution, or timeout.

        Returns:
            RetrievalJob: wrapper around remote job that returns file uri to the result file.
        """

        with open(job_params.get_main_file_path()) as f:
            pyspark_script = f.read()

        pyspark_script_path = urlunparse(
            self._staging_client.upload_fileobj(
                BytesIO(pyspark_script.encode("utf8")),
                local_path="historical_retrieval.py",
                remote_path_prefix=self._staging_location,
                remote_path_suffix=".py",
            )
        )

        job_id = _generate_job_id()

        resource = _prepare_job_resource(
            job_template=self._historical_retrieval_template,
            job_id=job_id,
            job_type=HISTORICAL_RETRIEVAL_JOB_TYPE,
            main_application_file=pyspark_script_path,
            main_class=None,
            packages=[],
            jars=[],
            extra_metadata={METADATA_OUTPUT_URI: job_params.get_destination_path()},
            arguments=job_params.get_arguments(),
            namespace=self._namespace,
            extra_labels={LABEL_PROJECT: job_params.get_project()},
        )

        job_info = _submit_job(
            api=self._api, resource=resource, namespace=self._namespace,
        )

        return cast(RetrievalJob, self._job_from_job_info(job_info))

    def _upload_jar(self, jar_path: str) -> str:
        if (
            jar_path.startswith("s3://")
            or jar_path.startswith("s3a://")
            or jar_path.startswith("https://")
            or jar_path.startswith("local://")
        ):
            return jar_path
        elif jar_path.startswith("file://"):
            local_jar_path = urlparse(jar_path).path
        else:
            local_jar_path = jar_path
        with open(local_jar_path, "rb") as f:
            return urlunparse(
                self._staging_client.upload_fileobj(
                    f,
                    local_jar_path,
                    remote_path_prefix=self._staging_location,
                    remote_path_suffix=".jar",
                )
            )

    def offline_to_online_ingestion(
        self, ingestion_job_params: BatchIngestionJobParameters
    ) -> BatchIngestionJob:
        """
        Submits a batch ingestion job to a Spark cluster.

        Raises:
            SparkJobFailure: The spark job submission failed, encountered error
                during execution, or timeout.

        Returns:
            BatchIngestionJob: wrapper around remote job that can be used to check when job completed.
        """

        jar_s3_path = self._upload_jar(ingestion_job_params.get_main_file_path())

        job_id = _generate_job_id()

        resource = _prepare_job_resource(
            job_template=self._batch_ingestion_template,
            job_id=job_id,
            job_type=OFFLINE_TO_ONLINE_JOB_TYPE,
            main_application_file=jar_s3_path,
            main_class=ingestion_job_params.get_class_name(),
            packages=[],
            jars=[],
            extra_metadata={},
            arguments=ingestion_job_params.get_arguments(),
            namespace=self._namespace,
            extra_labels={
                LABEL_FEATURE_TABLE: _truncate_label(
                    ingestion_job_params.get_feature_table_name()
                ),
                LABEL_FEATURE_TABLE_HASH: _generate_project_table_hash(
                    ingestion_job_params.get_project(),
                    ingestion_job_params.get_feature_table_name(),
                ),
                LABEL_PROJECT: ingestion_job_params.get_project(),
            },
        )

        job_info = _submit_job(
            api=self._api, resource=resource, namespace=self._namespace,
        )

        return cast(BatchIngestionJob, self._job_from_job_info(job_info))

    def schedule_offline_to_online_ingestion(
        self, ingestion_job_params: ScheduledBatchIngestionJobParameters
    ):
        """
        Schedule a batch ingestion job using Spark Operator.

        Raises:
            SparkJobFailure: Failure to create the ScheduleSparkApplication resource, or timeout.

        Returns:
            ScheduledBatchIngestionJob: wrapper around remote job that can be used to check the job id.
        """

        jar_s3_path = self._upload_jar(ingestion_job_params.get_main_file_path())

        schedule_job_id = _generate_scheduled_job_id(
            project=ingestion_job_params.get_project(),
            feature_table_name=ingestion_job_params.get_feature_table_name(),
        )

        resource = _prepare_scheduled_job_resource(
            scheduled_job_template=self._scheduled_resource_template,
            scheduled_job_id=schedule_job_id,
            job_schedule=ingestion_job_params.get_job_schedule(),
            job_template=self._batch_ingestion_template,
            job_type=OFFLINE_TO_ONLINE_JOB_TYPE,
            main_application_file=jar_s3_path,
            main_class=ingestion_job_params.get_class_name(),
            packages=[],
            jars=[],
            extra_metadata={},
            arguments=ingestion_job_params.get_arguments(),
            namespace=self._namespace,
            extra_labels={
                LABEL_FEATURE_TABLE: _truncate_label(
                    ingestion_job_params.get_feature_table_name()
                ),
                LABEL_FEATURE_TABLE_HASH: _generate_project_table_hash(
                    ingestion_job_params.get_project(),
                    ingestion_job_params.get_feature_table_name(),
                ),
                LABEL_PROJECT: ingestion_job_params.get_project(),
            },
        )

        _submit_scheduled_job(
            api=self._api,
            name=schedule_job_id,
            resource=resource,
            namespace=self._namespace,
        )

    def unschedule_offline_to_online_ingestion(self, project: str, feature_table: str):
        _unschedule_job(
            api=self._api,
            namespace=self._namespace,
            resource_name=_generate_scheduled_job_id(project, feature_table),
        )

    def start_stream_to_online_ingestion(
        self, ingestion_job_params: StreamIngestionJobParameters
    ) -> StreamIngestionJob:
        """
        Starts a stream ingestion job to a Spark cluster.

        Raises:
            SparkJobFailure: The spark job submission failed, encountered error
                during execution, or timeout.

        Returns:
            StreamIngestionJob: wrapper around remote job.
        """

        jar_s3_path = self._upload_jar(ingestion_job_params.get_main_file_path())

        extra_jar_paths: List[str] = []
        for extra_jar in ingestion_job_params.get_extra_jar_paths():
            extra_jar_paths.append(self._upload_jar(extra_jar))

        job_hash = ingestion_job_params.get_job_hash()
        job_id = _generate_job_id()

        resource = _prepare_job_resource(
            job_template=self._stream_ingestion_template,
            job_id=job_id,
            job_type=STREAM_TO_ONLINE_JOB_TYPE,
            main_application_file=jar_s3_path,
            main_class=ingestion_job_params.get_class_name(),
            packages=[],
            jars=extra_jar_paths,
            extra_metadata={METADATA_JOBHASH: job_hash},
            arguments=ingestion_job_params.get_arguments(),
            namespace=self._namespace,
            extra_labels={
                LABEL_FEATURE_TABLE: _truncate_label(
                    ingestion_job_params.get_feature_table_name()
                ),
                LABEL_FEATURE_TABLE_HASH: _generate_project_table_hash(
                    ingestion_job_params.get_project(),
                    ingestion_job_params.get_feature_table_name(),
                ),
                LABEL_PROJECT: ingestion_job_params.get_project(),
            },
        )

        job_info = _submit_job(
            api=self._api, resource=resource, namespace=self._namespace,
        )

        return cast(StreamIngestionJob, self._job_from_job_info(job_info))

    def get_job_by_id(self, job_id: str) -> SparkJob:
        job_info = _get_job_by_id(self._api, self._namespace, job_id)
        if job_info is None:
            raise KeyError(f"Job with id {job_id} not found")
        else:
            return self._job_from_job_info(job_info)

    def list_jobs(
        self,
        include_terminated: bool,
        project: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> List[SparkJob]:
        return [
            self._job_from_job_info(job)
            for job in _list_jobs(self._api, self._namespace, project, table_name)
            if include_terminated
            or job.state not in (SparkJobStatus.COMPLETED, SparkJobStatus.FAILED)
        ]
