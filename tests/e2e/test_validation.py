import time
import uuid

import numpy as np
import pandas as pd
import pytest
from great_expectations.dataset import PandasDataset

from feast import Client
from feast.wait import wait_retry_backoff
from feast_spark import Client as SparkClient
from feast_spark.contrib.validation.ge import apply_validation, create_validation_udf
from tests.e2e.fixtures.statsd_stub import StatsDServer
from tests.e2e.utils.common import avro_schema, create_schema, start_job, stop_job
from tests.e2e.utils.kafka import check_consumer_exist, ingest_and_retrieve


def generate_train_data():
    df = pd.DataFrame(columns=["key", "num", "set", "event_timestamp"])
    df["key"] = np.random.choice(999999, size=100, replace=False)
    df["num"] = np.random.randint(0, 100, 100)
    df["set"] = np.random.choice(["a", "b", "c"], size=100)
    df["event_timestamp"] = pd.to_datetime(int(time.time()), unit="s")

    return df


def generate_test_data():
    df = pd.DataFrame(columns=["key", "num", "set", "event_timestamp"])
    df["key"] = np.random.choice(999999, size=100, replace=False)
    df["num"] = np.random.randint(0, 150, 100)
    df["set"] = np.random.choice(["a", "b", "c", "d"], size=100)
    df["event_timestamp"] = pd.to_datetime(int(time.time()), unit="s")

    return df


def test_validation_with_ge(
    feast_client: Client, feast_spark_client: SparkClient, kafka_server, pytestconfig
):
    kafka_broker = f"{kafka_server[0]}:{kafka_server[1]}"
    topic_name = f"avro-{uuid.uuid4()}"

    entity, feature_table = create_schema(kafka_broker, topic_name, "validation_ge")
    feast_client.apply_entity(entity)
    feast_client.apply_feature_table(feature_table)

    train_data = generate_train_data()
    ge_ds = PandasDataset(train_data)
    ge_ds.expect_column_values_to_be_between("num", 0, 100)
    ge_ds.expect_column_values_to_be_in_set("set", ["a", "b", "c"])
    expectations = ge_ds.get_expectation_suite()

    udf = create_validation_udf("testUDF", expectations, feature_table)
    apply_validation(
        feast_client, feature_table, udf, validation_window_secs=1, include_py_libs=""
    )

    job = start_job(feast_spark_client, feature_table, pytestconfig)

    wait_retry_backoff(
        lambda: (None, check_consumer_exist(kafka_broker, topic_name)), 300
    )

    test_data = generate_test_data()
    ge_ds = PandasDataset(test_data)
    validation_result = ge_ds.validate(expectations, result_format="COMPLETE")
    invalid_idx = list(
        {
            idx
            for check in validation_result.results
            for idx in check.result["unexpected_index_list"]
        }
    )

    entity_rows = [{"key": key} for key in test_data["key"].tolist()]

    try:
        ingested = ingest_and_retrieve(
            feast_client,
            test_data,
            avro_schema_json=avro_schema(),
            topic_name=topic_name,
            kafka_broker=kafka_broker,
            entity_rows=entity_rows,
            feature_names=["validation_ge:num", "validation_ge:set"],
            expected_ingested_count=test_data.shape[0] - len(invalid_idx),
        )
    finally:
        stop_job(job, feast_spark_client, feature_table)

    test_data["num"] = test_data["num"].astype(np.float64)
    test_data["num"].iloc[invalid_idx] = np.nan
    test_data["set"].iloc[invalid_idx] = None

    pd.testing.assert_frame_equal(
        ingested[["key", "validation_ge:num", "validation_ge:set"]],
        test_data[["key", "num", "set"]].rename(
            columns={"num": "validation_ge:num", "set": "validation_ge:set"}
        ),
    )


@pytest.mark.env("local")
def test_validation_reports_metrics(
    feast_client: Client,
    feast_spark_client: SparkClient,
    kafka_server,
    statsd_server: StatsDServer,
    pytestconfig,
):
    kafka_broker = f"{kafka_server[0]}:{kafka_server[1]}"
    topic_name = f"avro-{uuid.uuid4()}"

    entity, feature_table = create_schema(
        kafka_broker, topic_name, "validation_ge_metrics"
    )
    feast_client.apply_entity(entity)
    feast_client.apply_feature_table(feature_table)

    train_data = generate_train_data()
    ge_ds = PandasDataset(train_data)
    ge_ds.expect_column_values_to_be_between("num", 0, 100)
    ge_ds.expect_column_values_to_be_in_set("set", ["a", "b", "c"])
    expectations = ge_ds.get_expectation_suite()

    udf = create_validation_udf("testUDF", expectations, feature_table)
    apply_validation(
        feast_client, feature_table, udf, validation_window_secs=10, include_py_libs=""
    )

    job = start_job(feast_spark_client, feature_table, pytestconfig)

    wait_retry_backoff(
        lambda: (None, check_consumer_exist(kafka_broker, topic_name)), 300
    )

    test_data = generate_test_data()
    ge_ds = PandasDataset(test_data)
    validation_result = ge_ds.validate(expectations, result_format="COMPLETE")
    unexpected_counts = {
        "expect_column_values_to_be_between_num_0_100": validation_result.results[
            0
        ].result["unexpected_count"],
        "expect_column_values_to_be_in_set_set": validation_result.results[1].result[
            "unexpected_count"
        ],
    }
    invalid_idx = list(
        {
            idx
            for check in validation_result.results
            for idx in check.result["unexpected_index_list"]
        }
    )

    entity_rows = [{"key": key} for key in test_data["key"].tolist()]

    try:
        ingest_and_retrieve(
            feast_client,
            test_data,
            avro_schema_json=avro_schema(),
            topic_name=topic_name,
            kafka_broker=kafka_broker,
            entity_rows=entity_rows,
            feature_names=["validation_ge_metrics:num", "validation_ge_metrics:set"],
            expected_ingested_count=test_data.shape[0] - len(invalid_idx),
        )
    finally:
        stop_job(job, feast_spark_client, feature_table)

    expected_metrics = [
        (
            f"feast_feature_validation_check_failed#check:{check_name},"
            f"feature_table:{feature_table.name},project:{feast_client.project}",
            value,
        )
        for check_name, value in unexpected_counts.items()
    ]
    wait_retry_backoff(
        lambda: (
            None,
            all(statsd_server.metrics.get(m) == v for m, v in expected_metrics),
        ),
        timeout_secs=30,
        timeout_msg="Expected metrics were not received: "
        + str(expected_metrics)
        + "\n"
        "Actual received metrics" + str(statsd_server.metrics),
    )
