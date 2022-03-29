from feast_spark.pyspark.abc import StreamIngestionJobParameters


def test_stream_ingestion_job_hash():
    streaming_source = {
        "kafka": {
            "event_timestamp_column": "event_timestamp",
            "bootstrap_servers": "localhost:9092",
            "topic": "test",
            "format": {
                "class_path": "com.test.someprotos",
                "json_class": "ProtoFormat",
            },
        }
    }
    feature_table = {
        "features": [
            {"name": "feature_1", "type": "STRING"},
            {"name": "feature_2", "type": "STRING"},
        ],
        "entities": [
            {"name": "entity_1", "type": "STRING"},
            {"name": "entity_2", "type": "STRING"},
        ],
        "project": "someproject",
    }
    feature_table_with_different_order = {
        "features": [
            {"name": "feature_2", "type": "STRING"},
            {"name": "feature_1", "type": "STRING"},
        ],
        "entities": [
            {"name": "entity_2", "type": "STRING"},
            {"name": "entity_1", "type": "STRING"},
        ],
        "project": "someproject",
    }
    param = StreamIngestionJobParameters(
        source=streaming_source, feature_table=feature_table, jar=""
    )
    param_different_order = StreamIngestionJobParameters(
        source=streaming_source,
        feature_table=feature_table_with_different_order,
        jar="",
    )
    assert param.get_job_hash() == param_different_order.get_job_hash()
