# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %% [markdown]
# # Ride Hailing Example
# %% [markdown]
# ![chart](https://feaststore.blob.core.windows.net/feastjar/FeastArchitectureNew.png)

# %%
import io
import json
import os
from datetime import datetime
from urllib.parse import urlparse

import avro.schema
import feast_spark
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytz
from avro.io import BinaryEncoder, DatumWriter
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from confluent_kafka import Producer
from feast import Client, Entity, Feature, FeatureTable, ValueType
from feast.data_format import AvroFormat, ParquetFormat
from feast.data_source import FileSource, KafkaSource
from pyarrow.parquet import ParquetDataset
from google.protobuf.duration_pb2 import Duration
# %% [markdown]
# ## Introduction
# %% [markdown]
# For this demo, we will:
# 
# 1. Register two driver features, one for driver statistics, the other for driver trips. Driver statistics are updated on daily basis, whereas driver trips are updated in real time.
# 2. Creates a driver dataset, then use Feast SDK to retrieve the features corresponding to these drivers from an offline store.
# 3. Store the features in an online store (Redis), and retrieve the features via Feast SDK.
# %% [markdown]
# ## Features Registry (Feast Core)
# %% [markdown]
# ### Configuration
# %% [markdown]
# Configurations can be provided in three different methods:

# %%
# get_historical_features will return immediately once the Spark job has been submitted succesfully.
os.environ["FEAST_SPARK_LAUNCHER"] = "synapse"
os.environ["FEAST_SPARK_HOME"] = "/usr/local/spark"
os.environ["FEAST_azure_synapse_dev_url"] = "https://xiaoyzhuspark3synapse.dev.azuresynapse.net"
os.environ["FEAST_azure_synapse_pool_name"] = "xiaoyzhuspark3"

# the datalake dir is the same with this one os.environ["FEAST_SPARK_STAGING_LOCATION"] = "wasbs://feasttest@feaststore.blob.core.windows.net/artifacts/"
os.environ["FEAST_AZURE_SYNAPSE_DATALAKE_DIR"] = "abfss://feastsparkstagingprivate@xiaoyzhusynapse.dfs.core.windows.net/feast"
os.environ["FEAST_HISTORICAL_FEATURE_OUTPUT_LOCATION"] = "abfss://feastsparkstagingprivate@xiaoyzhusynapse.dfs.core.windows.net/feast/out"
os.environ["FEAST_SPARK_STAGING_LOCATION"] = "wasbs://feasttest@feaststore.blob.core.windows.net/artifacts/"
os.environ["FEAST_SPARK_INGESTION_JAR"] = "https://feaststore.blob.core.windows.net/feastjar/feast-ingestion-spark-latest.jar"

# Redis Config
os.environ["FEAST_REDIS_HOST"] = "feastredistest.redis.cache.windows.net"
os.environ["FEAST_REDIS_PORT"] = "6380"
os.environ["FEAST_REDIS_SSL"] = "true"
os.environ["FEAST_REDIS_AUTH"] = "ruWBZ6WZsjUk5lEnDirM9JGoV1UgtMFbAO5lWoRY1QQ="

# EventHub config
os.environ["FEAST_AZURE_EVENTHUB_KAFKA_CONNECTION_STRING"] = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://xiaoyzhufeasttest.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=z9obEAyVvD36fZIEvvtNlCRBEDjIrsfNfDAbgDyTbDg=;EntityPath=driver_trips\";"

os.environ["FEAST_AZURE_BLOB_ACCOUNT_NAME"] = "feaststore"
os.environ["FEAST_AZURE_BLOB_ACCOUNT_ACCESS_KEY"] = "0V7PybxIprcykx3UEfygMTgRIn7pBH794KymizfArYArlB9OsQoVua32iJc5SkSpJZDPoFzDw4lAC2jGIuvAfg=="
os.environ["FEAST_HISTORICAL_FEATURE_OUTPUT_FORMAT"] = "parquet"

# Xiaoyong's app, name: ml-auth-xiaoyzhu
os.environ['AZURE_CLIENT_ID'] = 'b92d6810-7e28-4380-89d8-103ad00e9acd'
os.environ['AZURE_TENANT_ID'] = '72f988bf-86f1-41af-91ab-2d7cd011db47'
os.environ['AZURE_CLIENT_SECRET'] = '_l5mz-6vj2c97Nb0YeNfs0Axa-Yhd0Q~u_'


# %%
# Using environmental variables
import os
os.environ["FEAST_CORE_URL"] = "20.62.162.242:6565"
os.environ["FEAST_SERVING_URL"] = "20.62.162.242:6566"

# Provide a map during client initialization
# options = {
#     "CORE_URL": "core:6565",
#     "SERVING_URL": "online_serving:6566", 
# }
# client = Client(options)

# As keyword arguments, without the `FEAST` prefix
# client = Client(core_url="core:6565", serving_url="online_serving:6566")

# %% [markdown]
# If you are following the quick start guide, all required configurations to follow the remainder of the tutorial should have been setup, in the form of environmental variables, as showned below. The configuration values may differ depending on the environment. For a full list of configurable values and explanation, please refer to the user guide.

# %%
import os
from pprint import pprint
pprint({key: value for key, value in os.environ.items() if key.startswith("FEAST_")})

# %% [markdown]
# ### Basic Imports and Feast Client initialization

# %%
import os

from feast import Client, Feature, Entity, ValueType, FeatureTable
from feast.data_source import FileSource, KafkaSource
from feast.data_format import ParquetFormat, AvroFormat


# %%
client = Client(core_url='13.66.211.41:6565')

# %% [markdown]
# ### Declare Features and Entities
# %% [markdown]
# Entity defines the primary key(s) associated with one or more feature tables. The entity must be registered before declaring the associated feature tables. 

# %%
driver_id = Entity(name="driver_id", description="Driver identifier", value_type=ValueType.INT64)


# %%
# Daily updated features 
acc_rate = Feature("acc_rate", ValueType.FLOAT)
conv_rate = Feature("conv_rate", ValueType.FLOAT)
avg_daily_trips = Feature("avg_daily_trips", ValueType.INT32)

# Real-time updated features
trips_today = Feature("trips_today", ValueType.INT32)

# %% [markdown]
# ```python
# FeatureTable(
#     name = "driver_statistics",
#     entities = ["driver_id"],
#     features = [
#         acc_rate,
#         conv_rate,
#         avg_daily_trips
#     ]
#     
# )
# ```
# 
# 
# ```python
# FeatureTable(
#     name = "driver_trips",
#     entities = ["driver_id"],
#     features = [
#         trips_today
#     ]
#     
# )
# 
# ```
# %% [markdown]
# ![Features Join](https://raw.githubusercontent.com/feast-dev/feast-spark-examples/main/minimal/images/features-join.png)
# %% [markdown]
# ```python
# FeatureTable(
#     ...,
#     batch_source=FileSource(  # Required
#         file_format=ParquetFormat(),
#         file_url="abfss://feast-demo-data-lake",
#         ...
#     ),
#     stream_source=KafkaSource(  # Optional
#         bootstrap_servers="...",
#         topic="driver_trips",
#         ...
#     )
# ```
# %% [markdown]
# Feature tables group the features together and describe how they can be retrieved. The following examples assume that the feature tables are stored on the local file system, and is accessible from the Spark cluster. If you have setup a GCP service account, you may use GCS instead as the file source.
# %% [markdown]
# `batch_source` defines where the historical features are stored. It is also possible to have an optional `stream_source`, which the feature values are delivered continuously.
# 
# For now we will define only `batch_source` for both `driver_statistics` and `driver_trips`, and demonstrate the usage of `stream_source` in later part of the tutorial.

# %%
# This is the location we're using for the offline feature store.

import os
demo_data_location = "wasbs://feasttest@feaststore.blob.core.windows.net/"


# %%
driver_statistics_source_uri = os.path.join(demo_data_location, "driver_statistics")

driver_statistics = FeatureTable(
    name = "driver_statistics",
    entities = ["driver_id"],
    features = [
        acc_rate,
        conv_rate,
        avg_daily_trips
    ],
    max_age=Duration(seconds=86400 * 1),
    batch_source=FileSource(
        event_timestamp_column="datetime",
        created_timestamp_column="created",
        file_format=ParquetFormat(),
        file_url=driver_statistics_source_uri,
        date_partition_column="date"
    )
)


# %%
driver_trips_source_uri = os.path.join(demo_data_location, "driver_trips")


driver_trips = FeatureTable(
    name = "driver_trips",
    entities = ["driver_id"],
    features = [
        trips_today
    ],
    max_age=Duration(seconds=86400 * 1),
    batch_source=FileSource(
        event_timestamp_column="datetime",
        created_timestamp_column="created",
        file_format=ParquetFormat(),
        file_url=driver_trips_source_uri,
        date_partition_column="date"
    )
)

# %% [markdown]
# ### Registering entities and feature tables in Feast Core

# %%
client.apply(driver_id)
client.apply(driver_statistics)
client.apply(driver_trips)


# %%
print(client.get_feature_table("driver_statistics").to_yaml())
print(client.get_feature_table("driver_trips").to_yaml())

# %% [markdown]
# ### Populating batch source
# %% [markdown]
# Feast is agnostic to how the batch source is populated, as long as it complies to the Feature Table specification. Therefore, any existing ETL tools can be used for the purpose of data ingestion. Alternatively, you can also use Feast SDK to ingest a Panda Dataframe to the batch source.

# %%
import pandas as pd
import numpy as np
from datetime import datetime


# %%
def generate_entities():
    return np.random.choice(999999, size=100, replace=False)


# %%
def generate_trips(entities):
    df = pd.DataFrame(columns=["driver_id", "trips_today", "datetime", "created"])
    df['driver_id'] = entities
    df['trips_today'] = np.random.randint(0, 1000, size=100).astype(np.int32)
    df['datetime'] = pd.to_datetime(
            np.random.randint(
                datetime(2020, 10, 10).timestamp(),
                datetime(2020, 10, 20).timestamp(),
                size=100),
        unit="s"
    )
    df['created'] = pd.to_datetime(datetime.now())
    return df
    


# %%
def generate_stats(entities):
    df = pd.DataFrame(columns=["driver_id", "conv_rate", "acc_rate", "avg_daily_trips", "datetime", "created"])
    df['driver_id'] = entities
    df['conv_rate'] = np.random.random(size=100).astype(np.float32)
    df['acc_rate'] = np.random.random(size=100).astype(np.float32)
    df['avg_daily_trips'] = np.random.randint(0, 1000, size=100).astype(np.int32)
    df['datetime'] = pd.to_datetime(
            np.random.randint(
                datetime(2020, 10, 10).timestamp(),
                datetime(2020, 10, 20).timestamp(),
                size=100),
        unit="s"
    )
    df['created'] = pd.to_datetime(datetime.now())
    return df


# %%
entities = generate_entities()
stats_df = generate_stats(entities)
trips_df = generate_trips(entities)


# %%
client.ingest(driver_statistics, stats_df)
client.ingest(driver_trips, trips_df)

# %% [markdown]
# ## Historical Retrieval For Training
# %% [markdown]
# ### Point-in-time correction
# %% [markdown]
# ![Point In Time](https://raw.githubusercontent.com/feast-dev/feast-spark-examples/main/minimal/images/pit-1.png)
# %% [markdown]
# Feast joins the features to the entities based on the following conditions:
# 
# 1. Entity primary key(s) value matches.
# 2. Feature event timestamp is the closest match possible to the entity event timestamp,
#    but must not be more recent than the entity event timestamp, and the difference must
#    not be greater than the maximum age specified in the feature table, unless the maximum age is not specified.
# 3. If more than one feature table rows satisfy condition 1 and 2, feature row with the
#    most recent created timestamp will be chosen.
# 4. If none of the above conditions are satisfied, the feature rows will have null values.

# %%
from pyarrow.parquet import ParquetDataset
from urllib.parse import urlparse


# %%
def read_parquet(uri):
    parsed_uri = urlparse(uri)
    if parsed_uri.scheme == "file":
        return pd.read_parquet(parsed_uri.path)
    elif parsed_uri.scheme == 'wasbs':
        import adlfs
        fs = adlfs.AzureBlobFileSystem(
            account_name=os.getenv('FEAST_AZURE_BLOB_ACCOUNT_NAME'), account_key=os.getenv('FEAST_AZURE_BLOB_ACCOUNT_ACCESS_KEY')
        )
        uripath = parsed_uri.username + parsed_uri.path
        files = fs.glob(uripath + '/part-*')
        ds = ParquetDataset(files, filesystem=fs)
        return ds.read().to_pandas()
    elif parsed_uri.scheme == 'abfss':
        credential = ClientSecretCredential(os.getenv('AZURE_TENANT_ID'), os.getenv('AZURE_CLIENT_ID'), os.getenv('AZURE_CLIENT_SECRET'))
        # credential = DefaultAzureCredential()
        datalake = parsed_uri.netloc.split('@')
        service_client = DataLakeServiceClient(account_url="https://" + datalake[1], credential=credential)
        file_system_client = service_client.get_file_system_client(datalake[0])
        file_client = file_system_client.get_file_client(parsed_uri.path)
        data = file_client.download_file(0)
        with io.BytesIO() as b:
            data.readinto(b)
            table = pq.read_table(b)
            print(table)
            return table
    else:
        raise ValueError(f"Unsupported URL scheme {uri}")


# %%
entities_with_timestamp = pd.DataFrame(columns=['driver_id', 'event_timestamp'])
entities_with_timestamp['driver_id'] = np.random.choice(entities, 10, replace=False)
entities_with_timestamp['event_timestamp'] = pd.to_datetime(np.random.randint(
    datetime(2020, 10, 18).timestamp(),
    datetime(2020, 10, 20).timestamp(),
    size=10), unit='s')
entities_with_timestamp


# %%
job = feast_spark.Client(client).get_historical_features(
    feature_refs=[
        "driver_statistics:avg_daily_trips",
        "driver_statistics:conv_rate",
        "driver_statistics:acc_rate",
        "driver_trips:trips_today"
    ], 
    entity_source=entities_with_timestamp
)

# %% [markdown]
# ![Spark Job](https://feaststore.blob.core.windows.net/feastjar/SparkJobSubmission.PNG)
# 

# %%
# get_output_file_uri will block until the Spark job is completed.
# output_file_uri = job.get_output_file_uri()

# print("output path:", output_file_uri)
# %%
# read_parquet(output_file_uri)

# %% [markdown]
# The retrieved result can now be used for model training.
# %% [markdown]
# ## Populating Online Storage with Batch Ingestion
# %% [markdown]
# In order to populate the online storage, we can use Feast SDK to start a Spark batch job which will extract the features from the batch source, then load the features to an online store.

# %%
job = feast_spark.Client(client).start_offline_to_online_ingestion(
    driver_statistics,
    datetime(2020, 10, 10),
    datetime(2020, 10, 20)
)


# %%
# It will take some time before the Spark Job is completed
job.get_status()

# %% [markdown]
# Once the job is completed, the SDK can be used to retrieve the result from the online store.

# %%
entities_sample = np.random.choice(entities, 10, replace=False)
entities_sample = [{"driver_id": e} for e in entities_sample]
entities_sample


# %%
features = client.get_online_features(
    feature_refs=["driver_statistics:avg_daily_trips"],
    entity_rows=entities_sample).to_dict()
features


# %%
# pd.DataFrame(features)

# %% [markdown]
# The features can now be used as an input to the trained model.
# %% [markdown]
# ## Bonus: Ingestion from Streaming Source - EventHub
# %% [markdown]
# With a streaming source, we can use Feast SDK to launch a Spark streaming job that continuously update the online store. First, we will update `driver_trips` feature table such that a new streaming source is added.

# %%



# %%
import json
import pytz
import io
import avro.schema
from avro.io import BinaryEncoder, DatumWriter
from confluent_kafka import Producer


# %%
# Change this to any Kafka broker addresses which is accessible by the spark cluster
KAFKA_BROKER = "xiaoyzhufeasttest.servicebus.windows.net:9093"


# %%
avro_schema_json = json.dumps({
    "type": "record",
    "name": "DriverTrips",
    "fields": [
        {"name": "driver_id", "type": "long"},
        {"name": "trips_today", "type": "int"},
        {
            "name": "datetime",
            "type": {"type": "long", "logicalType": "timestamp-micros"},
        },
    ],
})


# %%
kafka_topic = "driver_trips"
driver_trips.stream_source = KafkaSource(
    event_timestamp_column="datetime",
    created_timestamp_column="datetime",
    bootstrap_servers=KAFKA_BROKER,
    topic=kafka_topic,
    message_format=AvroFormat(avro_schema_json)
)
client.apply(driver_trips)

# %% [markdown]
# Start the streaming job and send avro record to EventHub:

# %%
job = feast_spark.Client(client).start_stream_to_online_ingestion(
    driver_trips
)


# %%
def send_avro_record_to_kafka(topic, record):
    value_schema = avro.schema.parse(avro_schema_json)
    writer = DatumWriter(value_schema)
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write(record, encoder)

    conf = {
        'bootstrap.servers': 'xiaoyzhufeasttest.servicebus.windows.net:9093', #replace
        'security.protocol': 'SASL_SSL',
        'ssl.ca.location': '/usr/lib/ssl/certs/ca-certificates.crt',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': '$ConnectionString',
        'sasl.password': 'Endpoint=sb://xiaoyzhufeasttest.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=z9obEAyVvD36fZIEvvtNlCRBEDjIrsfNfDAbgDyTbDg=;',          #replace
        'client.id': 'python-example-producer'
    }

    
    producer = Producer({
        **conf
    })
    producer.produce(topic=topic, value=bytes_writer.getvalue())
    producer.flush()


# %%
# Note: depending on the Kafka configuration you may need to create the Kafka topic first, like below:
#from confluent_kafka.admin import AdminClient, NewTopic
#admin = AdminClient({'bootstrap.servers': KAFKA_BROKER})
#new_topic = NewTopic('driver_trips', num_partitions=1, replication_factor=3)
#admin.create_topics(new_topic)
for record in trips_df.drop(columns=['created']).to_dict('record'):
    # print("record", record)
    record["datetime"] = (
        record["datetime"].to_pydatetime().replace(tzinfo=pytz.utc)
    )

    # send_avro_record_to_kafka(topic="driver_trips", record=record)
    send_avro_record_to_kafka(topic=kafka_topic, record=record)

# %% [markdown]
# ### Retrieving joined features from several feature tables

# %%
entities_sample = np.random.choice(entities, 10, replace=False)
entities_sample = [{"driver_id": e} for e in entities_sample]
entities_sample


# %%
features = client.get_online_features(
    feature_refs=["driver_statistics:avg_daily_trips", "driver_trips:trips_today"],
    entity_rows=entities_sample).to_dict()


# %%
pd.DataFrame(features)


# %%
# This will stop the streaming job
job.cancel()


# %%



