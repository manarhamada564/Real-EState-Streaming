# import logging
# from cassandra.cluster import Cluster
# from cassandra.cqlengine.management import create_keyspace_simple
# from jiter import *
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
#
#
# def create_keyspace(session):
#     session.execute("""
#     CREATE KEYSPACE IF NOT EXISTS property_streams
#     WITH replication = {
#         'class': 'SimpleStrategy',
#         'replication_factor': 1
#     }
#     AND durable_writes = true;
#     """)
#     print("Keyspace created successfully!")
#
# def create_table(session):
#     session.execute("""
#         CREATE TABLE IF NOT EXISTS property_streams.properties (
#             address TEXT,
#             description TEXT,
#             link TEXT,
#             pictures LIST<TEXT>,
#             price TEXT,
#             bedrooms TEXT,
#             bathrooms TEXT,
#             receptions TEXT,
#             tenure TEXT,
#             time_remaining_on_lease TEXT,
#             service_charge TEXT,
#             council_tax_band TEXT,
#             ground_rent TEXT,
#             ground_rent_review TEXT,
#             inserted_at TIMESTAMP,
#             PRIMARY KEY (link)
#         );
#     """)
#     print("Table created successfully!")
#
#
# def cassandra_session():
#     session = Cluster(["localhost"]).connect()
#     if session is not None:
#         create_keyspace(session)
#         create_table(session)
#     return session
#
# def insert_data(session, **kwargs):
#     """Insert property data into Cassandra table"""
#     print("Inserting data...")
#     session.execute(
#             """
#             INSERT INTO property_streams.properties (
#                 address, description, link, pictures, price,
#                 bedrooms, bathrooms, receptions, tenure,
#                 time_remaining_on_lease, service_charge,
#                 council_tax_band, ground_rent, ground_rent_review,
#                 inserted_at
#             ) VALUES (
#                 %s, %s, %s, %s, %s,
#                 %s, %s, %s, %s,
#                 %s, %s,
#                 %s, %s, %s,
#                 toTimestamp(now())
#             );
#             """, kwargs.values()
#         )
#     print("Data inserted successfully!")
#
# def process_batch(batch_df, batch_id):
#     session = cassandra_session()
#     try:
#         batch_df.foreach(lambda row: insert_data(session, row.asDict()))
#     finally:
#         session.shutdown()
#
# def main():
#     """
#     conect with kafka to fetch informaion  and write in cassandra
#     """
#     logging.basicConfig(level=logging.INFO)
#
#     spark = (SparkSession.builder
#         .appName("RealEstateProcessor")
#         .config("spark.jars.packages",
#                 "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
#                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
#         .config("spark.cassandra.connection.host", "cassandra_db")
#         .getOrCreate()
#              )
#
#     kafka_df = (spark.readStream.format("kafka")
#                 .option("kafka.bootstrap.servers", "localhost:9092")
#                 .option("subscribe", "properties")
#                 .option("startingOffsets", "earliest")
#                 .load())
#
#     schema = StructType([
#         StructField("address", StringType(), True),
#         StructField("description", StringType(), True),
#         StructField("link", StringType(), True),
#         StructField("pictures", ArrayType(StringType()), True),
#         StructField("price", StringType(), True),
#         StructField("bedrooms", StringType(), True),
#         StructField("bathrooms", StringType(), True),
#         StructField("receptions", StringType(), True),
#         StructField("tenure", StringType(), True),
#         StructField("time_remaining_on_lease", StringType(), True),
#         StructField("service_charge", StringType(), True),
#         StructField("Council tax band", StringType(), True),
#         StructField("Ground rent", StringType(), True),
#         StructField("Ground rent review", StringType(), True),
#     ])
#
#     kafka_df = (kafka_df.selectExpr("CAST(value AS STRING) as value")
#     .select(from_json(col("value"), schema).alias("data"))
#     .select("data.*")
#     )
#
#     cassandra_query = (kafka_df.writeStream
#              .foreachBatch(process_batch)
#              .start()
#              .awaitTermination())
#
#
# if __name__ == "__main__":
#     main()

import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

CASSANDRA_HOST = "localhost"


def setup_cassandra():
    cluster = Cluster(['cassandra'], port=9042)
    session = cluster.connect()
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS property_streams
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    AND durable_writes = true;
    """)
    session.execute("""
    CREATE TABLE IF NOT EXISTS property_streams.properties (
        address TEXT,
        description TEXT,
        link TEXT PRIMARY KEY,
        pictures LIST<TEXT>,
        price TEXT,
        bedrooms TEXT,
        bathrooms TEXT,
        receptions TEXT,
        tenure TEXT,
        time_remaining_on_lease TEXT,
        service_charge TEXT,
        council_tax_band TEXT,
        ground_rent TEXT,
        ground_rent_review TEXT,
        inserted_at TIMESTAMP
    );
    """)
    cluster.shutdown()


# Per-partition processing
def process_partition(rows):
    cluster = Cluster(['cassandra'], port=9042)
    session = cluster.connect('property_streams')

    for row in rows:
        row_dict = row.asDict()
        # Ensure link exists and is not empty
        if row_dict.get('link'):
            insert_data(session, **row_dict)
        else:
            print(f"Skipping record with missing link: {row_dict}")

    cluster.shutdown()

def insert_data(session, **kwargs):
    """Insert property data into Cassandra table"""
    if not kwargs.get('link'):
        print(f"Skipping record with missing link: {kwargs}")
        return

    query = """
    INSERT INTO  (
        address, description, link, pictures, price,
        bedrooms, bathrooms, receptions, tenure,
        time_remaining_on_lease, service_charge,
        council_tax_band, ground_rent, ground_rent_review,
        inserted_at
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, toTimestamp(now()))
    """

    params = (
        kwargs.get('address'),
        kwargs.get('description'),
        kwargs.get('link'),
        kwargs.get('pictures', []),  # Default empty list if missing
        kwargs.get('price'),
        kwargs.get('bedrooms'),
        kwargs.get('bathrooms'),
        kwargs.get('receptions'),
        kwargs.get('tenure'),
        kwargs.get('time_remaining_on_lease', ''),  # Default empty string
        kwargs.get('service_charge', ''),
        kwargs.get('council_tax_band', ''),
        kwargs.get('ground_rent', ''),
        kwargs.get('ground_rent_review', '')
    )

    try:
        session.execute(query, params)
    except Exception as e:
        print(f"Failed to insert record: {e}\nRecord: {kwargs}")

#Batch processing using partitions
def process_batch(batch_df, batch_id):
    batch_df.foreachPartition(process_partition)


def main():
    setup_cassandra()

    spark = (SparkSession.builder
             .appName("RealEstateProcessor")
             .config("spark.jars.packages",
                     "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1")
             .config("spark.cassandra.connection.host", "cassandra")
             .getOrCreate()
             )

    # Schema with corrected column names
    schema = StructType([
        StructField("address", StringType()),
        StructField("description", StringType()),
        StructField("link", StringType()),
        StructField("pictures", ArrayType(StringType())),
        StructField("price", StringType()),
        StructField("bedrooms", StringType()),
        StructField("bathrooms", StringType()),
        StructField("receptions", StringType()),
        StructField("tenure", StringType()),
        StructField("time_remaining_on_lease", StringType()),
        StructField("service_charge", StringType()),
        StructField("Council tax band", StringType()),
        StructField("Ground rent", StringType()),
        StructField("Ground rent review", StringType())
    ])

    kafka_df = (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", "kafka-broker:29092")
                .option("subscribe", "properties")
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"), schema).alias("data"))
                .select("data.*")
                # Rename columns to match Cassandra table
                .withColumnRenamed("Council tax band", "council_tax_band")
                .withColumnRenamed("Ground rent", "ground_rent")
                .withColumnRenamed("Ground rent review", "ground_rent_review")
                .withColumn("price", regexp_replace(col("price"), "[^0-9]", ""))
                .withColumn("tenure", when(col("tenure").contains("Freehold"), "Freehold").otherwise(col("tenure")))
                )

    query = (kafka_df.writeStream
             .foreachBatch(process_batch)
             .start()
             .awaitTermination())

if __name__ == "__main__":
    main()