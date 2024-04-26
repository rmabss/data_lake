from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, to_timestamp
from pyspark.sql.types import StructType, StringType, FloatType, TimestampType, IntegerType

# Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
    .getOrCreate()

# Définition du schéma pour les données JSON
schema = StructType().add("id_transaction", StringType()) \
                     .add("type_transaction", StringType()) \
                     .add("montant", FloatType()) \
                     .add("devise", StringType()) \
                     .add("date", TimestampType()) \
                     .add("lieu", StringType()) \
                     .add("moyen_paiement", StringType()) \
                     .add("details", StructType().add("produit", StringType()) \
                                                .add("quantite", IntegerType()) \
                                                .add("prix_unitaire", FloatType())) \
                     .add("utilisateur", StructType().add("id_utilisateur", StringType()) \
                                                     .add("nom", StringType()) \
                                                     .add("adresse", StringType()) \
                                                     .add("email", StringType()))

# Lecture des données à partir de Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "transaction") \
    .option("startingOffsets", "earliest") \
    .load()

# Conversion des données JSON en DataFrame
df = kafka_df.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("data")).select("data.*")

# Conversion de USD en EUR
df = df.withColumn("montant_eur", when(col("devise") == "USD", col("montant") * 0.84).otherwise(col("montant")))

# Ajout du TimeZone
df = df.withColumn("date_with_timezone", to_timestamp("date", "yyyy-MM-dd HH:mm:ss"))

# Suppression des transactions en erreur (par exemple, montant négatif) et moyen_paiement==erreur
df = df.filter(col("montant") > 0)
df = df.filter(col("moyen_paiement") != "erreur")

# Suppression des valeurs nulles dans l'adresse
df = df.filter(~(col("lieu") == "None"))
df = df.filter(~col("utilisateur.adresse").contains("None"))
df = df.filter((col("utilisateur.adresse").isNotNull()) & (col("utilisateur.adresse") != "None"))

# Écriture du DataFrame en streaming au format parquet
query = df.writeStream.outputMode("append").format("parquet").option("checkpointLocation","metadata").option("path", "elem").start()

# Écriture des données sur la console
query = df.writeStream.outputMode("append").format("console").option("truncate", False).start()


# Attente de la terminaison du streaming
query.awaitTermination()
