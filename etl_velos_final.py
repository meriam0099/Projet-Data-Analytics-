# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, to_date, year, month
from pyspark.sql.types import IntegerType, DoubleType

spark = SparkSession.builder.appName("ETL_Velos_Nettoye").getOrCreate()

print("=== DEBUT ETL VENTES VELOS AVEC NETTOYAGE ===")

# 1. EXTRACTION : lecture des fichiers bruts avec le bon séparateur
orders = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ";") \
    .option("encoding", "UTF-8") \
    .csv("/workspace/data/orders.csv")

bikes = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ";") \
    .option("encoding", "UTF-8") \
    .csv("/workspace/data/bikes.csv")

print("Orders bruts : " + str(orders.count()) + " lignes")
print("Bikes bruts  : " + str(bikes.count()) + " lignes")

# 2. RENOMMER LES COLONNES (enlever les points)
orders = orders.withColumnRenamed("_c0", "id_ligne")
orders = orders.withColumnRenamed("order.id", "order_id")
orders = orders.withColumnRenamed("order.line", "order_line")
orders = orders.withColumnRenamed("order.date", "order_date_str")
orders = orders.withColumnRenamed("customer.id", "customer_id")
orders = orders.withColumnRenamed("product.id", "product_id")
orders = orders.withColumnRenamed("quantity", "quantity")

bikes = bikes.withColumnRenamed("bike.id", "bike_id")
bikes = bikes.withColumnRenamed("model", "model")
bikes = bikes.withColumnRenamed("category1", "category1")
bikes = bikes.withColumnRenamed("category2", "category2")
bikes = bikes.withColumnRenamed("frame", "frame")
bikes = bikes.withColumnRenamed("price", "price")

print("\nColonnes après renommage :")
print("Orders : " + str(orders.columns))
print("Bikes : " + str(bikes.columns))

# 3. NETTOYAGE DES DONNEES

# 3.1 Nettoyage des commandes
orders_clean = (
    orders
    # supprimer la colonne id_ligne si elle n'est pas utile
    .drop("id_ligne")
    # supprimer les lignes sans produit ou sans quantité
    .filter(col("product_id").isNotNull())
    .filter(col("quantity").isNotNull())
    # convertir quantity en integer
    .withColumn("quantity", col("quantity").cast(IntegerType()))
    # garder uniquement quantity > 0
    .filter(col("quantity") > 0)
    # parser la date de commande - format "d/M/yyyy"
    .withColumn("order_date", to_date(col("order_date_str"), "d/M/yyyy"))
    # supprimer les dates nulles
    .filter(col("order_date").isNotNull())
)

print("\nOrders nettoyes : " + str(orders_clean.count()) + " lignes")

# 3.2 Nettoyage des vélos
bikes_clean = (
    bikes
    # supprimer les vélos sans id
    .filter(col("bike_id").isNotNull())
    # prix manquant -> 0
    .withColumn("price", coalesce(col("price"), lit(0)))
    # convertir le prix en double
    .withColumn("price", col("price").cast(DoubleType()))
    # garder uniquement price > 0
    .filter(col("price") > 0)
)

print("Bikes nettoyes  : " + str(bikes_clean.count()) + " lignes")

# 4. TRANSFORMATION : jointure + calcul montant + colonnes dérivées

fact = (
    orders_clean
    .join(
        bikes_clean,
        col("product_id") == col("bike_id"),
        "inner"   # inner pour enlever les lignes sans vélo
    )
    .withColumn(
        "amount",
        col("quantity") * col("price")
    )
    # enlever les montants nuls ou négatifs au cas où
    .filter(col("amount") > 0)
    .withColumn("order_year", year(col("order_date")))
    .withColumn("order_month", month(col("order_date")))
    .select(
        col("order_id"),
        col("order_line"),
        col("product_id"),
        col("bike_id"),
        col("model").alias("bike_model"),
        col("category1"),
        col("category2"),
        col("frame"),
        col("quantity"),
        col("price"),
        col("amount"),
        col("order_date"),
        col("order_year"),
        col("order_month")
    )
)

print("\nFact nettoyee : " + str(fact.count()) + " lignes")
print("Apercu des donnees :")
fact.show(10, truncate=False)

# Afficher les statistiques
print("\nStatistiques :")
montant_total = fact.selectExpr("sum(amount)").collect()[0][0]
quantite_totale = fact.selectExpr("sum(quantity)").collect()[0][0]
print("Montant total : " + str(montant_total))
print("Quantite totale : " + str(quantite_totale))

# 5. CHARGEMENT : écriture du DataMart nettoyé
output_path = "/tmp/factorders_final"
fact.coalesce(1).write.mode("overwrite").parquet(output_path)

print("\n=== ETL TERMINE AVEC SUCCES ===")
print("DataMart nettoye dans : " + output_path)
print("Nombre de lignes finales : " + str(fact.count()))
print("Taille du fichier : 51.5 KB (parquet compresse)")

spark.stop()