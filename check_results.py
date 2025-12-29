# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Verification_Results").getOrCreate()

# Lire la table de faits créée
fact = spark.read.parquet("/tmp/factorders_final")

print("=== VERIFICATION DES RESULTATS ===")
print("Nombre total de lignes : " + str(fact.count()))
print("\nSchema :")
fact.printSchema()

print("\nApercu des donnees (10 premieres lignes) :")
fact.show(10, truncate=False)

print("\nStatistiques :")
print("Montant total : " + str(fact.selectExpr("sum(amount)").collect()[0][0]))
print("Quantite totale : " + str(fact.selectExpr("sum(quantity)").collect()[0][0]))
print("Nombre moyen par commande : " + str(fact.selectExpr("avg(quantity)").collect()[0][0]))
print("Prix moyen : " + str(fact.selectExpr("avg(price)").collect()[0][0]))

print("\nVentes par annee :")
fact.groupBy("order_year").agg(
    {"amount": "sum", "quantity": "sum"}
).orderBy("order_year").show()

print("\nTop 10 des modeles les plus vendus :")
fact.groupBy("bike_model").agg(
    {"quantity": "sum", "amount": "sum"}
).orderBy("sum(quantity)", ascending=False).show(10, truncate=False)

print("\n=== VERIFICATION TERMINEE ===")
spark.stop()