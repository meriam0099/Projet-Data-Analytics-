# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQL_Analytics").getOrCreate()

# Charge ton DataMart
df = spark.read.parquet("/tmp/factorders_final")
df.createOrReplaceTempView("fact_orders")

print("=== 1. TOP 10 VELOS (CA DESC) ===")
spark.sql("""
    SELECT bike_model, 
           SUM(amount)   AS ca_euros, 
           SUM(quantity) AS unites 
    FROM fact_orders 
    GROUP BY bike_model 
    ORDER BY ca_euros DESC 
    LIMIT 10
""").show()

print("=== 2. CA TOTAL & STATS ===")
spark.sql("""
    SELECT SUM(amount)               AS ca_total_euros,
           COUNT(DISTINCT order_id)  AS nb_commandes,
           AVG(amount)               AS panier_moyen,
           COUNT(*)                  AS nb_lignes
    FROM fact_orders
""").show()

print("=== 3. PRIX MOYEN PAR VELO ===")
spark.sql("""
    SELECT bike_model, 
           ROUND(AVG(price), 2) AS prix_moyen,
           COUNT(*)             AS nb_ventes
    FROM fact_orders 
    GROUP BY bike_model 
    ORDER BY prix_moyen DESC 
    LIMIT 5
""").show()

print("=== 4. CA PAR ANNEE ET MOIS ===")
spark.sql("""
    SELECT 
        YEAR(order_date)  AS annee,
        MONTH(order_date) AS mois,
        SUM(amount)       AS ca_euros,
        SUM(quantity)     AS unites
    FROM fact_orders
    GROUP BY YEAR(order_date), MONTH(order_date)
    ORDER BY annee, mois
""").show(50)

print("=== 5. TOP 10 JOURS PAR CA ===")
spark.sql("""
    SELECT 
        order_date,
        SUM(amount)   AS ca_euros,
        SUM(quantity) AS unites
    FROM fact_orders
    GROUP BY order_date
    ORDER BY ca_euros DESC
    LIMIT 10
""").show()

print("=== 6. CA PAR GAMME DE PRIX ===")
spark.sql("""
    SELECT 
        CASE 
            WHEN price < 1000 THEN 'Entree de gamme (<1000)'
            WHEN price BETWEEN 1000 AND 3000 THEN 'Milieu de gamme (1000-3000)'
            ELSE 'Haut de gamme (>3000)'
        END AS gamme_prix,
        SUM(amount)      AS ca_euros,
        SUM(quantity)    AS unites,
        COUNT(*)         AS nb_lignes
    FROM fact_orders
    GROUP BY 
        CASE 
            WHEN price < 1000 THEN 'Entree de gamme (<1000)'
            WHEN price BETWEEN 1000 AND 3000 THEN 'Milieu de gamme (1000-3000)'
            ELSE 'Haut de gamme (>3000)'
        END
    ORDER BY ca_euros DESC
""").show()

print("=== 7. VELO LE PLUS VENDU PAR ANNEE (QUANTITE) ===")
spark.sql("""
    WITH ventes_par_annee AS (
        SELECT 
            YEAR(order_date) AS annee,
            bike_model,
            SUM(quantity)    AS unites
        FROM fact_orders
        GROUP BY YEAR(order_date), bike_model
    )
    SELECT v.*
    FROM ventes_par_annee v
    JOIN (
        SELECT annee, MAX(unites) AS max_unites
        FROM ventes_par_annee
        GROUP BY annee
    ) m
    ON v.annee = m.annee AND v.unites = m.max_unites
    ORDER BY annee
""").show()

print("=== 8. CA ET QUANTITE MOYENS PAR COMMANDE ===")
spark.sql("""
    SELECT 
        AVG(amount)   AS ca_moyen_par_ligne,
        AVG(quantity) AS qte_moyenne_par_ligne
    FROM fact_orders
""").show()

print("=== 9. NOMBRE DE MODELES DIFFERENTS VENDUS ===")
spark.sql("""
    SELECT 
        COUNT(DISTINCT bike_model) AS nb_modeles,
        COUNT(DISTINCT bike_id)    AS nb_bikes_ids
    FROM fact_orders
""").show()

spark.stop()
print("✅ REQUETES SQL TERMINEES")
