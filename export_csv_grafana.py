from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Export_CSV_Velos").getOrCreate()

df = spark.read.parquet("/workspace/factorders_final")
df.createOrReplaceTempView("fact_orders")

# 1. TOP 10 VELOS (CA DESC)
top10_velos = spark.sql("""
    SELECT bike_model, 
           SUM(amount)   AS ca_euros, 
           SUM(quantity) AS unites 
    FROM fact_orders 
    GROUP BY bike_model 
    ORDER BY ca_euros DESC 
    LIMIT 10
""")

# 2. CA PAR ANNEE ET MOIS
ca_annee_mois = spark.sql("""
    SELECT 
        YEAR(order_date)  AS annee,
        MONTH(order_date) AS mois,
        SUM(amount)       AS ca_euros,
        SUM(quantity)     AS unites
    FROM fact_orders
    GROUP BY YEAR(order_date), MONTH(order_date)
    ORDER BY annee, mois
""")

# 3. CA PAR GAMME DE PRIX
ca_par_gamme = spark.sql("""
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
""")

print("=== EXPORT CSV POUR GRAFANA ===")

top10_velos.coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv("/workspace/export/top10_velos")

ca_annee_mois.coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv("/workspace/export/ca_annee_mois")

ca_par_gamme.coalesce(1).write.mode("overwrite").option("header", "true") \
    .csv("/workspace/export/ca_par_gamme")

spark.stop()
print("✅ CSV exportés dans /workspace/export")
