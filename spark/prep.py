import sys
from pyspark.sql import SparkSession, functions as F

if len(sys.argv) != 3:
    print("Uso: spark-submit prep.py <entrada> <salida>")
    sys.exit(1)

entrada, salida = sys.argv[1], sys.argv[2]

spark = SparkSession.builder.appName("G11-prep").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(entrada).select("author", "subreddit")

autor_sub = (df
    .filter(F.col("author").isNotNull() & F.col("subreddit").isNotNull())
    .filter(~F.col("author").isin("[deleted]", "AutoModerator"))
    .filter(~F.lower(F.col("author")).endswith("bot"))   
    .distinct())

autor_sub.write.parquet(salida)

# Estadísticas 
res = spark.read.parquet(salida)
print("Filas de entrada:          ", df.count())
print("Pares autor-subreddit:     ", res.count())
print("Autores distintos:         ", res.select("author").distinct().count())
print("Subreddits distintos:      ", res.select("subreddit").distinct().count())

spark.stop()