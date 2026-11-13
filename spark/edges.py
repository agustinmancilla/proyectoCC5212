import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


if len(sys.argv) < 3:
    print("Uso: spark-submit edges.py <entrada> <salida> [MIN_AUT] [MAX_SUBS] [K]")
    sys.exit(1)

entrada, salida = sys.argv[1], sys.argv[2]
MIN_AUT  = int(sys.argv[3]) if len(sys.argv) > 3 else 100
MAX_SUBS = int(sys.argv[4]) if len(sys.argv) > 4 else 50
K        = int(sys.argv[5]) if len(sys.argv) > 5 else 10

spark = SparkSession.builder.appName("G11-edges").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Filtrar autores hiperactivos 
a = spark.read.parquet(entrada)

subs_por_autor = a.groupBy("author").agg(F.count("*").alias("k"))
autores_ok = subs_por_autor.filter(F.col("k") <= MAX_SUBS).select("author")
a = a.join(autores_ok, "author")

# Tamaño de cada subreddit y filtro de subreddits chicos
n_sub = (a.groupBy("subreddit").agg(F.count("*").alias("n"))
          .filter(F.col("n") >= MIN_AUT))
a = a.join(n_sub.select("subreddit"), "subreddit").cache()

# Self-join 
x = a.withColumnRenamed("subreddit", "s1")
y = a.withColumnRenamed("subreddit", "s2")
inter = (x.join(y, "author")
          .filter(F.col("s1") < F.col("s2"))
          .groupBy("s1", "s2").agg(F.count("*").alias("inter")))

# Jaccard 
n1 = n_sub.select(F.col("subreddit").alias("s1"), F.col("n").alias("n1"))
n2 = n_sub.select(F.col("subreddit").alias("s2"), F.col("n").alias("n2"))
aristas = (inter.join(n1, "s1").join(n2, "s2")
           .withColumn("jaccard",
                       F.col("inter") / (F.col("n1") + F.col("n2") - F.col("inter"))))
aristas.write.parquet(salida + "/aristas")
aristas = spark.read.parquet(salida + "/aristas")   

# k-NN
sim = (aristas.select("s1", "s2", "jaccard")
       .union(aristas.select(F.col("s2").alias("s1"), F.col("s1").alias("s2"), "jaccard")))
w = Window.partitionBy("s1").orderBy(F.desc("jaccard"), F.col("s2"))
knn = (sim.withColumn("r", F.row_number().over(w))
          .filter(F.col("r") <= K)
          .select("s1", "s2"))

# TSV para Giraph 
knn.write.option("sep", "\t").csv(salida + "/giraph-knn")
und = knn.union(knn.select(F.col("s2").alias("s1"), F.col("s1").alias("s2"))).distinct()
und.write.option("sep", "\t").csv(salida + "/giraph-und")

#Estadísticas
print("Parámetros: MIN_AUT=%d MAX_SUBS=%d K=%d" % (MIN_AUT, MAX_SUBS, K))
print("Subreddits (nodos):        ", n_sub.count())
print("Pares con autores en común:", aristas.count())
print("Aristas k-NN (dirigidas):  ", spark.read.option("sep", "\t").csv(salida + "/giraph-knn").count())
print("Aristas no dirigidas (x2): ", spark.read.option("sep", "\t").csv(salida + "/giraph-und").count())

spark.stop()