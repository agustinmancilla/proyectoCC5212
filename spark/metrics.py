import sys
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


# Lee (bajo <base>)
#   grafo/aristas/     parquet  s1, s2, inter, n1, n2, jaccard   (edges.py)
#   grafo/giraph-und/  TSV      origen \t destino                (edges.py)
#   pagerank/          TSV      subreddit \t rank                (Giraph)
#   lpa/               TSV      subreddit \t etiqueta            (Giraph)
#
# Escribe (bajo <base>/resultados/, CSV con header, un archivo por carpeta)
#   nodes/                 Id, Label, comunidad, pagerank, n_autores
#   edges/                 Source, Target, Weight, Type
#   comunidades/           Id, Label, n_subreddits, frac_knn, esperada_knn,
#                          frac_autores, esperada_autores, top_subreddits
#   enlaces_comunidades/   Source, Target, Weight, autores_compartidos, Type

if len(sys.argv) < 2:
    print("Uso: spark-submit metrics.py <base> [TOP]")
    sys.exit(1)

base = sys.argv[1].rstrip("/")
TOP = int(sys.argv[2]) if len(sys.argv) > 2 else 5
salida = base + "/resultados"

spark = SparkSession.builder.appName("G11-metrics").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def tsv(ruta, c1, c2):
    return spark.read.option("sep", "\t").csv(ruta).toDF(c1, c2)


def escribir(df, nombre):
    df.coalesce(1).write.option("header", True).csv(salida + "/" + nombre)


aristas = spark.read.parquet(base + "/grafo/aristas")
und = tsv(base + "/grafo/giraph-und", "o", "d")
pr = (tsv(base + "/pagerank", "subreddit", "pagerank")
      .withColumn("pagerank", F.col("pagerank").cast("double")))
lpa = tsv(base + "/lpa", "subreddit", "comunidad").cache()

#nodos para graficar
n_aut = (aristas.select(F.col("s1").alias("subreddit"), F.col("n1").alias("n_autores"))
         .union(aristas.select(F.col("s2").alias("subreddit"), F.col("n2").alias("n_autores")))
         .distinct())

nodos = (lpa.join(pr, "subreddit", "left").join(n_aut, "subreddit", "left")
         .select(F.col("subreddit").alias("Id"), F.col("subreddit").alias("Label"),
                 "comunidad", "pagerank", "n_autores")
         .cache())
escribir(nodos.orderBy(F.desc("pagerank")), "nodes")

#aristas del k-NN para graficar (solo un sentido, con peso jaccard)
pesos = aristas.select(F.col("s1").alias("o"), F.col("s2").alias("d"), "jaccard")
aristas_knn = (und.filter(F.col("o") < F.col("d"))
               .join(pesos, ["o", "d"], "left")
               .select(F.col("o").alias("Source"), F.col("d").alias("Target"),
                       F.col("jaccard").alias("Weight"), F.lit("Undirected").alias("Type"))
               .cache())
escribir(aristas_knn, "edges")

#aislamiento sobre el grafo k-NN 
#giraph-und tiene cada conexion en los dos sentidos 
#para una comunidad C, entre las lineas con origen en C, las internas aparecen 2 veces y las que salen 1 vez: frac_knn = 2I / (2I + E) = 1 - conductancia 
#fraccion entre comunidades internas y externas, conductancia es cuanto se "escapa" de la comunidad (para ver si es aislada o no)
#tambien se calcula la fraccion que se esperaria si las aristas se distribuyeran al azar (esperada_knn = (2I + E) / volumen total = lineas totales de giraph-und)
co = lpa.select(F.col("subreddit").alias("o"), F.col("comunidad").alias("co"))
cd = lpa.select(F.col("subreddit").alias("d"), F.col("comunidad").alias("cd"))
und_c = und.join(co, "o").join(cd, "d").cache()

m_knn = (und_c.groupBy(F.col("co").alias("comunidad"))
         .agg(F.count("*").alias("vol_knn"),
              F.sum(F.when(F.col("co") == F.col("cd"), 1).otherwise(0)).alias("int_knn")))
total_knn = und_c.count()
m_knn = (m_knn.withColumn("frac_knn", F.col("int_knn") / F.col("vol_knn"))
         .withColumn("esperada_knn", F.col("vol_knn") / F.lit(float(total_knn)))
         .select("comunidad", "frac_knn", "esperada_knn"))

#aislamiento sobre todas las aristas pesadas por autores compartidos 
c1 = lpa.select(F.col("subreddit").alias("s1"), F.col("comunidad").alias("c1"))
c2 = lpa.select(F.col("subreddit").alias("s2"), F.col("comunidad").alias("c2"))
e = (aristas.select("s1", "s2", "inter").join(c1, "s1").join(c2, "s2")
     .withColumn("interna", F.col("c1") == F.col("c2"))
     .cache())
lados = (e.select(F.col("c1").alias("comunidad"), "inter", "interna")
         .union(e.select(F.col("c2").alias("comunidad"), "inter", "interna")))
m_aut = (lados.groupBy("comunidad")
         .agg(F.sum("inter").alias("vol_aut"),
              F.sum(F.when(F.col("interna"), F.col("inter")).otherwise(0)).alias("int_aut")))
total_aut = m_aut.agg(F.sum("vol_aut")).first()[0]
m_aut = (m_aut.withColumn("frac_autores", F.col("int_aut") / F.col("vol_aut"))
         .withColumn("esperada_autores", F.col("vol_aut") / F.lit(float(total_aut)))
         .select("comunidad", "frac_autores", "esperada_autores"))

#PAGERANK METRICS
#miembros con mas PageRank de cada comunidad 
w = Window.partitionBy("comunidad").orderBy(F.desc("pagerank"), F.col("Id"))
filas_top = (nodos.withColumn("r", F.row_number().over(w))
             .filter(F.col("r") <= TOP)
             .select("comunidad", "r", "Id").collect())
miembros = {}
for f in sorted(filas_top, key=lambda f: (f["comunidad"], f["r"])):
    miembros.setdefault(f["comunidad"], []).append(f["Id"])
top = spark.createDataFrame(
    [(c, m[0], "|".join(m)) for c, m in miembros.items()],
    ["comunidad", "Label", "top_subreddits"])

#tabla de comunidades 
tam = nodos.groupBy("comunidad").agg(F.count("*").alias("n_subreddits"))
comunidades = (tam.join(top, "comunidad", "left")
               .join(m_knn, "comunidad", "left")
               .join(m_aut, "comunidad", "left")
               .select(F.col("comunidad").alias("Id"), "Label", "n_subreddits",
                       "frac_knn", "esperada_knn", "frac_autores", "esperada_autores",
                       "top_subreddits")
               .orderBy(F.desc("n_subreddits"), F.col("Id"))
               .cache())
escribir(comunidades, "comunidades")

#enlaces entre comunidades (mapa de comunidades para Gephi) 
par_knn = (und_c.filter((F.col("o") < F.col("d")) & (F.col("co") != F.col("cd")))
           .select(F.least("co", "cd").alias("Source"), F.greatest("co", "cd").alias("Target"))
           .groupBy("Source", "Target").agg(F.count("*").alias("Weight")))
par_aut = (e.filter(~F.col("interna"))
           .select(F.least("c1", "c2").alias("Source"), F.greatest("c1", "c2").alias("Target"), "inter")
           .groupBy("Source", "Target").agg(F.sum("inter").alias("autores_compartidos")))
enlaces = (par_knn.join(par_aut, ["Source", "Target"], "left")
           .withColumn("Type", F.lit("Undirected"))
           .orderBy(F.desc("Weight")))
escribir(enlaces, "enlaces_comunidades")

#estadisticas y comprobaciones 
n_nodos = nodos.count()
n_aristas = aristas_knn.count()
print("Subreddits (nodos):               ", n_nodos)
print("  sin PageRank (debe ser 0):      ", nodos.filter(F.col("pagerank").isNull()).count())
print("  sin n_autores (debe ser 0):     ", nodos.filter(F.col("n_autores").isNull()).count())
print("Aristas k-NN no dirigidas:        ", n_aristas)
print("  sin peso (debe ser 0):          ", aristas_knn.filter(F.col("Weight").isNull()).count())
print("  lineas en giraph-und / 2:       ", total_knn / 2.0)
print("Comunidades:                      ", comunidades.count())
print("Suma de n_subreddits:             ", comunidades.agg(F.sum("n_subreddits")).first()[0])
print("Pares de comunidades conectadas:  ", enlaces.count())
print("")
print("Comunidades de 10 o mas subreddits, de la mas aislada a la menos (frac_knn):")
(comunidades.filter(F.col("n_subreddits") >= 10)
 .orderBy(F.desc("frac_knn"))
 .select("Label", "n_subreddits",
         F.round("frac_knn", 3).alias("frac_knn"),
         F.round("esperada_knn", 3).alias("esperada_knn"),
         F.round("frac_autores", 3).alias("frac_autores"),
         F.round("esperada_autores", 3).alias("esperada_autores"),
         "top_subreddits")
 .show(200, truncate=False))

spark.stop()