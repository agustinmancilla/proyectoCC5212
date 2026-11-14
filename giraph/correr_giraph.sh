#!/bin/bash
# Paso 5: PageRank y Label Propagation en Giraph (rol P3).
# Correr en el clúster, en la carpeta donde está mdp-giraph.jar:
#   bash correr_giraph.sh [workers]
# Requiere que edges.py ya haya dejado giraph-knn/ y giraph-und/ en HDFS.
set -e

B=/uhadoop2026/projects/grupo11
W=${1:-4}
JAR=mdp-giraph.jar

# 0) Prueba con el grafo de juguete (dos triángulos unidos por c-d).
#    Esperado: a, b, c -> etiqueta a ; d, e, f -> etiqueta d
hdfs dfs -mkdir -p $B/juguete
hdfs dfs -put -f datos/juguete.tsv $B/juguete/
hadoop jar $JAR LPARunner $B/juguete $B/lpa-juguete 1
hdfs dfs -cat $B/lpa-juguete/part* | sort

# 1) PageRank sobre el k-NN dirigido (clase del lab 8, sin cambios)
time hadoop jar $JAR PageRankRunner $B/grafo/giraph-knn $B/pagerank $W
hadoop jar $JAR SortByRank -D mapreduce.job.reduces=1 $B/pagerank $B/pagerank-s
hdfs dfs -cat $B/pagerank-s/part* | head -30

# 2) Label Propagation sobre el grafo no dirigido
time hadoop jar $JAR LPARunner $B/grafo/giraph-und $B/lpa $W

# Comunidades más grandes (subreddit \t etiqueta)
hdfs dfs -cat $B/lpa/part* | cut -f2 | sort | uniq -c | sort -rn | head -20
echo "Número de comunidades:"
hdfs dfs -cat $B/lpa/part* | cut -f2 | sort -u | wc -l
