# proyectoCC5212 - Mapa de Reddit: comunidades de subreddits por audiencia compartida

Construimos un grafo de subreddits conectados por audiencia compartida, a partir de un mes de comentarios de Reddit. Sobre ese grafo, Apache Giraph detecta comunidades (Label Propagation) e identifica los subreddits más centrales (PageRank). Con Spark se calcula además qué tan aislada está cada comunidad del resto.

> **Estado:** preprocesamiento y construcción del grafo terminados y validados con una muestra. Pendiente: corrida del mes completo, Giraph (PageRank y LPA) y métricas de comunidades.

---

## Estructura del repositorio

```
.
├── README.md
├── spark/
│   ├── prep.py     # Paso 1: pares (autor, subreddit) distintos y filtrados
│   └── edges.py    # Paso 2: self-join, Jaccard y grafo k-NN para Giraph
└── giraph/         # Paso 3: PageRank y Label Propagation (proyecto mdp-giraph del Lab 8)
    ├── build.xml
    ├── correr_giraph.sh
    ├── datos/juguete.tsv
    └── src/org/mdp/hadoop/...
```

## Requisitos

- Hadoop HDFS
- Apache Spark con PySpark (`spark-submit`)
- Apache Giraph y el JAR del Laboratorio 8 (`mdp-giraph.jar`), para la etapa de grafos

## Datos

- **Fuente:** [fddemarco/pushshift-reddit-comments](https://huggingface.co/datasets/fddemarco/pushshift-reddit-comments) (Hugging Face)
- **Archivo usado:** `RC_2012-10.parquet` (comentarios de octubre de 2012)
- **Filas:** 22.855.205 comentarios
- **Columnas usadas:** `author`, `subreddit` (y `created_utc` solo para crear la muestra)

### Descarga y carga en HDFS

En el servidor, dentro de un `screen`:

```bash
wget -c "https://huggingface.co/datasets/fddemarco/pushshift-reddit-comments/resolve/main/<ruta>/RC_2012-10.parquet"
hdfs dfs -mkdir -p /uhadoop2026/projects/grupo11/raw
hdfs dfs -D dfs.replication=1 -put RC_2012-10.parquet /uhadoop2026/projects/grupo11/raw/
rm RC_2012-10.parquet
```

### Muestra de desarrollo

Las primeras 48 horas del mes, creadas desde la consola `pyspark`. Se filtra por segundos desde el primer comentario para no depender de la zona horaria del servidor:

```python
from pyspark.sql import functions as F
base = "hdfs://cm:9000/uhadoop2026/projects/grupo11"
df = spark.read.parquet(base + "/raw/")
inicio = df.select(F.min("created_utc")).first()[0]
(df.filter(F.col("created_utc") < inicio + 2 * 86400)
   .select("author", "subreddit")
   .write.parquet(base + "/sample/"))
```

---

## Ejecución

Los scripts se copian al servidor con `scp -P 220` y se ejecutan con `spark-submit`. Para abreviar:

```bash
B=hdfs://cm:9000/uhadoop2026/projects/grupo11
```

### Paso 1: `prep.py`, pares autor–subreddit

```bash
spark-submit --master spark://cluster-01:7077 --total-executor-cores 4 --executor-memory 2g \
  prep.py <entrada> <salida>
```

- Quita filas con autor o subreddit nulo.
- Elimina `[deleted]`, `AutoModerator` y cuentas cuyo nombre termina en "bot" (heurística).
- Deja los pares (autor, subreddit) **distintos**.
- Imprime estadísticas de entrada y salida.

### Paso 2: `edges.py`, construcción del grafo

```bash
spark-submit --master spark://cluster-01:7077 --total-executor-cores 4 --executor-memory 2g \
  edges.py <entrada_autor_sub> <carpeta_salida> [MIN_AUT] [MAX_SUBS] [K]
```

1. Descarta a los autores presentes en más de `MAX_SUBS` subreddits.
2. Calcula el número de autores de cada subreddit y descarta los que tienen menos de `MIN_AUT`.
3. **Self-join por autor**: cuenta cuántos autores comparte cada par de subreddits (intersección).
4. Calcula la **similitud de Jaccard**: `inter / (n1 + n2 − inter)`.
5. Para cada subreddit conserva sus `K` vecinos con mayor Jaccard (**grafo k-NN**).
6. Escribe los TSV para Giraph.

### Parámetros

| Parámetro | Por defecto | Qué controla |
|---|---|---|
| `MIN_AUT` | 100 | Mínimo de autores para incluir un subreddit |
| `MAX_SUBS` | 50 | Máximo de subreddits por autor para entrar al self-join |
| `K` | 10 | Vecinos por subreddit en el grafo k-NN |

### Salidas de `edges.py`

| Carpeta | Formato | Contenido |
|---|---|---|
| `aristas/` | Parquet | `s1, s2, inter, n1, n2, jaccard` (cada par una vez, `s1 < s2`) |
| `giraph-knn/` | TSV `origen\tdestino` | Grafo dirigido: cada subreddit apunta a sus `K` más similares. Entrada de PageRank. |
| `giraph-und/` | TSV `origen\tdestino` | Mismo grafo con cada arista en ambos sentidos. Entrada de Label Propagation. |



### Paso 3: Giraph, PageRank y Label Propagation

El proyecto Java está en `giraph/` y parte del proyecto `mdp-giraph` del Laboratorio 8 (mismas clases de entrada y salida, mismo `build.xml`). Las clases nuevas son:

| Clase | Qué hace |
|---|---|
| `cli/LabelPropagation` | LPA sincrónico con ids `Text`. Superstep 0: cada subreddit toma su nombre como etiqueta. Luego adopta la etiqueta más frecuente entre la suya y las de sus vecinos; en empate gana la menor. |
| `pr/LabelPropagationMaster` | Cuenta cuántos vértices cambian en cada superstep (queda en el log del job) y detiene el cómputo cuando ya nadie cambia. Tope de 20 supersteps por si el LPA oscila. |
| `cli/LPARunner` | Igual que `PageRankRunner`, pero lanza `LabelPropagation`. |

**Compilar:** copiar en `giraph/lib/` los 4 JAR de `lib/` del Laboratorio 8 (no se suben al repo por tamaño) y correr `ant` en `giraph/`. Queda `giraph/dist/mdp-giraph.jar`, que se copia al servidor con `scp -P 220`.

**Ejecutar** (`giraph/correr_giraph.sh` tiene todo en orden, partiendo por el grafo de juguete):

```bash
B=/uhadoop2026/projects/grupo11
# PageRank sobre el k-NN dirigido
hadoop jar mdp-giraph.jar PageRankRunner $B/giraph-knn $B/pagerank 4
hadoop jar mdp-giraph.jar SortByRank -D mapreduce.job.reduces=1 $B/pagerank $B/pagerank-s
# Label Propagation sobre el grafo no dirigido
hadoop jar mdp-giraph.jar LPARunner $B/giraph-und $B/lpa 4
```

Entrada: los TSV `origen\tdestino` de `edges.py` (leídos con `TextNullTextEdgeInputFormat`). Salida: `subreddit\trank` en `pagerank/` y `subreddit\tetiqueta` en `lpa/`.

**Prueba con grafo de juguete** (`giraph/datos/juguete.tsv`, dos triángulos unidos por `c–d`): lo esperado es `a, b, c → a` y `d, e, f → d`, convergiendo en el superstep 2.


---

## Resultados con la muestra (48 horas)

Ejecutado en modo local (`--master local[4] --driver-memory 2g`) porque los workers del clúster estaban ocupados. Los resultados no cambian respecto a la ejecución en el clúster.

### Muestra

| Métrica | Valor |
|---|---|
| Comentarios (filas) | 1.530.474 |

### `prep.py`

| Métrica | Valor |
|---|---|
| Pares autor–subreddit distintos | 631.691 |
| Autores distintos | 322.566 |
| Subreddits distintos | 8.105 |

### `edges.py` (`MIN_AUT=20`, `MAX_SUBS=50`, `K=10`)

| Métrica | Valor |
|---|---|
| Subreddits (nodos) | 1.969 |
| Pares de subreddits con autores en común | 167.541 |
| Aristas k-NN dirigidas | 19.668 |
| Líneas en el grafo no dirigido | 26.234 (13.117 conexiones distintas) |
| Pares k-NN recíprocos | 6.551 |


---

## Pendiente

- [ ] `prep.py` y `edges.py` sobre el mes completo (`MIN_AUT=100`)
- [ ] PageRank en Giraph sobre `giraph-knn`
- [ ] Label Propagation en Giraph sobre `giraph-und`
- [ ] Métricas de comunidades (tamaño y aislamiento) y exportación para Gephi