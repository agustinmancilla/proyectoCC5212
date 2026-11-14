# proyectoCC5212 - Mapa de Reddit: comunidades de subreddits por audiencia compartida

Construimos un grafo de subreddits conectados por audiencia compartida, a partir de un mes de comentarios de Reddit. Sobre ese grafo, Apache Giraph detecta comunidades (Label Propagation) e identifica los subreddits más centrales (PageRank). Con Spark se calcula además qué tan aislada está cada comunidad del resto.



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



## Ejecución

Los scripts se copian al servidor con `scp -P 220` y se ejecutan con `spark-submit`. Para abreviar:

```bash
B=hdfs://cm:9000/uhadoop2026/projects/grupo11
```

Comandos usados para el mes completo:

```bash
spark-submit --master spark://cluster-01:7077 --total-executor-cores 4 --executor-memory 2g \
  prep.py $B/raw/ $B/autor_sub/

spark-submit --master spark://cluster-01:7077 --total-executor-cores 4 --executor-memory 2g \
  edges.py $B/autor_sub/ $B/grafo 100 50 10
```

Si los workers del clúster no entregan recursos, se puede reemplazar `--master spark://cluster-01:7077 --total-executor-cores 4 --executor-memory 2g` por `--master local[4] --driver-memory 4g`. Los resultados son los mismos.

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


## Resultados con el mes completo (octubre de 2012)

### `prep.py`

| Métrica | Valor |
|---|---|
| Comentarios (filas de entrada) | 22.855.205 |
| Pares autor–subreddit distintos | 4.918.269 |
| Autores distintos | 1.130.728 |
| Subreddits distintos | 20.831 |

En promedio, cada autor comentó en unos 4,3 subreddits distintos durante el mes.

### `edges.py` (`MIN_AUT=100`, `MAX_SUBS=50`, `K=10`)

| Métrica | Valor |
|---|---|
| Subreddits (nodos) | 2.695 |
| Pares de subreddits con autores en común | 1.414.749 |
| Aristas k-NN dirigidas | 26.950 |
| Líneas en el grafo no dirigido | 38.758 (19.379 conexiones distintas) |
| Pares k-NN recíprocos | 7.571 |


---


## Pendiente

- [x] `prep.py` y `edges.py` sobre el mes completo (`MIN_AUT=100`)
- [ ] PageRank en Giraph sobre `giraph-knn`
- [ ] Label Propagation en Giraph sobre `giraph-und`
- [ ] Métricas de comunidades (tamaño y aislamiento) y exportación para Gephi
