# proyectoCC5212 - Mapa de Reddit: comunidades de subreddits por audiencia compartida

Construimos un grafo de subreddits conectados por audiencia compartida, a partir de un mes de comentarios de Reddit. Sobre ese grafo, Apache Giraph detecta comunidades (Label Propagation) e identifica los subreddits más centrales (PageRank). Con Spark se calcula además qué tan aislada está cada comunidad del resto.

> **Estado (14/11/2026):** preprocesamiento, grafo y Giraph (PageRank y Label Propagation) ejecutados sobre el mes completo. Pendiente: métricas de aislamiento (`metrics.py`) y mapa en Gephi.



## Estructura del repositorio

```
.
├── README.md
├── spark/
│   ├── prep.py     # Paso 1: pares (autor, subreddit) distintos y filtrados
│   └── edges.py    # Paso 2: self-join, Jaccard y grafo k-NN para Giraph
└── giraph/         # Paso 3: PageRank y Label Propagation (proyecto mdp-giraph del Lab 8)
    ├── build.xml           # compila y arma dist/mdp-giraph.jar con ant
    ├── correr_giraph.sh    # comandos del clúster en orden
    ├── datos/juguete.tsv   # grafo de prueba
    └── src/org/mdp/hadoop/
        ├── cli/            # PageRank, LabelPropagation, sus runners y SortByRank
        ├── io/             # formato de entrada (TSV origen\tdestino) y de salida
        └── pr/             # masters con los agregadores de PageRank y LPA
```

## Requisitos

- Hadoop HDFS
- Apache Spark con PySpark (`spark-submit`)
- Apache Giraph 1.3 y Apache Ant (Java 8) para compilar `giraph/`; los 4 JAR de `giraph/lib/` se copian desde el Laboratorio 8 (no se suben al repo por tamaño)

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


### Paso 3: Giraph, PageRank y Label Propagation

El proyecto Java está en `giraph/` y parte del proyecto `mdp-giraph` del Laboratorio 8 (mismas clases de entrada y salida, mismo `build.xml`). Las clases nuevas son:

| Clase | Qué hace |
|---|---|
| `cli/LabelPropagation` | LPA sincrónico con ids `Text`. Superstep 0: cada subreddit toma su nombre como etiqueta. Luego adopta la etiqueta más frecuente entre la suya y las de sus vecinos; en empate gana la menor. |
| `pr/LabelPropagationMaster` | Cuenta cuántos vértices cambian en cada superstep y detiene el cómputo cuando ya nadie cambia. Tope de 20 supersteps por si el LPA oscila. |
| `cli/LPARunner` | Igual que `PageRankRunner`, pero lanza `LabelPropagation`. |

`PageRank`, `PageRankAgg`, `PageRankRunner` y `SortByRank` son los del laboratorio, sin cambios (10 iteraciones, factor de amortiguación 0,85).

**Compilar:** copiar en `giraph/lib/` los 4 JAR de `lib/` del Laboratorio 8 y correr `ant` en `giraph/`. Queda `giraph/dist/mdp-giraph.jar`, que se copia al servidor con `scp -P 220` a `/data/2026/uhadoop/projects/grupo11/`.

**Ejecutar** (`giraph/correr_giraph.sh` tiene todo en orden, partiendo por el grafo de juguete):

```bash
B=/uhadoop2026/projects/grupo11
# PageRank sobre el k-NN dirigido
hadoop jar mdp-giraph.jar PageRankRunner $B/grafo/giraph-knn $B/pagerank 4
hadoop jar mdp-giraph.jar SortByRank -D mapreduce.job.reduces=1 $B/pagerank $B/pagerank-s
# Label Propagation sobre el grafo no dirigido
hadoop jar mdp-giraph.jar LPARunner $B/grafo/giraph-und $B/lpa 4
```

Entrada: los TSV `origen\tdestino` de `edges.py`, leídos con `TextNullTextEdgeInputFormat`. Salidas en HDFS:

| Carpeta | Formato | Contenido |
|---|---|---|
| `pagerank/` | TSV `subreddit\trank` | PageRank de cada subreddit |
| `pagerank-s/` | TSV `subreddit\trank` | Lo mismo, ordenado de mayor a menor |
| `lpa/` | TSV `subreddit\tetiqueta` | Comunidad de cada subreddit (la etiqueta es el nombre de uno de sus miembros) |

**Prueba con grafo de juguete** (`giraph/datos/juguete.tsv`, dos triángulos unidos por `c–d`): resultado `a, b, c → a` y `d, e, f → d`, igual al esperado.


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


### PageRank en Giraph (`giraph-knn`)

| Métrica | Valor |
|---|---|
| Vértices / aristas | 2.695 / 26.950 |
| Workers | 4 |
| Supersteps | 13 (10 iteraciones + activar, contar y cerrar) |
| Mensajes enviados | 296.450 |
| Tiempo de cómputo en Giraph | 13,3 s (50 s el job completo) |
| `SortByRank` | 36 s |

Subreddits más centrales (si el rank fuera parejo, cada uno tendría 1/2.695 ≈ 0,00037):

| # | Subreddit | PageRank |
|---|---|---|
| 1 | todayilearned | 0,00850 |
| 2 | AdviceAnimals | 0,00795 |
| 3 | videos | 0,00755 |
| 4 | WTF | 0,00697 |
| 5 | politics | 0,00676 |
| 6 | gaming | 0,00657 |
| 7 | pics | 0,00655 |
| 8 | funny | 0,00636 |
| 9 | IAmA | 0,00600 |
| 10 | AskReddit | 0,00550 |

Los más centrales son los subreddits *default* de 2012: comparten audiencia con casi todos, así que muchos los tienen entre sus 10 más parecidos.

### Label Propagation en Giraph (`giraph-und`)

| Métrica | Valor |
|---|---|
| Vértices / aristas | 2.695 / 38.758 |
| Workers | 4 |
| Supersteps | 21 (llegó al tope de 20) |
| Mensajes enviados | 775.160 |
| Tiempo de cómputo en Giraph | 15,2 s (45 s el job completo) |
| Comunidades | 118 |
| Comunidad más grande | 273 subreddits (10 % del grafo) |
| Subreddits inestables al terminar | 8 de 2.695 (0,3 %) |

Como el LPA sincrónico puede oscilar, se midió con la salida cuántos subreddits cambiarían en un superstep más: solo 8 de frontera (sociology, askphilosophy, linguistics, Archaeology, MapPorn, polandball, rockhounds y CallOfDuty).

Comunidades más grandes. El tema se asignó a mano a partir de sus 5 miembros con mayor PageRank:

| # | Subreddits | Tema | Miembros con más PageRank |
|---|---|---|---|
| 1 | 273 | Reddit general | TrueReddit, news, offbeat, SubredditDrama, gifs |
| 2 | 125 | Videojuegos de PC | Games, truegaming, GameDeals, gamernews, Minecraft |
| 3 | 97 | Contenido adulto (NSFW) | Boobies, milf, Amateur, curvy, ass |
| 4 | 88 | Comunidad femenina, moda y belleza | TwoXChromosomes, AskWomen, TrollXChromosomes, MakeupAddiction, femalefashionadvice |
| 5 | 85 | Consolas y fandoms | pokemon, TheLastAirbender, PS3, xbox360, zelda |
| 6 | 75 | Series de TV | community, television, PandR, thewalkingdead, arresteddevelopment |
| 7 | 69 | Autos | Autos, Justrolledintotheshop, carporn, cars, forza |
| 8 | 65 | Hacer música | WeAreTheMusicMakers, audioengineering, Guitar, Bass, musictheory |
| 9 | 52 | Deportes de EE.UU. | baseball, OaklandAthletics, motorcitykitties, sports, Braves |
| 10 | 49 | Fitness | xxfitness, Fitness, weightroom, swoleacceptance, fitnesscirclejerk |
| 11 | 49 | Armas de fuego | Firearms, guns, CCW, GunPorn, ar15 |
| 12 | 48 | Cocina, ahorro y DIY | Frugal, DIY, Cooking, food, BuyItForLife |
| 13 | 45 | Linux y programación | linux, linuxquestions, linux4noobs, programming, linux_gaming |
| 14 | 42 | Mascotas | Pets, dogpictures, cats, dogs, lookatmydog |
| 15 | 38 | Música electrónica y DJ | edmproduction, DJs, electrohouse, electronicmusic, Beatmatch |

**Política.** `politics`, `worldnews` y `atheism` quedan en una comunidad de 26 subreddits junto a `AdviceAnimals` (el núcleo de los *defaults*). Los subreddits de política de nicho forman otra comunidad aparte, de 24, que mezcla todos los lados: `Republican`, `Conservative`, `conservatives`, `prolife` y `Romney` junto a `democrats`, `Liberal`, `progressive`, `obama`, `GreenParty` y `GaryJohnson`, además de `PoliticalDiscussion`, `NeutralPolitics`, `moderatepolitics` y otros. `Libertarian` cae con `Anarchism` y `Christianity` con el bloque de debate religioso (`AdviceAtheists`).

---

## Limitaciones

- Comentar en dos subreddits no implica pertenecer a ambos.
- La detección de bots es una heurística simple (nombre terminado en "bot").
- Los autores en más de 50 subreddits no entran al self-join, así que el Jaccard queda algo subestimado para los subreddits grandes.
- LPA no es determinista entre implementaciones y, en su versión sincrónica, puede oscilar: 8 subreddits no se estabilizaron al llegar al tope de 20 supersteps.
- Las etiquetas de LPA son nombres de miembros, no temas; el tema de cada comunidad se asignó a mano.

## Pendiente

- [x] `prep.py` y `edges.py` sobre el mes completo (`MIN_AUT=100`)
- [x] PageRank en Giraph sobre `giraph-knn`
- [x] Label Propagation en Giraph sobre `giraph-und`
- [ ] Métricas de comunidades (tamaño y aislamiento) con `metrics.py`
- [ ] Exportación de `nodes.csv` y `edges.csv` y mapa en Gephi
- [ ] Opcional: repetir con septiembre y noviembre de 2012 y comparar el bloque político alrededor de la elección
