# Uso:
# pip install networkx matplotlib scipy pandas
# python viz/mapas.py resultados/

#Lee   resultados/{nodes,edges,comunidades,enlaces_comunidades}.csv
#Crea  resultados/mapa_subreddits.png    grafo completo, color por comunidad
#      resultados/mapa_destacado.png     mismas posiciones, 6 comunidades destacadas
#      resultados/mapa_comunidades.png   una burbuja por comunidad, color por aislamiento

import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
from matplotlib.colors import LinearSegmentedColormap

if len(sys.argv) != 2:
    print("Uso: python mapas.py <carpeta_resultados>")
    sys.exit(1)
d = sys.argv[1]

nodes = pd.read_csv(os.path.join(d, "nodes.csv"))
edges = pd.read_csv(os.path.join(d, "edges.csv"))
com = pd.read_csv(os.path.join(d, "comunidades.csv"))
enl = pd.read_csv(os.path.join(d, "enlaces_comunidades.csv"))

SEED = 11
GRIS = "#d4d4d4"
FONDO = "white"
etiqueta_com = dict(zip(com["Id"], com["Label"]))   # Id de LPA -> miembro con más PageRank

# Temas asignados a mano en el README (sección "Label Propagation") 
# a partir de los 5 miembros con más PageRank de cada comunidad
# Clave = Id de LPA en comunidades.csv
TEMAS = {
    "4chan": "Reddit general",
    "0x10c": "Videojuegos de PC",
    "Amateur": "Contenido adulto (NSFW)",
    "2XLookbook": "Comunidad femenina, moda y belleza",
    "3DS": "Consolas y fandoms",
    "30ROCK": "Series de TV",
    "240sx": "Autos",
    "Bass": "Hacer música",
    "49ers": "Deportes de EE.UU.",
    "AdvancedFitness": "Fitness",
    "1911": "Armas de fuego",
    "AskCulinary": "Cocina, ahorro y DIY",
    "AskComputerScience": "Linux y programación",
    "Animals": "Mascotas",
    "Beatmatch": "Música electrónica y DJ",
    "GaybrosGoneWild": "Comunidad gay masculina",
    # comunidades destacadas que no están entre las 15 más grandes
    "2012Elections": "Política",
    "AdviceAnimals": "Defaults",
}


def nombre(c):
    """Tema de la comunidad si tiene uno asignado; si no, su miembro con más PageRank."""
    return TEMAS.get(c, etiqueta_com[c])


# Comunidades destacadas en mapa_destacado.png (Id de LPA -> texto de la leyenda y color).
# Criterios (ver README, paso 5), calculados sobre comunidades.csv y nodes.csv.
# Exceso por autores = frac_autores - esperada_autores (medida de aislamiento, ver README, paso 4).
#   AdviceAnimals: contiene a los 10 subreddits con más PageRank
#   4chan:         la comunidad más grande (273 subreddits)
#   AskFeminists:  la más aislada por exceso por autores (0,1364)
#   Amateur:       segunda por exceso por autores, casi empatada (0,1356), y la más separada en el k-NN
#   AbandonedPorn: la menos aislada (menor exceso por autores y menor frac_knn, entre las de 20+ subreddits)
#   2012Elections: la comunidad de política comentada en la sección de Label Propagation
DESTACADAS = {
    "AdviceAnimals": ("Defaults: los 10 con más PageRank (todayilearned, AdviceAnimals, politics…)", "#1f77b4"),
    "4chan": ("Reddit general: la más grande (TrueReddit, news, SubredditDrama…)", "#2ca02c"),
    "AskFeminists": ("SRSMeta: la más aislada (SRSMeta, SRSBusiness, ShitRedditSays…)", "#8c564b"),
    "Amateur": ("Contenido adulto (NSFW): 2.ª más aislada, la más separada en el k-NN (Boobies, milf…)", "#ff7f0e"),
    "AbandonedPorn": ("MapPorn: la menos aislada (MapPorn, CityPorn, EarthPorn…)", "#9467bd"),
    "2012Elections": ("Política (PoliticalDiscussion, Republican, democrats…)", "#d62728"),
}


# ---------------------------------------------------------------- grafo de subreddits
G = nx.Graph()
for r in nodes.itertuples(index=False):
    G.add_node(r.Id, comunidad=r.comunidad, pagerank=r.pagerank)
for r in edges.itertuples(index=False):
    G.add_edge(r.Source, r.Target, weight=r.Weight)
print("Subreddits:", G.number_of_nodes(), "aristas:", G.number_of_edges())

print("Calculando posiciones (ForceAtlas2, tarda unos 3-4 minutos)...")
# Sin pesos: el Jaccard es muy chico (0,01-0,3) y con pesos la atraccion casi no actua,
# asi que las comunidades no se separan. Sin pesos, cada arista k-NN atrae igual.
pos = nx.forceatlas2_layout(G, max_iter=600, scaling_ratio=2.0, gravity=1.0,
                            weight=None, seed=SEED)

pr = nodes.set_index("Id")["pagerank"]
pr_max = pr.max()


def tam(s, minimo=4, maximo=420):
    return minimo + (maximo - minimo) * (pr[s] / pr_max)


def dibujar_aristas(ax, color_de):
    """Aristas finas; cada una toma el color de su comunidad si sus dos extremos comparten una."""
    grupos = {}
    for u, v in G.edges():
        cu, cv = G.nodes[u]["comunidad"], G.nodes[v]["comunidad"]
        c = color_de.get(cu) if cu == cv else None
        grupos.setdefault(c, []).append((u, v))
    for c, lista in grupos.items():
        nx.draw_networkx_edges(G, pos, edgelist=lista, ax=ax, width=0.25,
                               edge_color=c if c else "#bdbdbd",
                               alpha=0.35 if c else 0.12)


def etiquetar(ax, subs, tam_fuente=8, negrita=False):
    for s in subs:
        x, y = pos[s]
        ax.annotate(s, (x, y), xytext=(4, 4), textcoords="offset points",
                    fontsize=tam_fuente, fontweight="bold" if negrita else "normal",
                    color="#222222",
                    bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.7))


def guardar(fig, nombre):
    ruta = os.path.join(d, nombre)
    fig.savefig(ruta, dpi=200, bbox_inches="tight", facecolor=FONDO)
    plt.close(fig)
    print("Escrito:", ruta)


# Mapa 1: las 15 comunidades más grandes con color, el resto gris
mas_grandes = list(com.sort_values("n_subreddits", ascending=False)["Id"][:15])
# tab20 sin sus dos grises (índices 14 y 15), para no confundirlos con "otras comunidades"
paleta = [c for i, c in enumerate(plt.get_cmap("tab20").colors) if i not in (14, 15)]
paleta.append("#17becf")
color_com = {c: paleta[i % len(paleta)] for i, c in enumerate(mas_grandes)}

fig, ax = plt.subplots(figsize=(16, 16))
ax.set_facecolor(FONDO)
dibujar_aristas(ax, color_com)
orden = sorted(G.nodes(), key=lambda s: pr[s])          # los grandes encima
nx.draw_networkx_nodes(G, pos, nodelist=orden, ax=ax,
                       node_size=[tam(s) for s in orden],
                       node_color=[color_com.get(G.nodes[s]["comunidad"], GRIS) for s in orden],
                       linewidths=0.3, edgecolors="white")
etiquetar(ax, list(pr.sort_values(ascending=False).index[:20]))
for c in mas_grandes:
    ax.scatter([], [], s=80, color=color_com[c],
               label="%s (%d)" % (nombre(c), int(com.set_index("Id").loc[c, "n_subreddits"])))
ax.scatter([], [], s=80, color=GRIS, label="otras 103 comunidades")
ax.legend(loc="upper left", bbox_to_anchor=(1.0, 0.95), fontsize=11, frameon=False,
          title="Comunidad (n.º de subreddits)", title_fontsize=12)
ax.set_title("Mapa de Reddit, octubre 2012: 2.695 subreddits conectados por audiencia compartida\n"
             "Color = comunidad (Label Propagation) · tamaño = PageRank", fontsize=15)
ax.axis("off")
guardar(fig, "mapa_subreddits.png")

# Mapa 2: mismas posiciones, solo 6 comunidades destacadas
color_dest = {c: col for c, (_, col) in DESTACADAS.items()}
fig, ax = plt.subplots(figsize=(16, 16))
ax.set_facecolor(FONDO)
dibujar_aristas(ax, color_dest)
resto = [s for s in orden if G.nodes[s]["comunidad"] not in DESTACADAS]
dest = [s for s in orden if G.nodes[s]["comunidad"] in DESTACADAS]
nx.draw_networkx_nodes(G, pos, nodelist=resto, ax=ax, node_size=[tam(s) for s in resto],
                       node_color="#e3e3e3", linewidths=0)
nx.draw_networkx_nodes(G, pos, nodelist=dest, ax=ax, node_size=[tam(s, 18) for s in dest],
                       node_color=[color_dest[G.nodes[s]["comunidad"]] for s in dest],
                       linewidths=0.4, edgecolors="white")
for c in DESTACADAS:
    miembros = [s for s in dest if G.nodes[s]["comunidad"] == c]
    etiquetar(ax, sorted(miembros, key=lambda s: -pr[s])[:6], tam_fuente=9, negrita=True)
for c, (texto, col) in DESTACADAS.items():
    ax.scatter([], [], s=90, color=col, label=texto)
ax.scatter([], [], s=90, color="#e3e3e3", label="resto de Reddit")
ax.legend(loc="upper left", bbox_to_anchor=(1.0, 0.95), fontsize=12, frameon=False)
ax.set_title("Seis comunidades destacadas en el mapa de Reddit (oct. 2012)",
             fontsize=15)
ax.axis("off")
guardar(fig, "mapa_destacado.png")


# grafo de comunidades
C = nx.Graph()
for r in com.itertuples(index=False):
    # Aislamiento = exceso por autores sobre lo esperado (frac_knn es alta por construcción de LPA)
    C.add_node(r.Id, label=r.Label, n=r.n_subreddits,
               exceso=r.frac_autores - r.esperada_autores)
for r in enl.itertuples(index=False):
    C.add_edge(r.Source, r.Target, weight=float(r.Weight))

print("Calculando posiciones del mapa de comunidades...")
posc = nx.forceatlas2_layout(C, max_iter=500, scaling_ratio=3.0, gravity=2.0,
                             weight="weight", seed=SEED)

# Se dibujan solo los 3 enlaces más fuertes de cada comunidad, para que se lea.
fuertes = set()
for c in C.nodes():
    vecinos = sorted(C[c].items(), key=lambda kv: -kv[1]["weight"])[:3]
    for v, _ in vecinos:
        fuertes.add(tuple(sorted((c, v))))
pmax = max(C.edges[e]["weight"] for e in fuertes)

fig, ax = plt.subplots(figsize=(16, 13))
ax.set_facecolor(FONDO)
for u, v in fuertes:
    w = C.edges[u, v]["weight"]
    ax.plot([posc[u][0], posc[v][0]], [posc[u][1], posc[v][1]], color="#9e9e9e",
            linewidth=0.4 + 3.5 * w / pmax, alpha=0.45, zorder=1)

cmap = LinearSegmentedColormap.from_list("aislamiento", ["#fde9d6", "#e6550d", "#7f2704"])
nodos_c = sorted(C.nodes(), key=lambda c: -C.nodes[c]["n"])
sc = ax.scatter([posc[c][0] for c in nodos_c], [posc[c][1] for c in nodos_c],
                s=[25 + 9 * C.nodes[c]["n"] for c in nodos_c],
                c=[C.nodes[c]["exceso"] for c in nodos_c], cmap=cmap, vmin=0, vmax=0.14,
                edgecolors="white", linewidths=0.8, zorder=2)
for c in nodos_c:
    nd = C.nodes[c]
    if nd["n"] >= 20 or c in DESTACADAS:
        radio = (25 + 9 * nd["n"]) ** 0.5 / 2          # radio del círculo, en puntos
        ax.annotate("%s (%d)" % (nombre(c), nd["n"]), posc[c], xytext=(radio + 2, 0),
                    textcoords="offset points", ha="left", va="center",
                    fontsize=10 if c in DESTACADAS else 8,
                    fontweight="bold" if c in DESTACADAS else "normal",
                    color="#111111", zorder=3,
                    bbox=dict(boxstyle="round,pad=0.15", fc="white", ec="none", alpha=0.65))
cb = fig.colorbar(sc, ax=ax, shrink=0.5, pad=0.01)
cb.set_label("Aislamiento: frac_autores − esperada_autores", fontsize=11)
ax.set_title("Las 118 comunidades de Reddit (oct. 2012)\n"
             "Tamaño = n.º de subreddits · color = aislamiento por autores compartidos · líneas = 3 vínculos más fuertes de cada una",
             fontsize=15)
ax.axis("off")
guardar(fig, "mapa_comunidades.png")