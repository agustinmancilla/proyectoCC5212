package org.mdp.hadoop.cli;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 * Label Propagation (LPA) sincrónico para detectar comunidades de subreddits.
 *
 * Text: id del vértice (nombre del subreddit)
 * Text: valor del vértice (etiqueta = nombre del subreddit que "lidera" la comunidad)
 * NullWritable: las aristas no tienen peso (grafo k-NN no dirigido)
 * Text: mensajes (la etiqueta actual de cada vecino)
 *
 * Superstep 0: cada vértice toma su propio id como etiqueta y la envía a sus vecinos.
 * Superstep 1 en adelante: adopta la etiqueta más frecuente entre la suya y las recibidas.
 *   - La etiqueta propia también cuenta: así se evita la mayoría de las oscilaciones
 *     típicas del LPA sincrónico (dos grupos que intercambian etiquetas para siempre).
 *   - En empate gana la etiqueta menor (orden lexicográfico), así el resultado es
 *     determinista para una misma entrada.
 *
 * Se detiene cuando ningún vértice cambia de etiqueta en un superstep (lo decide
 * LabelPropagationMaster con el agregador CAMBIOS) o al llegar a MAX_ITERS.
 *
 * La entrada TIENE que traer cada arista en los dos sentidos (giraph-und/): Giraph solo
 * manda mensajes por las aristas salientes de cada vértice.
 */
public class LabelPropagation extends BasicComputation<Text, Text, NullWritable, Text> {

	/** Tope de supersteps, por si el LPA sincrónico oscila. */
	public static final int MAX_ITERS = 20;

	/** Agregador: cuántos vértices cambiaron de etiqueta en el superstep anterior. */
	public static final String CAMBIOS = "cambios";

	private static final LongWritable UNO = new LongWritable(1);

	@Override
	public void compute(Vertex<Text, Text, NullWritable> vertex, Iterable<Text> messages) throws IOException {
		if (getSuperstep() == 0) {
			// etiqueta inicial = su propio nombre
			vertex.setValue(new Text(vertex.getId()));
		} else {
			String actual = vertex.getValue().toString();

			Map<String, Integer> cuenta = new HashMap<String, Integer>();
			cuenta.put(actual, 1); // la etiqueta propia cuenta como un voto
			for (Text m : messages) {
				String et = m.toString();
				Integer c = cuenta.get(et);
				cuenta.put(et, c == null ? 1 : c + 1);
			}

			String mejor = null;
			int max = -1;
			for (Map.Entry<String, Integer> e : cuenta.entrySet()) {
				int c = e.getValue();
				if (c > max || (c == max && e.getKey().compareTo(mejor) < 0)) {
					mejor = e.getKey();
					max = c;
				}
			}

			if (!mejor.equals(actual)) {
				vertex.setValue(new Text(mejor));
				aggregate(CAMBIOS, UNO);
			}
		}

		if (getSuperstep() < MAX_ITERS) {
			sendMessageToAllEdges(vertex, vertex.getValue());
		} else {
			vertex.voteToHalt();
		}
	}
}
