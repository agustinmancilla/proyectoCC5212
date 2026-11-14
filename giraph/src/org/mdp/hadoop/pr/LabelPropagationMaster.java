package org.mdp.hadoop.pr;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.LongWritable;
import org.mdp.hadoop.cli.LabelPropagation;

/**
 * Master del LPA: registra el agregador de cambios, imprime cuántos vértices
 * cambiaron en cada superstep (queda en el log del job, sirve para el informe)
 * y detiene el cómputo cuando ya nadie cambia.
 */
public class LabelPropagationMaster extends DefaultMasterCompute {

	@Override
	public void initialize() throws InstantiationException, IllegalAccessException {
		registerAggregator(LabelPropagation.CAMBIOS, LongSumAggregator.class);
	}

	@Override
	public void compute() {
		long s = getSuperstep();
		// En el superstep s se lee lo que agregaron los vértices en el superstep s-1.
		// En el superstep 1 nadie compara todavía (el 0 solo inicializa), por eso se parte en 2.
		if (s >= 2) {
			long cambios = ((LongWritable) getAggregatedValue(LabelPropagation.CAMBIOS)).get();
			System.out.println("[LPA] superstep " + (s - 1) + ": " + cambios
					+ " de " + getTotalNumVertices() + " vertices cambiaron de etiqueta");
			if (cambios == 0) {
				System.out.println("[LPA] convergio en el superstep " + (s - 1));
				haltComputation();
			}
		}
	}
}
