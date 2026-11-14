package org.mdp.hadoop.cli;

import org.apache.giraph.GiraphRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.mdp.hadoop.io.TextNullTextEdgeInputFormat;
import org.mdp.hadoop.io.VertexValueTextOutputFormat;
import org.mdp.hadoop.pr.LabelPropagationMaster;

/**
 * Lanza LabelPropagation igual que PageRankRunner lanza PageRank.
 *
 * Entrada: TSV origen \t destino con cada arista en los dos sentidos (giraph-und/ de edges.py).
 * Salida:  TSV subreddit \t etiqueta (comunidad).
 *
 * Uso: hadoop jar mdp-giraph.jar LPARunner <entrada> <salida> <workers>
 */
public class LPARunner {

	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			System.err.println("Uso: LPARunner <entrada> <salida> <workers>");
			System.exit(1);
		}

		String input = args[0];
		String output = args[1];
		String workers = args[2];

		// Borrar la salida si ya existe
		Configuration conf = new Configuration();
		Path outPath = new Path(output);
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}

		String[] giraphArgs = {
			LabelPropagation.class.getName(),
			"-eif", TextNullTextEdgeInputFormat.class.getName(),
			"-eip", input,
			"-vof", VertexValueTextOutputFormat.class.getName(),
			"-op", output,
			"-w", workers,
			"-ca", "mapreduce.job.tracker=yarn",
			"-ca", "mapreduce.framework.name=yarn",
			"-mc", LabelPropagationMaster.class.getName()
		};

		int res = ToolRunner.run(conf, new GiraphRunner(), giraphArgs);
		if (res != 0) {
			System.exit(res);
		}
	}
}
