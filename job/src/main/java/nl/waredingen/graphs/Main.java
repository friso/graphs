package nl.waredingen.graphs;

import nl.waredingen.graphs.bgp.PrepareBgpGraphJob;
import nl.waredingen.graphs.importer.Neo4jImportJob;
import nl.waredingen.graphs.misc.RowNumberJob;
import nl.waredingen.graphs.neo.NeoGraphEdgesJob;
import nl.waredingen.graphs.neo.NeoGraphNodesJob;
import nl.waredingen.graphs.partition.IterateJob;
import nl.waredingen.graphs.partition.IterateWithFlagsJob;
import nl.waredingen.graphs.partition.PrepareJob;
import nl.waredingen.graphs.partition.PrepareWithFlagsJob;
import nl.waredingen.graphs.patternfind.PrepareSequenceFileJob;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args[0].equalsIgnoreCase("prepare")) {
			return PrepareJob.run(args[1], args[2]);
		} else if (args[0].equalsIgnoreCase("iterate")) {
			return IterateJob.run(args[1], args[2], Integer.MAX_VALUE);
		} else if (args[0].equalsIgnoreCase("iterate-once")) {
			return IterateJob.run(args[1], args[2], 1);
		} else if (args[0].equalsIgnoreCase("prepare-with-flags")) {
			return PrepareWithFlagsJob.run(args[1], args[2], Integer.parseInt(args[3]));
		} else if (args[0].equalsIgnoreCase("iterate-with-flags")) {
			return IterateWithFlagsJob.run(args[1], args[2], Integer.MAX_VALUE);
		} else if (args[0].equalsIgnoreCase("iterate-once-with-flags")) {
			return IterateWithFlagsJob.run(args[1], args[2], 1);
		} else if (args[0].equalsIgnoreCase("prepare-sequence-file")) {
			return PrepareSequenceFileJob.run(args[1], args[2], args[3]);
		} else if (args[0].equalsIgnoreCase("neo4j-import")) {
			String[] nodeFields = args.length > 3 && !"".equals(args[4].trim()) ? args[4].split(",") : new String[0];
			String[] edgeFields = args.length > 4 && !"".equals(args[5].trim()) ? args[5].split(",") : new String[0];
			return Neo4jImportJob.run(args[1], args[2], args[3], nodeFields, edgeFields);
		} else if (args[0].equalsIgnoreCase("prepare-bgp")) {
			return PrepareBgpGraphJob.runJob(args[1], args[2], args[3]);
		} else if (args[0].equalsIgnoreCase("rownumbers")) {
			return RowNumberJob.run(args[1], args[2], getConf());
		} else if (args[0].equalsIgnoreCase("neographnodes")) {
			return NeoGraphNodesJob.runJob(args[1], args[2], args[3]);
		} else if (args[0].equalsIgnoreCase("neographedges")) {
			return NeoGraphEdgesJob.runJob(args[1], args[2], args[3]);
		} else {
			System.err.println("Wrong arguments!");
			System.exit(1);
			return -1;
		}
	}
	
	public static void main(String args[]) throws Exception {
		ToolRunner.run(new Main(), args);
	}
}
