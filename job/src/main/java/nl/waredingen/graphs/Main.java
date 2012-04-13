package nl.waredingen.graphs;

import java.util.Arrays;

import nl.waredingen.graphs.bgp.PrepareBgpGraphJob;
import nl.waredingen.graphs.importer.Neo4jImportJob;
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
			return Neo4jImportJob.run(args[1], args[2], args[3], args.length > 4 ? Arrays.copyOfRange(args, 4, args.length) : new String[0]);
		} else if (args[0].equalsIgnoreCase("prepare-bgp")) {
			return PrepareBgpGraphJob.runJob(args[1], args[2], args[3], args.length > 4 && "true".equals(args[4]));
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
