package nl.waredingen.graphs;

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
		} else if (args[0].equalsIgnoreCase("count")) {
			return CountJob.run(args[1], args[2]);
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
