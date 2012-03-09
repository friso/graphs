package nl.waredingen.graphs.partition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.Scheme;
import cascading.scheme.TextDelimited;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class IterateWithFlagsJob {
	private static final Log log = LogFactory.getLog(IterateWithFlagsJob.class);
	
	public static int run(String input, String output, int maxIterations) {
		log.info("Starting iterative graph partitioning.");
		
		boolean done = false;
		int iterationCount = 0;
		while (!done) {
			log.info("Running iteration " + iterationCount + " of graph partitioning.");
			
			String currentIterationInputPath = iterationCount == 0 ? input :  output + ".iteration." + Integer.toString((iterationCount - 1) % 2);
			Scheme sourceScheme = new TextDelimited(new Fields("partition", "source", "list", "flags"), "\t");
			Tap source = new Hfs(sourceScheme, currentIterationInputPath);
			
			Scheme sinkScheme = new TextDelimited(new Fields("partition", "source", "list", "flags"), "\t");
			String currentIterationOutputPath = output + ".iteration." + Integer.toString(iterationCount % 2);
			Tap sink = new Hfs(sinkScheme, currentIterationOutputPath, SinkMode.REPLACE);
			
			Pipe iteration = new Pipe("iteration");
			iteration = new Each(iteration, new FanOut());
			iteration = new GroupBy(iteration, new Fields("node"), new Fields("flag", "partition"), true);
			iteration = new Every(iteration, new MaxPartitionToTuples(), Fields.RESULTS);
			
			iteration = new GroupBy(iteration, new Fields("source"), new Fields("flag", "partition"), true);
			iteration = new Every(iteration, new MaxPartitionToAdjacencyList(), Fields.RESULTS);
			
			Properties properties = new Properties();
			FlowConnector.setApplicationJarClass(properties, PrepareJob.class);
			
			FlowConnector flowConnector = new FlowConnector(properties);
			Flow flow = flowConnector.connect("iteration", source, sink, iteration);
			
			flow.writeDOT("flow.dot");
			
			flow.complete();
			
			long updatedPartitions = flow.getFlowStats().getCounterValue(MaxPartitionToAdjacencyList.COUNTER_GROUP, MaxPartitionToAdjacencyList.PARTITIONS_UPDATES_COUNTER_NAME);
			done = updatedPartitions == 0 || iterationCount == maxIterations - 1;
			
			log.info("Updated " + updatedPartitions + " during iteration " + iterationCount + ". " + (done ? "Done." : "Doing another iteration."));
			
			if (done) {
				FileSystem fs = null;
				try {
					log.info("Moving result into place and deleting intermediate iteration result.");
					fs = FileSystem.get(flow.getJobConf());
					fs.rename(new Path(currentIterationOutputPath), new Path(output));
				} catch (IOException e) {
					log.fatal("Could not move result into place. Error occured.", e);
					return 1;
				} finally {
					if (fs != null && iterationCount > 0) {
						try {
							fs.delete(new Path(currentIterationInputPath), true);
						} catch (IOException e) {
							log.warn("Could delete intermediate iteration result. Error occured.", e);
						}
					}
				}
			}
			
			iterationCount++;
		}
		
		return 0;
	}
	
	private static class MaxPartitionToAdjacencyListContext {
		int source;
		int partition = -1;
		List<Integer> targets;
		List<Byte> flags;
		
		public MaxPartitionToAdjacencyListContext() {
			this.targets = new ArrayList<Integer>();
			this.flags = new ArrayList<Byte>();
		}
	}
	
	@SuppressWarnings("serial")
	private static class MaxPartitionToAdjacencyList extends BaseOperation<MaxPartitionToAdjacencyListContext> implements Aggregator<MaxPartitionToAdjacencyListContext> {
		public static final String PARTITIONS_UPDATES_COUNTER_NAME = "Partitions updates";
		public static final String COUNTER_GROUP = "graphs";

		public MaxPartitionToAdjacencyList() {
			super(new Fields("partition", "source", "list", "flags"));
		}
		
		@Override
		public void start(FlowProcess flowProcess, AggregatorCall<MaxPartitionToAdjacencyListContext> aggregatorCall) {
			MaxPartitionToAdjacencyListContext context = new MaxPartitionToAdjacencyListContext();
			context.source = aggregatorCall.getGroup().getInteger("source");
			aggregatorCall.setContext(context);
		}

		@Override
		public void aggregate(FlowProcess flowProcess, AggregatorCall<MaxPartitionToAdjacencyListContext> aggregatorCall) {
			MaxPartitionToAdjacencyListContext context = aggregatorCall.getContext();
			TupleEntry arguments = aggregatorCall.getArguments();
			
			int node = arguments.getInteger("node");
			int partition = arguments.getInteger("partition");
			boolean flag = arguments.getBoolean("flag");
			
			if (context.partition == -1) {
				context.partition = partition;
			} else {
				if (flag && context.partition > partition) {
					flowProcess.increment(COUNTER_GROUP, PARTITIONS_UPDATES_COUNTER_NAME, 1);
				}
			}
			
			if (node != context.source) {
				context.targets.add(node);
				context.flags.add((byte) (flag ? 1 : 0));
			}
		}

		@Override
		public void complete(FlowProcess flowProcess, AggregatorCall<MaxPartitionToAdjacencyListContext> aggregatorCall) {
			MaxPartitionToAdjacencyListContext context = aggregatorCall.getContext();
			Tuple result = new Tuple(context.partition, context.source, StringUtils.joinObjects(",", context.targets), StringUtils.joinObjects(",", context.flags));
			aggregatorCall.getOutputCollector().add(result);
		}
	}
	
	@SuppressWarnings({ "serial", "rawtypes", "unchecked" })
	private  static class MaxPartitionToTuples extends BaseOperation implements Buffer {
		public MaxPartitionToTuples() {
			super(new Fields("partition", "node", "source", "flag"));
		}
		
		@Override
		public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
			Iterator<TupleEntry> itr = bufferCall.getArgumentsIterator();
			
			int maxPartition;
			TupleEntry entry = itr.next();
			maxPartition = entry.getInteger("partition");
			
			emitTuple(bufferCall, maxPartition, entry);
			
			while (itr.hasNext()) {
				entry = itr.next();
				emitTuple(bufferCall, maxPartition, entry);
			}
		}

		private void emitTuple(BufferCall bufferCall, int maxPartition, TupleEntry entry) {
			Tuple result = new Tuple(maxPartition, entry.getInteger("node"), entry.getInteger("source"), entry.getBoolean("flag"));
			bufferCall.getOutputCollector().add(result);
		}
	}
	
	@SuppressWarnings({ "serial", "rawtypes" })
	private static class FanOut extends BaseOperation implements Function {
		public FanOut() {
			super(new Fields("partition", "node", "source", "flag"));
		}
		
		@Override
		public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
			TupleEntry args = functionCall.getArguments();
			int partition = args.getInteger("partition");
			int source = args.getInteger("source");

			Tuple result = new Tuple(partition, source, source, true);
			functionCall.getOutputCollector().add(result);
			
			String[] nodeList = args.getString("list").split(",");
			String[] flagList = args.getString("flags").split(",");
			for (int c = 0; c < nodeList.length; c++) {
				result = new Tuple(partition, Integer.parseInt(nodeList[c]), source, Integer.parseInt(flagList[c]) != 0);
				functionCall.getOutputCollector().add(result);
			}
		}
	}
}
