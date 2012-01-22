package nl.waredingen.graphs;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import nl.waredingen.graphs.IterateJob.MaxPartitionToAdjacencyList.Context;

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

public class IterateJob {
	public static int run(String input, String output) {
		Scheme sourceScheme = new TextDelimited(new Fields("partition", "source", "list"), "\t");
		Tap source = new Hfs(sourceScheme, input);
		
		Scheme sinkScheme = new TextDelimited(new Fields("partition", "source", "list"), "\t");
		Tap sink = new Hfs(sinkScheme, output, SinkMode.REPLACE);
		
		Pipe iteration = new Pipe("iteration");
		iteration = new Each(iteration, new FanOut());
		iteration = new GroupBy(iteration, new Fields("node"), new Fields("partition"), true);
		iteration = new Every(iteration, new MaxPartitionToTuples(), Fields.RESULTS);
		
		iteration = new GroupBy(iteration, new Fields("source"), new Fields("partition"), true);
		iteration = new Every(iteration, new MaxPartitionToAdjacencyList(), Fields.RESULTS);
		
		Properties properties = new Properties();
		FlowConnector.setApplicationJarClass(properties, PrepareJob.class);
		
		FlowConnector flowConnector = new FlowConnector(properties);
		Flow flow = flowConnector.connect("iteration", source, sink, iteration);
		
		flow.writeDOT("flow.dot");
		
		flow.complete();
		
		return 0;
	}
	
	@SuppressWarnings("serial")
	public static class MaxPartitionToAdjacencyList extends BaseOperation<Context> implements Aggregator<Context> {
		public MaxPartitionToAdjacencyList() {
			super(new Fields("partition", "source", "list"));
		}
		
		public static class Context {
			int source;
			int partition = -1;
			List<Integer> targets;
			
			public Context() {
				this.targets = new ArrayList<Integer>();
			}
		}
		
		@Override
		public void start(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
			Context context = new Context();
			context.source = aggregatorCall.getGroup().getInteger("source");
			aggregatorCall.setContext(context);
		}

		@Override
		public void aggregate(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
			Context context = aggregatorCall.getContext();
			TupleEntry arguments = aggregatorCall.getArguments();
			if (context.partition == -1) {
				context.partition = arguments.getInteger("partition");
			}
			
			int node = arguments.getInteger("node");
			if (node != context.source) {
				context.targets.add(node);
			}
		}

		@Override
		public void complete(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
			Context context = aggregatorCall.getContext();
			Tuple result = new Tuple(context.partition, context.source, StringUtils.joinObjects(",", context.targets));
			aggregatorCall.getOutputCollector().add(result);
		}
	}
	
	@SuppressWarnings({ "serial", "rawtypes", "unchecked" })
	public static class MaxPartitionToTuples extends BaseOperation implements Buffer {
		public MaxPartitionToTuples() {
			super(new Fields("partition", "node", "source"));
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
			Tuple result = new Tuple(maxPartition, entry.getInteger("node"), entry.getInteger("source"));
			bufferCall.getOutputCollector().add(result);
		}
	}
	
	@SuppressWarnings({ "serial", "rawtypes" })
	public static class FanOut extends BaseOperation implements Function {
		public FanOut() {
			super(3, new Fields("partition", "node", "source"));
		}
		
		@Override
		public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
			TupleEntry args = functionCall.getArguments();
			int partition = args.getInteger("partition");
			int source = args.getInteger("source");

			Tuple result = new Tuple(partition, source, source);
			functionCall.getOutputCollector().add(result);
			
			for (String node : args.getString("list").split(",")) {
				result = new Tuple(partition, Integer.parseInt(node), source);
				functionCall.getOutputCollector().add(result);
			}
		}
	}
}
