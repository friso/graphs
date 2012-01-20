package nl.waredingen.graphs;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
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
	public static int run(int iterationNumber, String input, String output) {
		Scheme sourceScheme = new TextDelimited(new Fields("partition", "source", "target", "originalSource", "count"), ",");
		Tap source = new Hfs(sourceScheme, input);
		
		Scheme sinkScheme = new TextDelimited(new Fields("partition", "source", "target", "originalSource", "count"), ",");
		Tap sink = new Hfs(sinkScheme, output, SinkMode.REPLACE);
		
		Pipe iteration = new Pipe("iteration");
		iteration = new Each(iteration, new SelectPartition(1000));
//		iteration = new GroupBy(iteration, iterationNumber % 2 == 0 ? new Fields("target") : new Fields("previousSource"), new Fields("partition"), true);
//		iteration = new Every(iteration, new UpdatePartition());
		
		Properties properties = new Properties();
		FlowConnector.setApplicationJarClass(properties, EnrichJob.class);
		
		FlowConnector flowConnector = new FlowConnector(properties);
		Flow flow = flowConnector.connect("iteration", source, sink, iteration);
		
		flow.writeDOT("flow.dot");
		
		flow.complete();
		
		return 0;
	}
	
	public static class UpdatePartition extends BaseOperation implements Aggregator {
		public UpdatePartition() {
			super(new Fields("partition", "source", "target", "originalSource", "count"));
		}
		
		@Override
		public void start(FlowProcess flowProcess, AggregatorCall aggregatorCall) {
		}
		
		@Override
		public void aggregate(FlowProcess flowProcess, AggregatorCall aggregatorCall) {
			TupleEntry arguments = aggregatorCall.getArguments();
			int partition = arguments.getInteger("partition");
			int source = arguments.getInteger("source");
			int target = arguments.getInteger("target");
			int originalSource = arguments.getInteger("originalSource");
			int count = arguments.getInteger("count");
			
			if (aggregatorCall.getContext() == null) {
				aggregatorCall.setContext(partition);
			}
			
			Tuple result = new Tuple(aggregatorCall.getContext(), source, target, originalSource, count);
			aggregatorCall.getOutputCollector().add(result);
		}
		
		@Override
		public void complete(FlowProcess flowProcess, AggregatorCall aggregatorCall) {
		}
	}
	
	public static class SelectPartition extends BaseOperation implements Function {
		private final Integer threshold;
		
		public SelectPartition(int threshold) {
			super(4, new Fields("partition", "source", "target", "originalSource", "count", "previousSource"));
			this.threshold = threshold;
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
			TupleEntry args = functionCall.getArguments();
			int count = args.getInteger("count");
			int source = args.getInteger("source");
			int originalSource = args.getInteger("originalSource");
			int target = args.getInteger("target");
			int partition = args.getInteger("partition");
			int largest = Math.max(source, partition);
			
			Tuple result;
			if (count > threshold) {
				result = new Tuple(partition, source, target, originalSource, count, source);
			} else {
				result = new Tuple(largest, largest, target, originalSource, count, source);
			}
			
			functionCall.getOutputCollector().add(result);
		}
	}
}
