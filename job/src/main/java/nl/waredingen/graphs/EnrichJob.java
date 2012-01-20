package nl.waredingen.graphs;

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.aggregator.Count;
import cascading.pipe.CoGroup;
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

public class EnrichJob {

	public static int run(String input, String output) {
		Scheme sourceScheme = new TextDelimited(new Fields("source", "target", "partition"), ",");
		Tap source = new Hfs(sourceScheme, input);
		
		Scheme sinkScheme = new TextDelimited(new Fields("partition", "source", "target", "originalSource", "count"), ",");
		Tap sink = new Hfs(sinkScheme, output, SinkMode.REPLACE);
		
		Pipe originalSet = new Pipe("originalSet");
		
		Pipe targetCounts = new Pipe("targetCounts", originalSet);
		targetCounts = new GroupBy(targetCounts, new Fields("target"));
		
		Aggregator count = new Count(new Fields("count"));
		targetCounts = new Every(targetCounts, count);
		
		
		Pipe joined = new CoGroup(originalSet, new Fields("target"), targetCounts, new Fields("target"), new Fields("source", "target", "partition", "target1", "count"));
		joined = new Each(joined, new AddPartitionAndOriginalSource(1000));
		
		Properties properties = new Properties();
		FlowConnector.setApplicationJarClass(properties, EnrichJob.class);
		
		FlowConnector flowConnector = new FlowConnector(properties);
		Flow flow = flowConnector.connect("originalSet", source, sink, joined);
		
		flow.writeDOT("flow.dot");
		
		flow.complete();
		
		return 0;
	}
	
	@SuppressWarnings("serial")
	public static class AddPartitionAndOriginalSource extends BaseOperation implements Function {
		private final Integer threshold;
		
		public AddPartitionAndOriginalSource(int threshold) {
			super(4, new Fields("source", "target", "count", "partition", "originalSource"));
			this.threshold = threshold;
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
			TupleEntry args = functionCall.getArguments();
			Comparable count = args.getInteger("count");
			Comparable source = args.getInteger("source");
			Comparable target = args.getInteger("target");
			Comparable partition = args.getInteger("partition");
			Comparable largest = source.compareTo(target) > 0 ? source : target;
			
			Tuple result = new Tuple(
					source, 
					target, 
					count, 
					count.compareTo(threshold) > 0 ? partition.compareTo(source) > 0 ? partition : source : partition.compareTo(largest) > 0 ? partition : largest,
					source);
			
			functionCall.getOutputCollector().add(result);
		}
	}
}
