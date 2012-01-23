package nl.waredingen.graphs;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.util.StringUtils;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.Identity;
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

public class PrepareWithFlagsJob {
	public static int run(String input, String output, int incomingEdgeThreshold) {
		Scheme sourceScheme = new TextDelimited(new Fields("source", "target"), ",");
		Tap source = new Hfs(sourceScheme, input);
		
		Scheme sinkScheme = new TextDelimited(new Fields("partition", "source", "list", "flags"), "\t");
		Tap sink = new Hfs(sinkScheme, output, SinkMode.REPLACE);
		
		Pipe original = new Pipe("prepare");
		Pipe counts = new Pipe("counts", original);
		counts = new GroupBy(counts, new Fields("target"));
		counts = new Every(counts, new Count());
		
		Pipe joined = new CoGroup(original, new Fields("target"), counts, new Fields("target"), new Fields("source", "target", "target1", "count"));
		joined = new Each(joined, new Identity(Integer.TYPE, Integer.TYPE, Integer.TYPE, Long.TYPE));
		joined = new GroupBy(joined, new Fields("source"));
		joined = new Every(joined, new ToAdjacencyList(incomingEdgeThreshold), Fields.RESULTS);
		
		Properties properties = new Properties();
		FlowConnector.setApplicationJarClass(properties, PrepareWithFlagsJob.class);
		
		FlowConnector flowConnector = new FlowConnector(properties);
		Flow flow = flowConnector.connect(source, sink, joined);
		
		flow.writeDOT("flow.dot");
		
		flow.complete();
		
		return 0;
	}
	
	private static class ToAdjacencyListContext {
		int source;
		int partition = -1;
		List<Integer> targets = new ArrayList<Integer>();
		List<Byte> flags = new ArrayList<Byte>();
	}
	
	@SuppressWarnings("serial")
	private static class ToAdjacencyList extends BaseOperation<ToAdjacencyListContext> implements Aggregator<ToAdjacencyListContext> {
		private int threshold;
		
		public ToAdjacencyList(int threshold) {
			super(new Fields("partition", "source", "list", "flags"));
			this.threshold = threshold;
		}

		@Override
		public void start(FlowProcess flowProcess, AggregatorCall<ToAdjacencyListContext> aggregatorCall) {
			ToAdjacencyListContext context = new ToAdjacencyListContext();
			context.source = aggregatorCall.getGroup().getInteger("source");
			aggregatorCall.setContext(context);
		}

		@Override
		public void aggregate(FlowProcess flowProcess, AggregatorCall<ToAdjacencyListContext> aggregatorCall) {
			ToAdjacencyListContext context = aggregatorCall.getContext();
			TupleEntry arguments = aggregatorCall.getArguments();
			
			int count = arguments.getInteger("count");
			byte flag = (byte) (count > threshold ? 0 : 1);
			int target = arguments.getInteger("target");
			
			if (flag == (byte) 1 && target > context.partition) {
				context.partition = target;
			}
			
			context.targets.add(target);
			context.flags.add(flag);
		}

		@Override
		public void complete(FlowProcess flowProcess, AggregatorCall<ToAdjacencyListContext> aggregatorCall) {
			ToAdjacencyListContext context = aggregatorCall.getContext();
			if (context.source > context.partition) {
				context.partition = context.source;
			}
			
			Tuple result = new Tuple(context.partition, context.source, StringUtils.joinObjects(",", context.targets), StringUtils.joinObjects(",", context.flags));
			aggregatorCall.getOutputCollector().add(result);
		}
	}
}
