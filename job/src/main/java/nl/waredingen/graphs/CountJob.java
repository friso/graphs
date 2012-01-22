package nl.waredingen.graphs;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import nl.waredingen.graphs.CountJob.ToAdjacencyList.Context;

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

public class CountJob {
	public static int run(String input, String output) {
		Scheme sourceScheme = new TextDelimited(new Fields("source", "target"), ",");
		Tap source = new Hfs(sourceScheme, input);
		
		Scheme sinkScheme = new TextDelimited(new Fields("partition", "source", "list", "counts"), "\t");
		Tap sink = new Hfs(sinkScheme, output, SinkMode.REPLACE);
		
		Pipe original = new Pipe("prepare");
		Pipe counts = new Pipe("counts", original);
		counts = new GroupBy(counts, new Fields("target"));
		counts = new Every(counts, new Count());
		
		Pipe joined = new CoGroup(original, new Fields("target"), counts, new Fields("target"), new Fields("source", "target", "target1", "count"));
		joined = new Each(joined, new Identity(Integer.TYPE, Integer.TYPE, Integer.TYPE, Long.TYPE));
		joined = new GroupBy(joined, new Fields("source"), new Fields("target"), true);
		joined = new Every(joined, new ToAdjacencyList(), Fields.RESULTS);
		
		Properties properties = new Properties();
		FlowConnector.setApplicationJarClass(properties, PrepareJob.class);
		
		FlowConnector flowConnector = new FlowConnector(properties);
		Flow flow = flowConnector.connect(source, sink, joined);
		
		flow.writeDOT("flow.dot");
		
		flow.complete();
		
		return 0;
	}
	
	@SuppressWarnings("serial")
	public static class ToAdjacencyList extends BaseOperation<Context> implements Aggregator<Context> {
		public ToAdjacencyList() {
			super(new Fields("partition", "source", "list", "counts"));
		}
		
		public static class Context {
			int source;
			int partition = -1;
			List<Integer> targets = new ArrayList<Integer>();
			List<Long> counts = new ArrayList<Long>();
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
			int target = arguments.getInteger("target");
			if (context.partition == -1) {
				context.partition = target > context.source ? target : context.source;
			}
			context.targets.add(target);
			
			long count = arguments.getLong("count");
			context.counts.add(count);
		}

		@Override
		public void complete(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
			Context context = aggregatorCall.getContext();
			Tuple result = new Tuple(context.partition, context.source, StringUtils.joinObjects(",", context.targets), StringUtils.joinObjects(",", context.counts));
			aggregatorCall.getOutputCollector().add(result);
		}
	}
}
