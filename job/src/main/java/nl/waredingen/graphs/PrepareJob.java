package nl.waredingen.graphs;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.util.StringUtils;

import nl.waredingen.graphs.PrepareJob.ToAdjacencyList.Context;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.operation.Identity;
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

public class PrepareJob {

	public static int run(String input, String output) {
		Scheme sourceScheme = new TextDelimited(new Fields("source", "target"), ",");
		Tap source = new Hfs(sourceScheme, input);
		
		Scheme sinkScheme = new TextDelimited(new Fields("partition", "source", "list"), "\t");
		Tap sink = new Hfs(sinkScheme, output, SinkMode.REPLACE);
		
		Pipe prepare = new Pipe("prepare");
		prepare = new Each(prepare, new Identity(Integer.TYPE, Integer.TYPE));
		prepare = new GroupBy(prepare, new Fields("source"), new Fields("target"), true);
		prepare = new Every(prepare, new ToAdjacencyList(), Fields.RESULTS);
		
		Properties properties = new Properties();
		FlowConnector.setApplicationJarClass(properties, PrepareJob.class);
		
		FlowConnector flowConnector = new FlowConnector(properties);
		Flow flow = flowConnector.connect("originalSet", source, sink, prepare);
		
		flow.writeDOT("flow.dot");
		
		flow.complete();
		
		return 0;
	}
	
	@SuppressWarnings("serial")
	public static class ToAdjacencyList extends BaseOperation<Context> implements Aggregator<Context> {
		public ToAdjacencyList() {
			super(new Fields("partition", "source", "list"));
		}
		
		public static class Context {
			int source;
			int partition = -1;
			List<Integer> targets = new ArrayList<Integer>();
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
		}

		@Override
		public void complete(FlowProcess flowProcess, AggregatorCall<Context> aggregatorCall) {
			Context context = aggregatorCall.getContext();
			Tuple result = new Tuple(context.partition, context.source, StringUtils.joinObjects(",", context.targets));
			aggregatorCall.getOutputCollector().add(result);
		}
	}
}
