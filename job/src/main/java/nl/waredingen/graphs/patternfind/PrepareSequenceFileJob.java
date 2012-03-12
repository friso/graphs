package nl.waredingen.graphs.patternfind;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.aggregator.Count;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Rename;
import cascading.scheme.Scheme;
import cascading.scheme.SequenceFile;
import cascading.scheme.TextDelimited;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class PrepareSequenceFileJob {
	public static int run(String inputFile, String nodesFile, String edgesFile) {
		Scheme sourceScheme = new TextDelimited(new Fields("from", "to"), ",");
		Tap source = new Hfs(sourceScheme, inputFile);
		
		Scheme nodesScheme = new SequenceFile(new Fields("id", "features"));
		Tap nodesSink = new Hfs(nodesScheme, nodesFile, SinkMode.REPLACE);

		Scheme edgesScheme = new SequenceFile(new Fields("from", "to", "features"));
		Tap edgesSink = new Hfs(edgesScheme, edgesFile, SinkMode.REPLACE);
		
		
		Pipe original = new Pipe("prepare");
		
		Pipe inCounts = new Pipe("incounts", original);
		inCounts = new GroupBy(inCounts, new Fields("to"));
		inCounts = new Every(inCounts, new Count());
		
		Pipe outCounts = new Pipe("outcounts", original);
		outCounts = new GroupBy(outCounts, new Fields("from"));
		outCounts = new Every(outCounts, new Count());
		
		Pipe joined = new CoGroup("nodes", inCounts, new Fields("to"), outCounts, new Fields("from"), new Fields("to", "outdegree", "from", "indegree"));
		joined = new Rename(joined, new Fields("to"), new Fields("id"));
		joined = new Each(joined, new SetNodeFeatures(), Fields.RESULTS);
		
		Pipe edges = new Pipe("edges", original);
		edges = new Each(edges, new SetEdgeFeatures(), Fields.RESULTS);

		
		Map<String, Tap> sinksMap = new HashMap<String, Tap>(2);
		sinksMap.put("nodes", nodesSink);
		sinksMap.put("edges", edgesSink);
		
		Properties properties = new Properties();
		FlowConnector.setApplicationJarClass(properties, PrepareSequenceFileJob.class);
		
		FlowConnector flowConnector = new FlowConnector(properties);
		Flow flow = flowConnector.connect(source, sinksMap, edges, joined);
		
		flow.writeDOT("flow.dot");
		
		flow.complete();
		
		return 0;

	}
	
	@SuppressWarnings({ "serial", "rawtypes" })
	public static class SetEdgeFeatures extends BaseOperation implements Function {
		public SetEdgeFeatures() {
			super(new Fields("from", "to", "features"));
		}
		
		@Override
		public void operate(FlowProcess flow, FunctionCall call) {
			TupleEntry entry = call.getArguments();
			
			MapWritable features = new MapWritable();
			features.put(new Text("random"), new IntWritable((int) (Math.random() * 10)));
			
			Tuple result = new Tuple(entry.get("from"), entry.get("to"), features);
			call.getOutputCollector().add(result);
		}
	}
	
	@SuppressWarnings({ "serial", "rawtypes" })
	private static class SetNodeFeatures extends BaseOperation implements Function {
		public SetNodeFeatures() {
			super(new Fields("id", "features"));
		}
		
		@Override
		public void operate(FlowProcess flow, FunctionCall call) {
			TupleEntry entry = call.getArguments();
			
			MapWritable features = new MapWritable();
			features.put(new Text("indegree"), new LongWritable(Long.parseLong(entry.getString("indegree"))));
			features.put(new Text("outdegree"), new LongWritable(Long.parseLong(entry.getString("outdegree"))));
			features.put(new Text("degree"), new LongWritable(Long.parseLong(entry.getString("outdegree")) + Long.parseLong(entry.getString("indegree"))));
			features.put(new Text("random"), new IntWritable((int) (Math.random() * 10)));
			
			Tuple result = new Tuple(entry.get("id"), features);
			call.getOutputCollector().add(result);
		}
	}
}
