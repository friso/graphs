package nl.waredingen.graphs.bgp;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import nl.waredingen.graphs.partition.PrepareJob;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.aggregator.Sum;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Unique;
import cascading.scheme.Scheme;
import cascading.scheme.TextDelimited;
import cascading.scheme.TextLine.Compress;
import cascading.tap.GlobHfs;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class PrepareBgpGraphJob {
	public static int runJob(String inputFiles, String nodesFile, String edgesFile, boolean withCounts) {
		//From: http://www.routeviews.org/data.html
		//BGP protocol|unix time in seconds|Withdraw or Announce|PeerIP|PeerAS|Prefix|AS_PATH|Origin|Next_Hop|Local_Pref|MED|Community|AtomicAGG|AGGREGATOR|
		Scheme sourceScheme = new TextDelimited(new Fields(
				"proto", 
				"time", 
				"type", 
				"peerip", 
				"peeras", 
				"prefix", 
				"path", 
				"origin", 
				"nexthop", 
				"localpref", 
				"MED", 
				"community", 
				"AAGG", 
				"aggregator"), 
				Compress.DISABLE, 
				false, 
				"|", 
				false, 
				"", 
				new Class[] { String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class }, 
				true);
		
		Tap source = new GlobHfs(sourceScheme, inputFiles);
		
		Scheme nodesScheme = new TextDelimited(new Fields("id", "name"), "\t");
		Tap nodesSink = new Hfs(nodesScheme, nodesFile, SinkMode.REPLACE);
		
		Scheme edgesScheme = withCounts ? new TextDelimited(new Fields("from", "to", "updatecount"), "\t") : new TextDelimited(new Fields("from", "to"), ",");
		Tap edgesSink = new Hfs(edgesScheme, edgesFile, SinkMode.REPLACE);
		
		Pipe original = new Pipe("original");
		
		Pipe nodes = new Pipe("nodes", original);
		nodes = new Each(nodes, new PathToNodes());
		nodes = new Unique(nodes, new Fields("id"));
		
		Pipe edges = new Pipe("edges", original);
		edges = new Each(edges, new PathToEdges(), Fields.RESULTS);
		if (withCounts) {
			edges = new GroupBy(edges, new Fields("from", "to"));
			edges = new Every(edges, new Fields("updatecount"), new Sum(new Fields("updatecount"), Integer.TYPE));
		} else {
			edges = new Unique(edges, new Fields("from", "to"));
		}
		
		Map<String,Tap> sinkMap = new HashMap<String,Tap>();
		sinkMap.put("nodes", nodesSink);
		sinkMap.put("edges", edgesSink);
		
		Properties properties = new Properties();
		FlowConnector.setApplicationJarClass(properties, PrepareJob.class);
		
		FlowConnector flowConnector = new FlowConnector(properties);
		Flow flow = flowConnector.connect(source, sinkMap, nodes, edges);
		
		flow.writeDOT("flow.dot");
		
		flow.complete();

		return 0;
	}
	
	@SuppressWarnings({ "serial", "rawtypes" })
	private static class PathToNodes extends BaseOperation implements Function {
		public PathToNodes() {
			super(new Fields("id", "name"));
		}
		
		@Override
		public void operate(FlowProcess flow, FunctionCall call) {
			TupleEntry arguments = call.getArguments();
			String path = arguments.getString("path");
			if (path != null && !path.contains("{")) {
				String[] numbers = path.split(" ");
				for (String as : numbers) {
					call.getOutputCollector().add(new Tuple(as, "AS" + as));
				}
			}
		}
	}
	
	@SuppressWarnings({ "serial", "rawtypes" })
	private static class PathToEdges extends BaseOperation implements Function {
		public PathToEdges() {
			super(new Fields("from", "to", "updatecount"));
		}
		
		@Override
		public void operate(FlowProcess flow, FunctionCall call) {
			TupleEntry arguments = call.getArguments();
			String path = arguments.getString("path");
			if (path != null && !path.contains("{")) {
				String[] numbers = path.split(" ");
				for (int c = 0; c < numbers.length - 1; c++) {
					String proto = arguments.getString("proto");
					
					call.getOutputCollector().add(new Tuple(numbers[c], numbers[c + 1], "BGP4MP".equals(proto) ? 1 : 0));
				}
			}
		}
	}
}
