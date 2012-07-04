package nl.waredingen.graphs.neo;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.aggregator.First;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.cogroup.InnerJoin;
import cascading.scheme.Scheme;
import cascading.scheme.TextDelimited;
import cascading.tap.GlobHfs;
import cascading.tap.Hfs;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class NeoGraphNodesJob {

	public static final long NUMBER_OF_PROPERTIES_PER_NODE = 1L;
	private static final Log LOG = LogFactory.getLog(NeoGraphNodesJob.class);

	public static int runJob(String nodesFile, String edgesFile, String output) {
		// TODO properties store is yet to be done
		// TODO see if this job can be simplified and combined into one node creation job with rownumbering on the spot
		// TODO see if combining this with edges job is possible
		Scheme nodesScheme = new TextDelimited(new Fields("id", "name", "rownum"), "\t");
		Tap nodeSource = new GlobHfs(nodesScheme, nodesFile);

		Scheme edgesScheme = new TextDelimited(new Fields("from", "to", "rownum"), "\t");
		Tap edgeSource = new GlobHfs(edgesScheme, edgesFile);

		Map<String, Tap> sourceMap = new HashMap<String, Tap>(2);
		sourceMap.put("nodes", nodeSource);
		sourceMap.put("edges", edgeSource);

		Scheme graphNodesScheme = new ByteBufferScheme();
		Tap nodesSink = new Hfs(graphNodesScheme, output + "/neostore.nodestore.db", SinkMode.REPLACE);

		Map<String, Tap> sinkMap = new HashMap<String, Tap>(1);
		sinkMap.put("graphnodes", nodesSink);

		Pipe nodesPipe = new Pipe("nodes");
		Pipe edgesPipe = new Pipe("edges");

		Pipe fromjoin = new CoGroup("fromjoin", nodesPipe, new Fields("id"), edgesPipe, new Fields("from"), new Fields(
				"id", "name", "rownum", "from", "to", "relnum"), new InnerJoin());
		Pipe tojoin = new CoGroup("tojoin", nodesPipe, new Fields("id"), edgesPipe, new Fields("to"), new Fields("id",
				"name", "rownum", "from", "to", "relnum"), new InnerJoin());

		Pipe graphNodesJoinPipe = new GroupBy(Pipe.pipes(fromjoin, tojoin), new Fields("id"), new Fields("relnum"),
				true);

		Pipe graphNodesPipe = new Every(graphNodesJoinPipe, new First(), Fields.RESULTS);
		graphNodesPipe = new GroupBy("graphnodes", graphNodesPipe, new Fields("rownum"));
		graphNodesPipe = new Each(graphNodesPipe, new NodeRecordCreator(), Fields.RESULTS);

		Properties properties = new Properties();
		FlowConnector.setApplicationJarClass(properties, NeoGraphNodesJob.class);

		FlowConnector flowConnector = new FlowConnector(properties);
		Flow flow = flowConnector.connect(sourceMap, sinkMap, graphNodesPipe);
		flow.writeDOT("flow.dot");

		flow.complete();

		return 0;
	}

	@SuppressWarnings({ "serial", "rawtypes" })
	private static class NodeRecordCreator extends BaseOperation implements Function {
		public NodeRecordCreator() {
			super(new Fields("node"));
		}

		@Override
		public void operate(FlowProcess flow, FunctionCall call) {
			TupleEntry arguments = call.getArguments();
			long relnum = arguments.getLong("relnum");
			long id = arguments.getLong("rownum");
			if (id == 0L) {
				call.getOutputCollector().add(
						new Tuple(getNodeAsBuffer(id, Record.NO_NEXT_RELATIONSHIP.intValue(),
								Record.NO_NEXT_PROPERTY.intValue())));
			}
			call.getOutputCollector().add(
					new Tuple(getNodeAsBuffer(id + 1L, relnum, id * NUMBER_OF_PROPERTIES_PER_NODE)));

		}

		private Buffer getNodeAsBuffer(long id, long relnum, long prop) {
			ByteBuffer buffer = ByteBuffer.allocate(9);

			NodeRecord nr = new NodeRecord(id, relnum, prop);
			nr.setInUse(true);
			nr.setCreated();

//			LOG.debug(nr.toString());

			long nextRel = nr.getNextRel();
			long nextProp = nr.getNextProp();

			short relModifier = nextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0
					: (short) ((nextRel & 0x700000000L) >> 31);
			short propModifier = nextProp == Record.NO_NEXT_PROPERTY.intValue() ? 0
					: (short) ((nextProp & 0xF00000000L) >> 28);

			// [ , x] in use bit
			// [ ,xxx ] higher bits for rel id
			// [xxxx, ] higher bits for prop id
			short inUseUnsignedByte = (nr.inUse() ? Record.IN_USE : Record.NOT_IN_USE).byteValue();
			inUseUnsignedByte = (short) (inUseUnsignedByte | relModifier | propModifier);

			buffer.put((byte) inUseUnsignedByte).putInt((int) nextRel).putInt((int) nextProp);
			return buffer;

		}
	}
}
