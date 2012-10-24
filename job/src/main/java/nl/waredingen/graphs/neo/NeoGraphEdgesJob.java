package nl.waredingen.graphs.neo;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.BufferCall;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Unique;
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

public class NeoGraphEdgesJob {

	public static final long NUMBER_OF_PROPERTIES_PER_NODE = 1L;
	private static final Log LOG = LogFactory.getLog(NeoGraphEdgesJob.class);

	public static int runJob(String nodesFile, String edgesFile, String output) {

		//TODO properties store is yet to be done
		// TODO see if this job can be simplified and combined into one edge creation job with rownumbering on the spot
		// TODO see if combining this with nodes job is possible

		Scheme nodesScheme = new TextDelimited(new Fields("id", "name", "rownum"), "\t");
		Tap nodeSource = new GlobHfs(nodesScheme, nodesFile);

		Scheme edgesScheme = new TextDelimited(new Fields("from", "to", "rownum"), "\t");
		Tap edgeSource = new GlobHfs(edgesScheme, edgesFile);

		Map<String, Tap> sourceMap = new HashMap<String, Tap>(2);
		sourceMap.put("nodes", nodeSource);
		sourceMap.put("edges", edgeSource);

		Scheme graphEdgesScheme = new ByteBufferScheme();
		//Scheme graphEdgesScheme = new TextDelimited(new Fields("id", "name", "rownum", "from", "to", "relnum", "fromid", "toid"), "\t");
		Tap edgesSink = new Hfs(graphEdgesScheme, output + "/neostore.relationshipstore.db", SinkMode.REPLACE);

		Map<String, Tap> sinkMap = new HashMap<String, Tap>(1);
		sinkMap.put("graphedges", edgesSink);


		Pipe nodesPipe = new Pipe("nodes");
		Pipe edgesPipe = new Pipe("edges");

		Pipe fromjoin = new CoGroup("fromjoin", nodesPipe, new Fields("id"), edgesPipe, new Fields("from"), new Fields(
				"id", "name", "rownum", "from", "to", "relnum"), new InnerJoin());
		Pipe tojoin = new CoGroup("tojoin", nodesPipe, new Fields("id"), edgesPipe, new Fields("to"), new Fields("id",
				"name", "rownum", "from", "to", "relnum"), new InnerJoin());

		Pipe graphEdgesJoinPipe = new GroupBy(Pipe.pipes(fromjoin, tojoin), new Fields("id"), new Fields("relnum"),
				true);


		graphEdgesJoinPipe = new Unique(graphEdgesJoinPipe, new Fields("id", "from", "to", "relnum"));
		// TODO Edges are now outputed with original id, but this needs to be the rownum of the node (from and to node)
		// EdgeNodeIdsToNodeRownums method is a first try to do so.
//		graphEdgesJoinPipe = new Each(graphEdgesJoinPipe, new EdgeNodeIdsToNodeRownums(), Fields.RESULTS);
//		graphEdgesJoinPipe = new CoGroup(nodesPipe, new Fields("id"), graphEdgesJoinPipe, new Fields())
		Pipe graphEdgesPipe = new GroupBy(graphEdgesJoinPipe, new Fields("id"), new Fields("relnum"), true);
		graphEdgesPipe = new Every(graphEdgesPipe, new RelationshipRownumBuffer(), Fields.RESULTS);
		
		graphEdgesPipe = new CoGroup(graphEdgesPipe, new Fields("from", "to", "relnum"), 1, new Fields("id", "from", "to", "relnum", "fromprev", "fromnext", "toid", "from2", "to2", "relnum2", "toprev", "tonext"));
		graphEdgesPipe = new Unique(graphEdgesPipe, new Fields("id", "from", "to", "relnum", "fromprev", "fromnext", "toid", "from2", "to2", "relnum2", "toprev", "tonext"));
		graphEdgesPipe = new Each(graphEdgesPipe, new Fields("id", "from", "to", "relnum", "fromprev", "fromnext", "toid", "from2", "to2", "relnum2", "toprev", "tonext"), new DuplicateSelfJoinFilter());
		graphEdgesPipe = new GroupBy("graphedges", graphEdgesPipe, new Fields("relnum"));
		graphEdgesPipe = new Each(graphEdgesPipe, new EdgeRecordCreator(), Fields.RESULTS);
		
		Properties properties = new Properties();
		FlowConnector.setApplicationJarClass(properties, NeoGraphEdgesJob.class);

		FlowConnector flowConnector = new FlowConnector(properties);
		Flow flow = flowConnector.connect(sourceMap, sinkMap, graphEdgesPipe);
		flow.writeDOT("flow.dot");

		flow.complete();

		return 0;
	}

	private static class RelationshipRownumContext {
		long id = -1L, from = -1L, to = -1L, relnum = -1L, prev = -1L, next = -1L;

		public RelationshipRownumContext() {
		}

		public Tuple asTuple() {
			return new Tuple(id, from, to, relnum, prev, next);
		}

		@Override
		public String toString() {
			return "" + id + ";" + from + ":" + to + ":" + relnum + ":" + prev + ":" + next;
		}
	}

	@SuppressWarnings({ "serial" })
	private static class RelationshipRownumBuffer extends BaseOperation<RelationshipRownumContext> implements
			cascading.operation.Buffer<RelationshipRownumContext> {
		public RelationshipRownumBuffer() {
			super(new Fields("id", "from", "to", "relnum", "prev", "next"));
		}

		@Override
		public void operate(FlowProcess flow, BufferCall<RelationshipRownumContext> call) {
			RelationshipRownumContext context = new RelationshipRownumContext();

			Iterator<TupleEntry> arguments = call.getArgumentsIterator();

			while (arguments.hasNext()) {
				
				TupleEntry entry = arguments.next();
				long id = entry.getLong("id");
				long from = entry.getLong("from");
				long to = entry.getLong("to");
				long relnum = entry.getLong("relnum");
				if (context.id == -1L) {
					// first call, so set current fields
					context.id = id;
					context.from = from;
					context.to = to;
					context.relnum = relnum;
					context.prev = -1L; // don't know yet
					context.next = -1L; // first call, relationships ordered descending, so last rel, so no next available
				} else if (context.prev == -1L) {
					// not the first so current relationship will become prev in
					// context and context can be emitted and refilled with
					// current
					context.prev = relnum;
					call.getOutputCollector().add(context.asTuple());
					long next = context.relnum;
					context.id = id;
					context.from = from;
					context.to = to;
					context.relnum = relnum;
					context.prev = -1L; // don't know yet
					context.next = next;
				}

			}
			// write out last context
			call.getOutputCollector().add(context.asTuple());

		}
	}
	
	@SuppressWarnings({ "serial", "rawtypes" })
	private static class DuplicateSelfJoinFilter extends BaseOperation implements Filter {

		@Override
		public boolean isRemove(FlowProcess flow, FilterCall call) {
			TupleEntry arguments = call.getArguments();
			long id = arguments.getLong("id");
			long from = arguments.getLong("from");
			long to = arguments.getLong("to");
			long relnum = arguments.getLong("relnum");
			long toid = arguments.getLong("toid");
			long from2 = arguments.getLong("from2");
			long to2 = arguments.getLong("to2");
			long relnum2 = arguments.getLong("relnum2");

			//TODO Also remove the records which don't start
			return id == toid && from == from2 && to == to2 && relnum == relnum2;
			
		}
		
	}

	@SuppressWarnings({ "serial", "rawtypes" })
	private static class EdgeNodeIdsToNodeRownums extends BaseOperation implements Function {

		public EdgeNodeIdsToNodeRownums() {
			super(new Fields("id", "name", "rownum", "from", "to", "relnum", "fromid", "toid"));
		}
		
		@Override
		public void operate(FlowProcess flow, FunctionCall call) {
			TupleEntry arguments = call.getArguments();
			long id = arguments.getLong("id");
			long rownum = arguments.getLong("rownum");
			long from = arguments.getLong("from");
			long to = arguments.getLong("to");
			long toid = -1L;
			long fromid = -1L;
			if (id == from) fromid = rownum;
			if (id == to) toid = rownum;
			call.getOutputCollector().add(new Tuple(id, arguments.getString("name"), rownum, from, to, arguments.getLong("relnum"), fromid, toid));
		}
		
	}
	
	@SuppressWarnings({ "serial", "rawtypes" })
	private static class EdgeRecordCreator extends BaseOperation implements Function {
		public EdgeRecordCreator() {
			super(new Fields("edge"));
		}

		@Override
		public void operate(FlowProcess flow, FunctionCall call) {
			TupleEntry arguments = call.getArguments();
			long id = arguments.getLong("id");
			long from = arguments.getLong("from");
			long to = arguments.getLong("to");
			long relnum = arguments.getLong("relnum");
			long fromprev = arguments.getLong("fromprev");
			long fromnext = arguments.getLong("fromnext");
			long toprev = arguments.getLong("toprev");
			long tonext = arguments.getLong("tonext");
			

			call.getOutputCollector().add(
					new Tuple(getEdgeAsBuffer(relnum, from, to, -1, fromprev, fromnext, toprev, tonext, -1L)));

		}

		private Buffer getEdgeAsBuffer(long relnum, long from, long to, int type, long fromprev, long fromnext, long toprev, long tonext, long prop) {
			ByteBuffer buffer = ByteBuffer.allocate(33);

			RelationshipRecord rr = new RelationshipRecord(relnum, from, to, type);
			rr.setInUse(true);
			rr.setCreated();

//			LOG.debug(rr.toString());

            short fromMod = (short)((from & 0x700000000L) >> 31);
            long toMod = (to & 0x700000000L) >> 4;
            long fromPrevRelMod = fromprev == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (fromprev & 0x700000000L) >> 7;
            long fromNextRelMod = fromnext == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (fromnext & 0x700000000L) >> 10;
            long toPrevRelMod = toprev == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (toprev & 0x700000000L) >> 13;
            long toNextRelMod = tonext == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (tonext & 0x700000000L) >> 16;
            long propMod = prop == Record.NO_NEXT_PROPERTY.intValue() ? 0 : (prop & 0xF00000000L) >> 28;

            // [    ,   x] in use flag
            // [    ,xxx ] first node high order bits
            // [xxxx,    ] next prop high order bits
            short inUseUnsignedByte = (short)((rr.inUse() ? Record.IN_USE : Record.NOT_IN_USE).byteValue() | fromMod | propMod);

            // [ xxx,    ][    ,    ][    ,    ][    ,    ] second node high order bits,     0x70000000
            // [    ,xxx ][    ,    ][    ,    ][    ,    ] first prev rel high order bits,  0xE000000
            // [    ,   x][xx  ,    ][    ,    ][    ,    ] first next rel high order bits,  0x1C00000
            // [    ,    ][  xx,x   ][    ,    ][    ,    ] second prev rel high order bits, 0x380000
            // [    ,    ][    , xxx][    ,    ][    ,    ] second next rel high order bits, 0x70000
            // [    ,    ][    ,    ][xxxx,xxxx][xxxx,xxxx] type
            int typeInt = (int)(rr.getType() | toMod | fromPrevRelMod | fromNextRelMod | toPrevRelMod | toNextRelMod);

            buffer.put( (byte)inUseUnsignedByte ).putInt( (int) from ).putInt( (int) to )
                .putInt( typeInt ).putInt( (int) fromprev ).putInt( (int) fromnext )
                .putInt( (int) toprev ).putInt( (int) tonext ).putInt( (int) prop );
			return buffer;

		}
	}

}
