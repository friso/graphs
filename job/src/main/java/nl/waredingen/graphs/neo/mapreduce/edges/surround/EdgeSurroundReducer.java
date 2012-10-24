package nl.waredingen.graphs.neo.mapreduce.edges.surround;

import java.io.IOException;
import java.util.Iterator;

import nl.waredingen.graphs.neo.mapreduce.SurroundingContext;
import nl.waredingen.graphs.neo.mapreduce.input.writables.AscLongDescLongWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.SurroundingEdgeWritable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class EdgeSurroundReducer extends Reducer<AscLongDescLongWritable, EdgeWritable, NullWritable, SurroundingEdgeWritable> {

	private SurroundingEdgeWritable outputValue = new SurroundingEdgeWritable();

	@Override
	protected void reduce(AscLongDescLongWritable key, Iterable<EdgeWritable> values, Context context) throws IOException ,InterruptedException {
		Iterator<EdgeWritable> iter = values.iterator();
		
		SurroundingContext edge = new SurroundingContext();
		
		while (iter.hasNext()) {
			EdgeWritable value = iter.next();
			
			long id = key.getLeft().get();
			long from = value.getFromNodeId().get();
			long to = value.getToNodeId().get();
			long relnum = value.getEdgeId().get();
			long prop = value.getEdgePropId().get();
			if (edge.nodeid == -1L) {
				// first call, so set current fields
				edge.nodeid = id;
				edge.from = from;
				edge.to = to;
				edge.edgeid = relnum;
				edge.edgeprop = prop;
				edge.prev = -1L; // don't know yet
				edge.next = -1L; // first call, relationships ordered descending, so last rel, so no next available

			} else if (edge.prev == -1L) {
				// not the first so current relationship will become prev in
				// context and context can be emitted and refilled with
				// current
				edge.prev = relnum;

				outputValue.set(edge.nodeid, edge.edgeid, edge.from, edge.to, edge.edgeprop, edge.prev, edge.next);
				context.write(NullWritable.get(), outputValue);

				long next = edge.edgeid;
				edge.nodeid = id;
				edge.from = from;
				edge.to = to;
				edge.edgeid = relnum;
				edge.edgeprop = prop;
				edge.prev = -1L; // don't know yet
				edge.next = next;

			}

		}

		// write out last context
		outputValue.set(edge.nodeid, edge.edgeid, edge.from, edge.to, edge.edgeprop, edge.prev, edge.next);
		context.write(NullWritable.get(), outputValue);

	}

}
