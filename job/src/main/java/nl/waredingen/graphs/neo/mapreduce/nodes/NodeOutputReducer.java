package nl.waredingen.graphs.neo.mapreduce.nodes;

import java.io.IOException;
import java.util.Iterator;

import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgeIdPropIdWritable;
import nl.waredingen.graphs.neo.neo4j.Neo4JUtils;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.neo4j.kernel.impl.nioneo.store.Record;

public class NodeOutputReducer extends Reducer<LongWritable, EdgeIdPropIdWritable, NullWritable, BytesWritable> {
	private BytesWritable outputValue = new BytesWritable();

	@Override
	protected void reduce(LongWritable key, Iterable<EdgeIdPropIdWritable> values, Context context) throws IOException,
			InterruptedException {
		Iterator<EdgeIdPropIdWritable> itr = values.iterator();
		if (!itr.hasNext()) {
			return;
		}

		long id = key.get();

		// walk through values and pick the smallest relnum and propid
		long relnum = Long.MAX_VALUE;
		long propnum = Long.MAX_VALUE;
		while (itr.hasNext()) {
			EdgeIdPropIdWritable value = itr.next();
			relnum = Math.min(relnum, value.getEdgeId().get());
			propnum = Math.min(propnum, value.getPropId().get());
		}

		if (relnum == Long.MAX_VALUE) relnum = -1L;
		if (propnum == Long.MAX_VALUE) propnum = -1L;

		if (id == 0L) {
			// write a rootnode once
			writeNode(id, Record.NO_NEXT_RELATIONSHIP.intValue(), Record.NO_NEXT_PROPERTY.intValue(), context);
		}

		writeNode(id + 1L, relnum, propnum, context);
	}

	private void writeNode(long id, long relnum, long prop, Context context) throws IOException, InterruptedException {
		byte[] ba = Neo4JUtils.getNodeAsByteArray(id, relnum, prop);
		outputValue.set(ba, 0, ba.length);
		context.write(NullWritable.get(), outputValue);
	}

}