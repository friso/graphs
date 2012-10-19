package nl.waredingen.graphs.neo.mapreduce.edges;

import java.io.IOException;
import java.util.Iterator;

import nl.waredingen.graphs.neo.mapreduce.input.writables.FullEdgeWritable;
import nl.waredingen.graphs.neo.neo4j.Neo4JUtils;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class EdgeOutputReducer extends Reducer<LongWritable, FullEdgeWritable, NullWritable, BytesWritable> {
	
	private BytesWritable outputValue = new BytesWritable();
	
	@Override
	protected void reduce(LongWritable key, Iterable<FullEdgeWritable> values, Context context) throws IOException,
			InterruptedException {
		Iterator<FullEdgeWritable> itr = values.iterator();
		if (!itr.hasNext()) {
			return;
		}

		// only use first record per key. Rest is duplicates from the selfjoin in the previous step
		FullEdgeWritable value = itr.next();

		long relnum = key.get();

		long from = value.getFromNodeId().get();
		long to = value.getToNodeId().get();
		long fromprev = value.getFromPrev().get();
		long fromnext = value.getFromNext().get();
		long toprev = value.getToPrev().get();
		long tonext = value.getToNext().get();
		long prop = value.getEdgeProp().get();

		writeEdge(relnum, from , to, 0, fromprev, fromnext, toprev, tonext, prop, context);
	}

	private void writeEdge(long relnum, long from, long to, int type, long fromprev, long fromnext, long toprev,
			long tonext, long prop, Context context) throws IOException, InterruptedException {
		byte[] ba = Neo4JUtils.getEdgeAsByteArray(relnum, from, to, type, fromprev, fromnext, toprev, tonext, prop);
		outputValue.set(ba, 0, ba.length);
		context.write(NullWritable.get(), outputValue);
	}

}
