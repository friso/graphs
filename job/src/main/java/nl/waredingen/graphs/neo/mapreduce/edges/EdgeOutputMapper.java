package nl.waredingen.graphs.neo.mapreduce.edges;

import java.io.IOException;

import nl.waredingen.graphs.neo.mapreduce.input.writables.DoubleSurroundingEdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.FullEdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.SurroundingEdgeWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class EdgeOutputMapper extends Mapper<NullWritable, DoubleSurroundingEdgeWritable, LongWritable, FullEdgeWritable> {

	private LongWritable outputKey = new LongWritable();
	private FullEdgeWritable outputValue = new FullEdgeWritable();

	protected void map(NullWritable key, DoubleSurroundingEdgeWritable value, Context context) throws IOException, InterruptedException {
		SurroundingEdgeWritable left = value.getLeft();
		SurroundingEdgeWritable right = value.getRight();
		
		if (left.getNodeId().equals(left.getFromNodeId())) {
			outputKey.set(value.getLeft().getEdgeId().get());
			outputValue.set(left.getFromNodeId(), left.getToNodeId(), left.getEdgePropId(), left.getEdgePrev(), left.getEdgeNext(), right.getEdgePrev(), right.getEdgeNext());
			context.write(outputKey, outputValue);
		}
	}
}
