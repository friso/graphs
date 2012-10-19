package nl.waredingen.graphs.neo.mapreduce.edges.surround.join;

import java.io.IOException;

import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.SurroundingEdgeWritable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinSurroundingEdgesMapper extends Mapper<NullWritable, SurroundingEdgeWritable, EdgeWritable, SurroundingEdgeWritable> {

	private EdgeWritable outputKey = new EdgeWritable();
	private SurroundingEdgeWritable outputValue = new SurroundingEdgeWritable();

	@Override
	protected void map(NullWritable key, SurroundingEdgeWritable value, Context context) throws IOException, InterruptedException {
		outputKey.set(value.getEdgeId(), value.getFromNodeId(), value.getToNodeId(), value.getEdgePropId());
		outputValue.set(value.getNodeId(), value.getEdgeId(), value.getFromNodeId(), value.getToNodeId(), value.getEdgePropId(), value.getEdgePrev(), value.getEdgeNext());
		context.write(outputKey, outputValue);
	}
}
