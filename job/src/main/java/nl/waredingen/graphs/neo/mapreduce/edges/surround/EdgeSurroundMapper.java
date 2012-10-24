package nl.waredingen.graphs.neo.mapreduce.edges.surround;

import java.io.IOException;

import nl.waredingen.graphs.neo.mapreduce.input.writables.AscLongDescLongWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.NodeEdgeWritable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class EdgeSurroundMapper extends Mapper<NullWritable, NodeEdgeWritable, AscLongDescLongWritable, EdgeWritable> {

	private AscLongDescLongWritable outputKey = new AscLongDescLongWritable();

	@Override
	protected void map(NullWritable key, NodeEdgeWritable value, Context context) throws IOException, InterruptedException {
		outputKey.setLeft(value.getNode().getNodeId());
		outputKey.setRight(value.getEdge().getEdgeId());

		context.write(outputKey, value.getEdge());
	}
}
