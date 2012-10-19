package nl.waredingen.graphs.neo.mapreduce.nodes;

import java.io.IOException;

import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgeIdPropIdWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.NodeEdgeWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class NodeOutputMapper extends Mapper<NullWritable, NodeEdgeWritable, LongWritable, EdgeIdPropIdWritable> {

	private LongWritable outputKey = new LongWritable();
	private EdgeIdPropIdWritable outputValue = new EdgeIdPropIdWritable();

	@Override
	protected void map(NullWritable key, NodeEdgeWritable value, Context context) throws IOException, InterruptedException {
		outputKey.set(value.getNode().getNodeId().get());

		outputValue.set(value.getEdge().getEdgeId(), value.getNode().getPropId());
		context.write(outputKey, outputValue);
	}
}
