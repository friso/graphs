package nl.waredingen.graphs.neo.mapreduce.group;

import java.io.IOException;

import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.NodeEdgeIdWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.NodeEdgeWritable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class GroupNodesAndEdgesReducer extends Reducer<NodeEdgeIdWritable, EdgeWritable, NullWritable, NodeEdgeWritable> {

	private NodeEdgeWritable outputValue = new NodeEdgeWritable();
	
	@Override
	protected void reduce(NodeEdgeIdWritable key, Iterable<EdgeWritable> values, Context context) throws IOException ,InterruptedException {
		for (EdgeWritable value : values) {
			outputValue.set(key.getNodeId().get(), key.getPropId().get(), value.getEdgeId().get(), value.getFromNodeId().get(), value.getToNodeId().get(), value.getEdgePropId().get());
			context.write(NullWritable.get(), outputValue);
		}
	}
}
