package nl.waredingen.graphs.neo.mapreduce.group;

import java.io.IOException;

import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.NodeEdgeIdWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GroupNodesAndEdgesMapper extends Mapper<NullWritable, Text, NodeEdgeIdWritable, EdgeWritable> {

	private NodeEdgeIdWritable outputKey = new NodeEdgeIdWritable();
	private EdgeWritable outputValue = new EdgeWritable();
	private LongWritable edgeId = new LongWritable();
	private LongWritable fromNodeId = new LongWritable();
	private LongWritable toNodeId = new LongWritable();
	private LongWritable edgePropId = new LongWritable();
	private LongWritable fromPropId = new LongWritable();
	private LongWritable toPropId = new LongWritable();

	@Override
	protected void map(NullWritable key, Text value, Context context) throws IOException ,InterruptedException {
		//edgeid	fromnode	tonode	edgePropId	fromnodeid	fromPropId	tonodeid toPropId
		String[] values = value.toString().split("\t", 8);

		edgeId.set(Long.parseLong(values[0]));
		edgePropId.set(Long.parseLong(values[3]));
		fromNodeId.set(Long.parseLong(values[4]));
		fromPropId.set(Long.parseLong(values[5]));
		toNodeId.set(Long.parseLong(values[6]));
		toPropId.set(Long.parseLong(values[7]));
		outputValue.set(edgeId, fromNodeId, toNodeId, edgePropId);

		outputKey.set(fromNodeId, fromPropId, edgeId);
		context.write(outputKey, outputValue);
		
		outputKey.set(toNodeId, toPropId, edgeId);
		context.write(outputKey, outputValue);
	}
}
