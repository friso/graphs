package nl.waredingen.graphs.neo.mapreduce.properties;

import java.io.IOException;

import nl.waredingen.graphs.neo.mapreduce.AscLongDescLongWritable;
import nl.waredingen.graphs.neo.neo4j.Neo4JUtils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;

public class NodePreparePropertiesMapper extends Mapper<LongWritable, Text, AscLongDescLongWritable, Text> {
	private AscLongDescLongWritable outputKey = new AscLongDescLongWritable();
	private Text outputValue = new Text();

	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// TODO choose correct number for 42
		String[] values = value.toString().split("\t", 42);
		int nodeId = Integer.parseInt(values[0]);
		for (int i = 0; i < values.length - 1; i++) {
			String property = values[i+1];
			int propId = (nodeId * (values.length - 1)) + i;
			outputKey.setLeft(new LongWritable(nodeId));
			outputKey.setRight(new LongWritable(propId));
			outputValue.set(i + "\t" +property + "\t" + getBlockCount(i, property));
			context.write(outputKey, outputValue);
		}
	}

	private int getBlockCount(int propertyKey, String property) {
		PropertyBlock block = new PropertyBlock();
		Neo4JUtils.encodeValue(block, propertyKey, property, 0L);
		return block.getValueRecords().size();
	}
}
