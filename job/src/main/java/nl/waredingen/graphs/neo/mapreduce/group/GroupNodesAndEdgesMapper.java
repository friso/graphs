package nl.waredingen.graphs.neo.mapreduce.group;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GroupNodesAndEdgesMapper extends Mapper<Text, Text, Text, Text> {

	private Text outputKey = new Text();
	private Text outputValue = new Text();
	
	protected void map(Text key, Text value, Context context) throws IOException ,InterruptedException {
		//edgeid fromnode	tonode	fromnodeid	fromnode	fromname	tonodeid	tonode	toname
		String[] values = value.toString().split("\t",9);
		//edgeid	fromnodeid	tonodeid
		outputValue.set(values[0] + "\t" + values[3]+ "\t" + values[6]);

		//fromnodeid	fromnode	fromname;edgeid
		outputKey.set(values[3]+"\t"+values[4]+"\t"+values[5]+";"+values[0]);
		context.write(outputKey, outputValue);
		
		//tonodeid	tonode	toname;edgeid
		outputKey.set(values[6]+"\t"+values[7]+"\t"+values[8]+";"+values[0]);
		context.write(outputKey, outputValue);
	}
}
