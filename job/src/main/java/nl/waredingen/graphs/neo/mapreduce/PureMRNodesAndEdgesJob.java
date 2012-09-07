package nl.waredingen.graphs.neo.mapreduce;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import nl.waredingen.graphs.misc.RowNumberJob;
import nl.waredingen.graphs.misc.RowNumberJob.IndifferentComparator;
import nl.waredingen.graphs.neo.mapreduce.edges.EdgeOutputMapper;
import nl.waredingen.graphs.neo.mapreduce.edges.EdgeOutputReducer;
import nl.waredingen.graphs.neo.mapreduce.edges.surround.EdgeSurroundMapper;
import nl.waredingen.graphs.neo.mapreduce.edges.surround.EdgeSurroundReducer;
import nl.waredingen.graphs.neo.mapreduce.edges.surround.join.JoinSurroundingEdgesMapper;
import nl.waredingen.graphs.neo.mapreduce.edges.surround.join.JoinSurroundingEdgesReducer;
import nl.waredingen.graphs.neo.mapreduce.group.GroupNodesAndEdgesMapper;
import nl.waredingen.graphs.neo.mapreduce.group.GroupNodesAndEdgesReducer;
import nl.waredingen.graphs.neo.mapreduce.group.NodeAndEdgeIdKeyComparator;
import nl.waredingen.graphs.neo.mapreduce.group.NodeAndEdgeIdKeyGroupingComparator;
import nl.waredingen.graphs.neo.mapreduce.group.NodeAndEdgeIdKeyPartitioner;
import nl.waredingen.graphs.neo.mapreduce.join.JoinFromEdgesMapper;
import nl.waredingen.graphs.neo.mapreduce.join.JoinNodesAndEdgesReducer;
import nl.waredingen.graphs.neo.mapreduce.join.JoinNodesMapper;
import nl.waredingen.graphs.neo.mapreduce.join.JoinToEdgesMapper;
import nl.waredingen.graphs.neo.mapreduce.join.NodeAndEdgeKeyComparator;
import nl.waredingen.graphs.neo.mapreduce.join.NodeAndEdgeKeyPartitioner;
import nl.waredingen.graphs.neo.mapreduce.join.NodeKeyGroupingComparator;
import nl.waredingen.graphs.neo.mapreduce.nodes.NodeOutputMapper;
import nl.waredingen.graphs.neo.mapreduce.nodes.NodeOutputReducer;
import nl.waredingen.graphs.neo.mapreduce.properties.NodePreparePropertiesMapper;
import nl.waredingen.graphs.neo.mapreduce.properties.NodePreparePropertiesReducer;
import nl.waredingen.graphs.neo.mapreduce.properties.PropertyOutputIdBlockcountPartitioner;
import nl.waredingen.graphs.neo.mapreduce.properties.PropertyOutputIdBlockcountValueWritable;
import nl.waredingen.graphs.neo.mapreduce.properties.PropertyOutputMapper;
import nl.waredingen.graphs.neo.mapreduce.properties.PropertyOutputReducer;
import nl.waredingen.graphs.neo.neo4j.Neo4JUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Task.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PureMRNodesAndEdgesJob {

	public static final String NUMBEROFROWS_CONFIG = "neo.nodes.edges.job.nr_of_rows";

	public static int run(String nodes, String edges, String output, Configuration conf) {
		String numberedNodes = output + "/numberednodes";
		String numberedEdges = output + "/numberededges";

		String temp = output + "/temp";
		String joined = output + "/joined";
		String grouped = output + "/grouped";
		String nodesOutput = output + "/neostore.nodestore.db";
		String surrounding = output + "/surrounding";
		String joinededges = output + "/joinededges";
		String edgesOutput = output + "/neostore.relationshipstore.db";
		String nodePropertiesPrepareOutput = output +"/nodeproperties";
		String propertiesOutput = output +"/properties";

		try {
			long nrOfNodes = RowNumberJob.run(nodes, numberedNodes, conf);
			long nrOfEdges = RowNumberJob.run(edges, numberedEdges, conf);

			System.out.println("Processing " + nrOfNodes + " nodes and " + nrOfEdges + " edges.");

			Job joinFrom = new Job(conf, "Join from nodes and edges job.");
			joinFrom.setGroupingComparatorClass(NodeKeyGroupingComparator.class);
			joinFrom.setSortComparatorClass(NodeAndEdgeKeyComparator.class);
			joinFrom.setPartitionerClass(NodeAndEdgeKeyPartitioner.class);

			joinFrom.setMapOutputKeyClass(Text.class);
			joinFrom.setMapOutputValueClass(Text.class);

			MultipleInputs
					.addInputPath(joinFrom, new Path(numberedNodes), TextInputFormat.class, JoinNodesMapper.class);
			MultipleInputs.addInputPath(joinFrom, new Path(numberedEdges), TextInputFormat.class,
					JoinFromEdgesMapper.class);

			joinFrom.setReducerClass(JoinNodesAndEdgesReducer.class);
			joinFrom.setOutputKeyClass(Text.class);
			joinFrom.setOutputValueClass(Text.class);

			joinFrom.setOutputFormatClass(TextOutputFormat.class);
			FileOutputFormat.setOutputPath(joinFrom, new Path(temp));

			joinFrom.setJarByClass(PureMRNodesAndEdgesJob.class);

			joinFrom.waitForCompletion(true);

			Job joinTo = new Job(conf, "Join to nodes and edges job.");
			joinTo.setGroupingComparatorClass(NodeKeyGroupingComparator.class);
			joinTo.setSortComparatorClass(NodeAndEdgeKeyComparator.class);
			joinTo.setPartitionerClass(NodeAndEdgeKeyPartitioner.class);

			joinTo.setMapOutputKeyClass(Text.class);
			joinTo.setMapOutputValueClass(Text.class);

			MultipleInputs.addInputPath(joinTo, new Path(numberedNodes), TextInputFormat.class, JoinNodesMapper.class);
			MultipleInputs.addInputPath(joinTo, new Path(temp), KeyValueTextInputFormat.class, JoinToEdgesMapper.class);

			joinTo.setReducerClass(JoinNodesAndEdgesReducer.class);
			joinTo.setOutputKeyClass(Text.class);
			joinTo.setOutputValueClass(Text.class);

			joinTo.setOutputFormatClass(TextOutputFormat.class);
			FileOutputFormat.setOutputPath(joinTo, new Path(joined));

			joinTo.setJarByClass(PureMRNodesAndEdgesJob.class);

			joinTo.waitForCompletion(true);

			Job groupJob = new Job(conf, "Join to nodes and edges job.");
			groupJob.setGroupingComparatorClass(NodeAndEdgeIdKeyGroupingComparator.class);
			groupJob.setSortComparatorClass(NodeAndEdgeIdKeyComparator.class);
			groupJob.setPartitionerClass(NodeAndEdgeIdKeyPartitioner.class);

			groupJob.setMapOutputKeyClass(Text.class);
			groupJob.setMapOutputValueClass(Text.class);

			groupJob.setMapperClass(GroupNodesAndEdgesMapper.class);
			groupJob.setInputFormatClass(KeyValueTextInputFormat.class);
			FileInputFormat.addInputPath(groupJob, new Path(joined));

			groupJob.setReducerClass(GroupNodesAndEdgesReducer.class);
			groupJob.setOutputKeyClass(NullWritable.class);
			groupJob.setOutputValueClass(Text.class);

			groupJob.setOutputFormatClass(TextOutputFormat.class);
			FileOutputFormat.setOutputPath(groupJob, new Path(grouped));

			groupJob.setJarByClass(PureMRNodesAndEdgesJob.class);

			groupJob.waitForCompletion(true);

			conf.set(NUMBEROFROWS_CONFIG, "" + nrOfNodes);
			Job nodeOutputJob = new Job(conf, "Output nodes job.");
			nodeOutputJob.setPartitionerClass(RownumPartitioner.class);

			nodeOutputJob.setMapOutputKeyClass(LongWritable.class);
			nodeOutputJob.setMapOutputValueClass(Text.class);

			nodeOutputJob.setMapperClass(NodeOutputMapper.class);
			nodeOutputJob.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPath(nodeOutputJob, new Path(grouped));

			nodeOutputJob.setReducerClass(NodeOutputReducer.class);
			nodeOutputJob.setOutputKeyClass(NullWritable.class);
			nodeOutputJob.setOutputValueClass(BytesWritable.class);

			nodeOutputJob.setOutputFormatClass(NewByteBufferOutputFormat.class);
			FileOutputFormat.setOutputPath(nodeOutputJob, new Path(nodesOutput));

			nodeOutputJob.setJarByClass(PureMRNodesAndEdgesJob.class);

			nodeOutputJob.waitForCompletion(true);

			Job edgeSurroundJob = new Job(conf, "Determine surrounding edges job.");
			edgeSurroundJob.setGroupingComparatorClass(AscLongDescLongKeyGroupingComparator.class);
			edgeSurroundJob.setSortComparatorClass(AscLongDescLongKeyComparator.class);
			edgeSurroundJob.setPartitionerClass(AscLongDescLongWritablePartitioner.class);

			edgeSurroundJob.setMapOutputKeyClass(AscLongDescLongWritable.class);
			edgeSurroundJob.setMapOutputValueClass(Text.class);

			edgeSurroundJob.setMapperClass(EdgeSurroundMapper.class);
			edgeSurroundJob.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPath(edgeSurroundJob, new Path(grouped));

			edgeSurroundJob.setReducerClass(EdgeSurroundReducer.class);
			edgeSurroundJob.setOutputKeyClass(Text.class);
			edgeSurroundJob.setOutputValueClass(Text.class);

			edgeSurroundJob.setOutputFormatClass(TextOutputFormat.class);
			FileOutputFormat.setOutputPath(edgeSurroundJob, new Path(surrounding));

			edgeSurroundJob.setJarByClass(PureMRNodesAndEdgesJob.class);

			edgeSurroundJob.waitForCompletion(true);

			Job joinSurroundJob = new Job(conf, "Join surrounding edges job.");

			joinSurroundJob.setMapOutputKeyClass(Text.class);
			joinSurroundJob.setMapOutputValueClass(Text.class);

			joinSurroundJob.setMapperClass(JoinSurroundingEdgesMapper.class);
			joinSurroundJob.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPath(joinSurroundJob, new Path(surrounding));

			joinSurroundJob.setReducerClass(JoinSurroundingEdgesReducer.class);
			joinSurroundJob.setOutputKeyClass(NullWritable.class);
			joinSurroundJob.setOutputValueClass(Text.class);

			joinSurroundJob.setOutputFormatClass(TextOutputFormat.class);
			FileOutputFormat.setOutputPath(joinSurroundJob, new Path(joinededges));

			joinSurroundJob.setJarByClass(PureMRNodesAndEdgesJob.class);

			joinSurroundJob.waitForCompletion(true);

			conf.set(NUMBEROFROWS_CONFIG, "" + nrOfEdges);
			Job edgeOutputJob = new Job(conf, "Output nodes job.");
			edgeOutputJob.setPartitionerClass(RownumPartitioner.class);

			edgeOutputJob.setMapOutputKeyClass(LongWritable.class);
			edgeOutputJob.setMapOutputValueClass(Text.class);

			edgeOutputJob.setMapperClass(EdgeOutputMapper.class);
			edgeOutputJob.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPath(edgeOutputJob, new Path(joinededges));

			edgeOutputJob.setReducerClass(EdgeOutputReducer.class);
			edgeOutputJob.setOutputKeyClass(NullWritable.class);
			edgeOutputJob.setOutputValueClass(BytesWritable.class);

			edgeOutputJob.setOutputFormatClass(NewByteBufferOutputFormat.class);
			FileOutputFormat.setOutputPath(edgeOutputJob, new Path(edgesOutput));

			edgeOutputJob.setJarByClass(PureMRNodesAndEdgesJob.class);

			edgeOutputJob.waitForCompletion(true);

			Neo4JUtils.writeNeostore(output, conf);
			Neo4JUtils.writeNodeIds(nrOfNodes, output, conf);
			Neo4JUtils.writeEdgeIds(nrOfEdges, output, conf);
			Neo4JUtils.writeSingleTypeStore("TRANSFER_TO", output, conf);

			Job nodePropertiesPrepareJob = new Job(conf, "Prepare node properties job.");
			nodePropertiesPrepareJob.setGroupingComparatorClass(AscLongDescLongKeyGroupingComparator.class);
			nodePropertiesPrepareJob.setSortComparatorClass(AscLongDescLongKeyComparator.class);
			nodePropertiesPrepareJob.setPartitionerClass(AscLongDescLongWritablePartitioner.class);

			nodePropertiesPrepareJob.setMapOutputKeyClass(AscLongDescLongWritable.class);
			nodePropertiesPrepareJob.setMapOutputValueClass(Text.class);

			nodePropertiesPrepareJob.setMapperClass(NodePreparePropertiesMapper.class);
			nodePropertiesPrepareJob.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPath(nodePropertiesPrepareJob, new Path(numberedNodes));

			nodePropertiesPrepareJob.setReducerClass(NodePreparePropertiesReducer.class);
			nodePropertiesPrepareJob.setOutputKeyClass(AscLongDescLongWritable.class);
			nodePropertiesPrepareJob.setOutputValueClass(Text.class);

			nodePropertiesPrepareJob.setOutputFormatClass(TextOutputFormat.class);
			FileOutputFormat.setOutputPath(nodePropertiesPrepareJob, new Path(nodePropertiesPrepareOutput));

			nodePropertiesPrepareJob.setJarByClass(PureMRNodesAndEdgesJob.class);

			nodePropertiesPrepareJob.waitForCompletion(true);

			Map<Integer, Entry<String, Class<?>>> namesMap = new HashMap<Integer, Entry<String, Class<?>>>();
			namesMap.put(0, new SimpleEntry<String, Class<?>>("identifier", String.class));
			namesMap.put(1, new SimpleEntry<String, Class<?>>("name", String.class));
			
			Neo4JUtils.writePropertyKeyStore(namesMap, propertiesOutput, conf);

			Neo4JUtils.writePropertyStoreFooter(propertiesOutput, conf);
			Neo4JUtils.writePropertyStringStoreHeader(propertiesOutput, conf);
			Neo4JUtils.writePropertyStringStoreFooter(propertiesOutput, conf);

			conf.set(NUMBEROFROWS_CONFIG, "" + nrOfNodes * namesMap.size());
			Job nodePropertiesOutputJob = new Job(conf, "Output nodes job.");
			nodePropertiesOutputJob.setPartitionerClass(PropertyOutputIdBlockcountPartitioner.class);
			nodePropertiesOutputJob.setGroupingComparatorClass(IndifferentComparator.class);

			nodePropertiesOutputJob.setMapOutputKeyClass(ByteWritable.class);
			nodePropertiesOutputJob.setMapOutputValueClass(PropertyOutputIdBlockcountValueWritable.class);

			nodePropertiesOutputJob.setMapperClass(PropertyOutputMapper.class);
			nodePropertiesOutputJob.setInputFormatClass(TextInputFormat.class);
			FileInputFormat.addInputPath(nodePropertiesOutputJob, new Path(nodePropertiesPrepareOutput));

			nodePropertiesOutputJob.setReducerClass(PropertyOutputReducer.class);

			FileOutputFormat.setOutputPath(nodePropertiesOutputJob, new Path(propertiesOutput + "/propertystore.db"));
			MultipleOutputs.addNamedOutput(nodePropertiesOutputJob, "props", NewByteBufferOutputFormat.class, NullWritable.class, BytesWritable.class);
			MultipleOutputs.addNamedOutput(nodePropertiesOutputJob, "strings", NewByteBufferOutputFormat.class, NullWritable.class, BytesWritable.class);

			MultipleOutputs.setCountersEnabled(nodePropertiesOutputJob, true);
			
			nodePropertiesOutputJob.setJarByClass(PureMRNodesAndEdgesJob.class);

			nodePropertiesOutputJob.waitForCompletion(true);
			
			long nrOfWrittenStringBlocks = nodePropertiesOutputJob.getCounters().findCounter("org.apache.hadoop.mapreduce.lib.output.MultipleOutputs", "strings.blocks").getValue();

				System.out.println(nrOfWrittenStringBlocks);

			Neo4JUtils.writePropertyIds(nrOfNodes * namesMap.size(), propertiesOutput + "/neostore.propertystore.db", conf);
			Neo4JUtils.writePropertyIds(nrOfWrittenStringBlocks, propertiesOutput + "/neostore.propertystore.db.strings", conf);
			
			Neo4JUtils.writeEmptArrayStore(propertiesOutput, conf);


		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace(System.err);
			return 1;
		}

		return 0;
	}
}