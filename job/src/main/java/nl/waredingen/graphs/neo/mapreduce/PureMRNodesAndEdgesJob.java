package nl.waredingen.graphs.neo.mapreduce;

import nl.waredingen.graphs.misc.RowNumberJob;
import nl.waredingen.graphs.neo.mapreduce.edges.EdgeOutputMapper;
import nl.waredingen.graphs.neo.mapreduce.edges.EdgeOutputReducer;
import nl.waredingen.graphs.neo.mapreduce.edges.EdgeOutputRownumPartitioner;
import nl.waredingen.graphs.neo.mapreduce.edges.surround.EdgeSurroundMapper;
import nl.waredingen.graphs.neo.mapreduce.edges.surround.EdgeSurroundReducer;
import nl.waredingen.graphs.neo.mapreduce.edges.surround.join.EdgeWritableKeyComparator;
import nl.waredingen.graphs.neo.mapreduce.edges.surround.join.EdgeWritableKeyGroupingComparator;
import nl.waredingen.graphs.neo.mapreduce.edges.surround.join.JoinSurroundingEdgesMapper;
import nl.waredingen.graphs.neo.mapreduce.edges.surround.join.JoinSurroundingEdgesReducer;
import nl.waredingen.graphs.neo.mapreduce.group.GroupNodesAndEdgesMapper;
import nl.waredingen.graphs.neo.mapreduce.group.GroupNodesAndEdgesReducer;
import nl.waredingen.graphs.neo.mapreduce.group.NodeAndEdgeIdKeyComparator;
import nl.waredingen.graphs.neo.mapreduce.group.NodeAndEdgeIdKeyGroupingComparator;
import nl.waredingen.graphs.neo.mapreduce.group.NodeAndEdgeIdKeyPartitioner;
import nl.waredingen.graphs.neo.mapreduce.input.AbstractMetaData;
import nl.waredingen.graphs.neo.mapreduce.input.MetaData;
import nl.waredingen.graphs.neo.mapreduce.input.MetaDataFromConfigImpl;
import nl.waredingen.graphs.neo.mapreduce.input.writables.AscLongDescLongWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.ByteMarkerIdPropIdWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.DoubleSurroundingEdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgeIdPropIdWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgePropertyOutputCountersAndValueWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.EdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.FullEdgePropertiesWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.FullEdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.FullNodePropertiesWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.NodeEdgeIdWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.NodeEdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.NodePropertyOutputCountersAndValueWritable;
import nl.waredingen.graphs.neo.mapreduce.input.writables.SurroundingEdgeWritable;
import nl.waredingen.graphs.neo.mapreduce.join.JoinFromEdgesMapper;
import nl.waredingen.graphs.neo.mapreduce.join.JoinNodesAndEdgesReducer;
import nl.waredingen.graphs.neo.mapreduce.join.JoinNodesMapper;
import nl.waredingen.graphs.neo.mapreduce.join.JoinToEdgesMapper;
import nl.waredingen.graphs.neo.mapreduce.join.NodeAndEdgeKeyComparator;
import nl.waredingen.graphs.neo.mapreduce.join.NodeAndEdgeKeyPartitioner;
import nl.waredingen.graphs.neo.mapreduce.join.NodeKeyGroupingComparator;
import nl.waredingen.graphs.neo.mapreduce.nodes.NodeOutputMapper;
import nl.waredingen.graphs.neo.mapreduce.nodes.NodeOutputReducer;
import nl.waredingen.graphs.neo.mapreduce.nodes.NodeOutputRownumPartitioner;
import nl.waredingen.graphs.neo.mapreduce.properties.ByteMarkerAndIdComparator;
import nl.waredingen.graphs.neo.mapreduce.properties.EdgePreparePropertiesMapper;
import nl.waredingen.graphs.neo.mapreduce.properties.EdgePreparePropertiesReducer;
import nl.waredingen.graphs.neo.mapreduce.properties.EdgePropertyOutputMapper;
import nl.waredingen.graphs.neo.mapreduce.properties.EdgePropertyOutputPartitioner;
import nl.waredingen.graphs.neo.mapreduce.properties.EdgePropertyOutputReducer;
import nl.waredingen.graphs.neo.mapreduce.properties.IndifferentByteMarkerAndIdComparator;
import nl.waredingen.graphs.neo.mapreduce.properties.NodePreparePropertiesMapper;
import nl.waredingen.graphs.neo.mapreduce.properties.NodePreparePropertiesReducer;
import nl.waredingen.graphs.neo.mapreduce.properties.NodePropertyOutputMapper;
import nl.waredingen.graphs.neo.mapreduce.properties.NodePropertyOutputPartitioner;
import nl.waredingen.graphs.neo.mapreduce.properties.NodePropertyOutputReducer;
import nl.waredingen.graphs.neo.neo4j.Neo4JUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class PureMRNodesAndEdgesJob {

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
		String nodePropertiesPrepareOutput = output +"/nodepropprepared";
		String edgePropertiesPrepareOutput = output +"/edgepropprepared";
		String propertiesOutput = output +"/properties";
		String nodePropertiesOutput = output +"/nodeproperties";
		String edgePropertiesOutput = output +"/edgeproperties";

		try {
			long nrOfNodes = RowNumberJob.run(nodes, numberedNodes, conf);
			long nrOfEdges = RowNumberJob.run(edges, numberedEdges, conf);

			System.out.println("Processing " + nrOfNodes + " nodes and " + nrOfEdges + " edges.");
			conf.set(AbstractMetaData.METADATA_NUMBER_OF_NODES, "" + nrOfNodes);
			conf.set(AbstractMetaData.METADATA_NUMBER_OF_EDGES, "" + nrOfEdges);
			
			conf.setClass(AbstractMetaData.METADATA_CLASS, MetaDataFromConfigImpl.class, MetaData.class);

			conf.setStrings(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_NAMES, "rekening", "integraalklantnummer", "klantnummer", "cddklasse", "individu_organisatie_code", "naam", "postcode", "woonplaats", "label");
			conf.set(MetaDataFromConfigImpl.METADATA_NODE_ID_NAME, "rekening");
			conf.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "rekening", String.class, Object.class);
			conf.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "integraalklantnummer", String.class, Object.class);
			conf.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "klantnummer", String.class, Object.class);
			conf.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "cddklasse", String.class, Object.class);
			conf.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "individu_organisatie_code", String.class, Object.class);
			conf.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "naam", String.class, Object.class);
			conf.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "postcode", String.class, Object.class);
			conf.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "woonplaats", String.class, Object.class);
			conf.setClass(MetaDataFromConfigImpl.METADATA_NODE_PROPERTY_TYPE_PREFIX + "label", String.class, Object.class);

			conf.setStrings(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_NAMES, "from", "to", "netto", "eerste", "laatste", "aantal");
			conf.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "from", String.class, Object.class);
			conf.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "to", String.class, Object.class);
			conf.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "netto", Long.class, Object.class);
			conf.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "eerste", String.class, Object.class);
			conf.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "laatste", String.class, Object.class);
			conf.setClass(MetaDataFromConfigImpl.METADATA_EDGE_PROPERTY_TYPE_PREFIX + "aantal", Long.class, Object.class);

			Job nodePropertiesPrepareJob = new Job(conf, "Prepare node properties job.");
			nodePropertiesPrepareJob.setGroupingComparatorClass(AscLongDescLongKeyGroupingComparator.class);
			nodePropertiesPrepareJob.setSortComparatorClass(AscLongDescLongKeyComparator.class);
			nodePropertiesPrepareJob.setPartitionerClass(AscLongDescLongWritablePartitioner.class);

			nodePropertiesPrepareJob.setMapOutputKeyClass(AscLongDescLongWritable.class);
			nodePropertiesPrepareJob.setMapOutputValueClass(FullNodePropertiesWritable.class);

			nodePropertiesPrepareJob.setMapperClass(NodePreparePropertiesMapper.class);
			nodePropertiesPrepareJob.setInputFormatClass(SequenceFileInputFormat.class);
			FileInputFormat.addInputPath(nodePropertiesPrepareJob, new Path(numberedNodes));

			nodePropertiesPrepareJob.setReducerClass(NodePreparePropertiesReducer.class);
			nodePropertiesPrepareJob.setOutputKeyClass(LongWritable.class);
			nodePropertiesPrepareJob.setOutputValueClass(FullNodePropertiesWritable.class);

			nodePropertiesPrepareJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileOutputFormat.setOutputPath(nodePropertiesPrepareJob, new Path(nodePropertiesPrepareOutput));

			nodePropertiesPrepareJob.setJarByClass(PureMRNodesAndEdgesJob.class);

			nodePropertiesPrepareJob.waitForCompletion(true);

			Job edgePropertiesPrepareJob = new Job(conf, "Prepare edge properties job.");
			edgePropertiesPrepareJob.setGroupingComparatorClass(AscLongDescLongKeyGroupingComparator.class);
			edgePropertiesPrepareJob.setSortComparatorClass(AscLongDescLongKeyComparator.class);
			edgePropertiesPrepareJob.setPartitionerClass(AscLongDescLongWritablePartitioner.class);
			
			edgePropertiesPrepareJob.setMapOutputKeyClass(AscLongDescLongWritable.class);
			edgePropertiesPrepareJob.setMapOutputValueClass(FullEdgePropertiesWritable.class);
			
			edgePropertiesPrepareJob.setMapperClass(EdgePreparePropertiesMapper.class);
			edgePropertiesPrepareJob.setInputFormatClass(SequenceFileInputFormat.class);
			FileInputFormat.addInputPath(edgePropertiesPrepareJob, new Path(numberedEdges));
			
			edgePropertiesPrepareJob.setReducerClass(EdgePreparePropertiesReducer.class);
			edgePropertiesPrepareJob.setOutputKeyClass(LongWritable.class);
			edgePropertiesPrepareJob.setOutputValueClass(FullEdgePropertiesWritable.class);
			
			edgePropertiesPrepareJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileOutputFormat.setOutputPath(edgePropertiesPrepareJob, new Path(edgePropertiesPrepareOutput));
			
			edgePropertiesPrepareJob.setJarByClass(PureMRNodesAndEdgesJob.class);
			
			edgePropertiesPrepareJob.waitForCompletion(true);
			
			Job nodePropertiesOutputJob = new Job(conf, "Output node properties job.");
			nodePropertiesOutputJob.setPartitionerClass(NodePropertyOutputPartitioner.class);
			nodePropertiesOutputJob.setSortComparatorClass(ByteMarkerAndIdComparator.class);
			nodePropertiesOutputJob.setGroupingComparatorClass(IndifferentByteMarkerAndIdComparator.class);

			nodePropertiesOutputJob.setMapOutputKeyClass(ByteMarkerIdPropIdWritable.class);
			nodePropertiesOutputJob.setMapOutputValueClass(NodePropertyOutputCountersAndValueWritable.class);

			nodePropertiesOutputJob.setMapperClass(NodePropertyOutputMapper.class);
			nodePropertiesOutputJob.setInputFormatClass(SequenceFileInputFormat.class);
			FileInputFormat.addInputPath(nodePropertiesOutputJob, new Path(nodePropertiesPrepareOutput));
			

			nodePropertiesOutputJob.setReducerClass(NodePropertyOutputReducer.class);

			FileOutputFormat.setOutputPath(nodePropertiesOutputJob, new Path(nodePropertiesOutput + "/propertystore.db"));
			MultipleOutputs.addNamedOutput(nodePropertiesOutputJob, "nodes", SequenceFileOutputFormat.class, NullWritable.class, BytesWritable.class);
			MultipleOutputs.addNamedOutput(nodePropertiesOutputJob, "props", NewByteBufferOutputFormat.class, NullWritable.class, BytesWritable.class);
			MultipleOutputs.addNamedOutput(nodePropertiesOutputJob, "strings", NewByteBufferOutputFormat.class, NullWritable.class, BytesWritable.class);

			MultipleOutputs.setCountersEnabled(nodePropertiesOutputJob, true);
			
			nodePropertiesOutputJob.setJarByClass(PureMRNodesAndEdgesJob.class);

			nodePropertiesOutputJob.waitForCompletion(true);
			
			long nrOfWrittenStringBlocks = nodePropertiesOutputJob.getCounters().findCounter("org.apache.hadoop.mapreduce.lib.output.MultipleOutputs", "strings.blocks").getValue();
			long nrOfWrittenNodeProperties = nodePropertiesOutputJob.getCounters().findCounter("org.apache.hadoop.mapreduce.lib.output.MultipleOutputs", "node.properties").getValue();

			conf.set(AbstractMetaData.METADATA_NUMBER_OF_NODE_PROPERTIES, "" + nrOfWrittenNodeProperties);

			Job edgePropertiesOutputJob = new Job(conf, "Output edge properties job.");
			edgePropertiesOutputJob.setPartitionerClass(EdgePropertyOutputPartitioner.class);
			edgePropertiesOutputJob.setSortComparatorClass(ByteMarkerAndIdComparator.class);
			edgePropertiesOutputJob.setGroupingComparatorClass(IndifferentByteMarkerAndIdComparator.class);

			edgePropertiesOutputJob.setMapOutputKeyClass(ByteMarkerIdPropIdWritable.class);
			edgePropertiesOutputJob.setMapOutputValueClass(EdgePropertyOutputCountersAndValueWritable.class);

			edgePropertiesOutputJob.setMapperClass(EdgePropertyOutputMapper.class);
			edgePropertiesOutputJob.setInputFormatClass(SequenceFileInputFormat.class);
			FileInputFormat.addInputPath(edgePropertiesOutputJob, new Path(edgePropertiesPrepareOutput));
			

			edgePropertiesOutputJob.setReducerClass(EdgePropertyOutputReducer.class);

			FileOutputFormat.setOutputPath(edgePropertiesOutputJob, new Path(edgePropertiesOutput + "/propertystore.db"));
			MultipleOutputs.addNamedOutput(edgePropertiesOutputJob, "edges", SequenceFileOutputFormat.class, NullWritable.class, BytesWritable.class);
			MultipleOutputs.addNamedOutput(edgePropertiesOutputJob, "props", NewByteBufferOutputFormat.class, NullWritable.class, BytesWritable.class);
			MultipleOutputs.addNamedOutput(edgePropertiesOutputJob, "strings", NewByteBufferOutputFormat.class, NullWritable.class, BytesWritable.class);

			MultipleOutputs.setCountersEnabled(edgePropertiesOutputJob, true);
			
			edgePropertiesOutputJob.setJarByClass(PureMRNodesAndEdgesJob.class);

			edgePropertiesOutputJob.waitForCompletion(true);
			
			nrOfWrittenStringBlocks += edgePropertiesOutputJob.getCounters().findCounter("org.apache.hadoop.mapreduce.lib.output.MultipleOutputs", "strings.blocks").getValue();
			long nrOfWrittenEdgeProperties = edgePropertiesOutputJob.getCounters().findCounter("org.apache.hadoop.mapreduce.lib.output.MultipleOutputs", "edge.properties").getValue();

			Neo4JUtils.writePropertyKeyStore(propertiesOutput, conf);

			Neo4JUtils.writePropertyStoreFooter(propertiesOutput, conf);
			Neo4JUtils.writePropertyStringStoreHeader(propertiesOutput, conf);
			Neo4JUtils.writePropertyStringStoreFooter(propertiesOutput, conf);

			Neo4JUtils.writePropertyIds((nrOfWrittenNodeProperties + nrOfWrittenEdgeProperties), propertiesOutput + "/neostore.propertystore.db", conf);
			Neo4JUtils.writePropertyIds(nrOfWrittenStringBlocks + 1, propertiesOutput + "/neostore.propertystore.db.strings", conf);
			
			Neo4JUtils.writeEmptArrayStore(propertiesOutput, conf);

			Job joinFrom = new Job(conf, "Join from nodes and edges job.");
			joinFrom.setGroupingComparatorClass(NodeKeyGroupingComparator.class);
			joinFrom.setSortComparatorClass(NodeAndEdgeKeyComparator.class);
			joinFrom.setPartitionerClass(NodeAndEdgeKeyPartitioner.class);

			joinFrom.setMapOutputKeyClass(Text.class);
			joinFrom.setMapOutputValueClass(Text.class);
			
			MultipleInputs
					.addInputPath(joinFrom, new Path(nodePropertiesOutput + "/propertystore.db/nodes*"), SequenceFileInputFormat.class, JoinNodesMapper.class);
			MultipleInputs.addInputPath(joinFrom, new Path(edgePropertiesOutput + "/propertystore.db/edges*"), SequenceFileInputFormat.class,
					JoinFromEdgesMapper.class);

			joinFrom.setReducerClass(JoinNodesAndEdgesReducer.class);
			joinFrom.setOutputKeyClass(NullWritable.class);
			joinFrom.setOutputValueClass(Text.class);

			joinFrom.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileOutputFormat.setOutputPath(joinFrom, new Path(temp));

			joinFrom.setJarByClass(PureMRNodesAndEdgesJob.class);

			joinFrom.waitForCompletion(true);

			Job joinTo = new Job(conf, "Join to nodes and edges job.");
			joinTo.setGroupingComparatorClass(NodeKeyGroupingComparator.class);
			joinTo.setSortComparatorClass(NodeAndEdgeKeyComparator.class);
			joinTo.setPartitionerClass(NodeAndEdgeKeyPartitioner.class);

			joinTo.setMapOutputKeyClass(Text.class);
			joinTo.setMapOutputValueClass(Text.class);

			MultipleInputs.addInputPath(joinTo, new Path(nodePropertiesOutput + "/propertystore.db/nodes*"), SequenceFileInputFormat.class, JoinNodesMapper.class);
			MultipleInputs.addInputPath(joinTo, new Path(temp), SequenceFileInputFormat.class, JoinToEdgesMapper.class);

			joinTo.setReducerClass(JoinNodesAndEdgesReducer.class);
			joinTo.setOutputKeyClass(NullWritable.class);
			joinTo.setOutputValueClass(Text.class);

			joinTo.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileOutputFormat.setOutputPath(joinTo, new Path(joined));

			joinTo.setJarByClass(PureMRNodesAndEdgesJob.class);

			joinTo.waitForCompletion(true);

			Job groupJob = new Job(conf, "Group nodes and edges job.");
			groupJob.setGroupingComparatorClass(NodeAndEdgeIdKeyGroupingComparator.class);
			groupJob.setSortComparatorClass(NodeAndEdgeIdKeyComparator.class);
			groupJob.setPartitionerClass(NodeAndEdgeIdKeyPartitioner.class);

			groupJob.setMapOutputKeyClass(NodeEdgeIdWritable.class);
			groupJob.setMapOutputValueClass(EdgeWritable.class);

			groupJob.setMapperClass(GroupNodesAndEdgesMapper.class);
			groupJob.setInputFormatClass(SequenceFileInputFormat.class);
			FileInputFormat.addInputPath(groupJob, new Path(joined));

			groupJob.setReducerClass(GroupNodesAndEdgesReducer.class);
			groupJob.setOutputKeyClass(NullWritable.class);
			groupJob.setOutputValueClass(NodeEdgeWritable.class);

			groupJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileOutputFormat.setOutputPath(groupJob, new Path(grouped));

			groupJob.setJarByClass(PureMRNodesAndEdgesJob.class);

			groupJob.waitForCompletion(true);

			Job nodeOutputJob = new Job(conf, "Output nodes job.");
			nodeOutputJob.setPartitionerClass(NodeOutputRownumPartitioner.class);

			nodeOutputJob.setMapOutputKeyClass(LongWritable.class);
			nodeOutputJob.setMapOutputValueClass(EdgeIdPropIdWritable.class);

			nodeOutputJob.setMapperClass(NodeOutputMapper.class);
			nodeOutputJob.setInputFormatClass(SequenceFileInputFormat.class);
			FileInputFormat.addInputPath(nodeOutputJob, new Path(grouped));

			nodeOutputJob.setReducerClass(NodeOutputReducer.class);
//			nodeOutputJob.setReducerClass(NodeOutputAsTextReducer.class);
			nodeOutputJob.setOutputKeyClass(NullWritable.class);
			nodeOutputJob.setOutputValueClass(BytesWritable.class);
//			nodeOutputJob.setOutputValueClass(Text.class);

			nodeOutputJob.setOutputFormatClass(NewByteBufferOutputFormat.class);
//			nodeOutputJob.setOutputFormatClass(TextOutputFormat.class);
			FileOutputFormat.setOutputPath(nodeOutputJob, new Path(nodesOutput));

			nodeOutputJob.setJarByClass(PureMRNodesAndEdgesJob.class);

			nodeOutputJob.waitForCompletion(true);

			Job edgeSurroundJob = new Job(conf, "Determine surrounding edges job.");
			edgeSurroundJob.setGroupingComparatorClass(AscLongDescLongKeyGroupingComparator.class);
			edgeSurroundJob.setSortComparatorClass(AscLongDescLongKeyComparator.class);
			edgeSurroundJob.setPartitionerClass(AscLongDescLongWritablePartitioner.class);

			edgeSurroundJob.setMapOutputKeyClass(AscLongDescLongWritable.class);
			edgeSurroundJob.setMapOutputValueClass(EdgeWritable.class);

			edgeSurroundJob.setMapperClass(EdgeSurroundMapper.class);
			edgeSurroundJob.setInputFormatClass(SequenceFileInputFormat.class);
			FileInputFormat.addInputPath(edgeSurroundJob, new Path(grouped));

			edgeSurroundJob.setReducerClass(EdgeSurroundReducer.class);
			edgeSurroundJob.setOutputKeyClass(NullWritable.class);
			edgeSurroundJob.setOutputValueClass(SurroundingEdgeWritable.class);

			edgeSurroundJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileOutputFormat.setOutputPath(edgeSurroundJob, new Path(surrounding));

			edgeSurroundJob.setJarByClass(PureMRNodesAndEdgesJob.class);

			edgeSurroundJob.waitForCompletion(true);

			Job joinSurroundJob = new Job(conf, "Join surrounding edges job.");
			joinSurroundJob.setGroupingComparatorClass(EdgeWritableKeyGroupingComparator.class);
			joinSurroundJob.setSortComparatorClass(EdgeWritableKeyComparator.class);

			joinSurroundJob.setMapOutputKeyClass(EdgeWritable.class);
			joinSurroundJob.setMapOutputValueClass(SurroundingEdgeWritable.class);

			joinSurroundJob.setMapperClass(JoinSurroundingEdgesMapper.class);
			joinSurroundJob.setInputFormatClass(SequenceFileInputFormat.class);
			FileInputFormat.addInputPath(joinSurroundJob, new Path(surrounding));

			joinSurroundJob.setReducerClass(JoinSurroundingEdgesReducer.class);
			joinSurroundJob.setOutputKeyClass(NullWritable.class);
			joinSurroundJob.setOutputValueClass(DoubleSurroundingEdgeWritable.class);

			joinSurroundJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileOutputFormat.setOutputPath(joinSurroundJob, new Path(joinededges));

			joinSurroundJob.setJarByClass(PureMRNodesAndEdgesJob.class);

			joinSurroundJob.waitForCompletion(true);

			Job edgeOutputJob = new Job(conf, "Output edges job.");
			edgeOutputJob.setPartitionerClass(EdgeOutputRownumPartitioner.class);

			edgeOutputJob.setMapOutputKeyClass(LongWritable.class);
			edgeOutputJob.setMapOutputValueClass(FullEdgeWritable.class);

			edgeOutputJob.setMapperClass(EdgeOutputMapper.class);
			edgeOutputJob.setInputFormatClass(SequenceFileInputFormat.class);
			FileInputFormat.addInputPath(edgeOutputJob, new Path(joinededges));

			edgeOutputJob.setReducerClass(EdgeOutputReducer.class);
//			edgeOutputJob.setReducerClass(EdgeOutputAsTextReducer.class);
			edgeOutputJob.setOutputKeyClass(NullWritable.class);
			edgeOutputJob.setOutputValueClass(BytesWritable.class);
//			edgeOutputJob.setOutputValueClass(Text.class);

			edgeOutputJob.setOutputFormatClass(NewByteBufferOutputFormat.class);
//			edgeOutputJob.setOutputFormatClass(TextOutputFormat.class);
			FileOutputFormat.setOutputPath(edgeOutputJob, new Path(edgesOutput));

			edgeOutputJob.setJarByClass(PureMRNodesAndEdgesJob.class);

			edgeOutputJob.waitForCompletion(true);

			Neo4JUtils.writeNeostore(output, conf);
			Neo4JUtils.writeNodeIds(nrOfNodes, output, conf);
			Neo4JUtils.writeEdgeIds(nrOfEdges, output, conf);
			Neo4JUtils.writeSingleTypeStore("TRANSFER_TO", output, conf);


		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace(System.err);
			return 1;
		}

		return 0;
	}
}