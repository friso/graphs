package nl.waredingen.graphs.importer;

import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.LuceneBatchInserterIndexProvider;

public class Neo4jImportJob {
	private final static String SPLIT_STRING = "\t";
	
	private static enum ConnectionTypes implements RelationshipType {
		LINKED_TO;
	}
	
	private final File nodesFile;
	private final File edgesFile;
	private final File output;
	private final String[] nodeFields;
	private final String[] edgeFields;
	
	private int progressEvery = 1000;
	private long globalCount;
	private static final String[] MAGNITUDES = {"K", "0K", "00K", "M", "0M", "00M", "B", "0B"};
	
	public Neo4jImportJob(String nodes, String edges, String outputDir, String[] nodeFields, String[] edgeFields) {
		this.output = new File(outputDir);
		this.nodesFile = new File(nodes);
		this.edgesFile = new File(edges);
		this.nodeFields = nodeFields;
		this.edgeFields = edgeFields;
		
		this.globalCount = 0;
	}
	
	public int runImport() {
		try {
			BatchInserter db;
			
			if (output.exists()) {
				System.err.println("Batch import output dir already exists!");
				return 1;
			} else {
				output.mkdirs();
			}
			
			System.out.println("Opening DB and indexes...");
			db = BatchInserters.inserter(output.getAbsolutePath(), getConfig());
			
			BatchInserterIndexProvider provider = new LuceneBatchInserterIndexProvider(db);
			BatchInserterIndex index = provider.nodeIndex("allnodes", stringMap(
					"type", "fulltext",
					IndexManager.PROVIDER, "lucene"
					));
			
			Map<Long,Long> fileNodeIdsToNodeIds = new HashMap<Long, Long>(2000000);
			
			System.out.print("Importing nodes...");
			long numNodes = importNodes(db, index, fileNodeIdsToNodeIds);
			System.out.println(" done (" + numNodes + " nodes)");
			
			resetProgress();
			System.out.print("Importing edges...");
			long numEdges = importEdges(db, index, fileNodeIdsToNodeIds);
			System.out.println(" done (" + numEdges + " edges)");
			
			System.out.print("Shutting down indexes and DB...");
			provider.shutdown();
			db.shutdown();
		} catch(Exception e) {
			System.err.println("Exception during import:");
			e.printStackTrace(System.err);
			return 1;
		}
		
		return 0;
	}
	
	private long importNodes(BatchInserter db, BatchInserterIndex index, Map<Long, Long> fileIdsToNodeIds) throws IOException {
		long nodeCount = 0;
		
		BufferedReader reader = new BufferedReader(new FileReader(nodesFile));
		Object[] properties = new Object[nodeFields.length * 2];
		for (int c = 0; c < nodeFields.length; c++) {
			properties[c * 2] = nodeFields[c];
		}
		
		String line;
		while ((line = reader.readLine()) != null) {
			String[] parts = line.split(SPLIT_STRING);
			
			long fileNodeId = Long.parseLong(parts[0]);
			long nodeId;
			for (int c = 0; c < nodeFields.length; c++) {
				properties[1 + c * 2] = objectFromProperty(parts[c + 1].trim());
			}
			nodeId = db.createNode(map(properties));
			index.add(nodeId, map(properties));
			
			fileIdsToNodeIds.put(fileNodeId, nodeId);
			
			if (progress()) {
				index.flush();
			}
			
			nodeCount++;
		}
		index.flush();
		
		return nodeCount;
	}
	
	private long importEdges(BatchInserter db, BatchInserterIndex index, Map<Long, Long> accountNodeIds) throws IOException {
		long edgeCount = 0;
		
		BufferedReader reader = new BufferedReader(new FileReader(edgesFile));
		
		Object[] properties = new Object[edgeFields.length * 2];
		for (int c = 0; c < edgeFields.length; c++) {
			properties[c * 2] = edgeFields[c];
		}
		
		String line;
		while ((line = reader.readLine()) != null) {
			String[] parts = line.split(SPLIT_STRING);
			
			long fromAccountId = Long.parseLong(parts[0]);
			long toAccountId = Long.parseLong(parts[1]);
			if (accountNodeIds.containsKey(fromAccountId) && accountNodeIds.containsKey(toAccountId)) {
				
				for (int c = 0; c < edgeFields.length; c++) {
					properties[1 + c * 2] = objectFromProperty(parts[c + 1].trim());
				}
				
				db.createRelationship(accountNodeIds.get(fromAccountId), accountNodeIds.get(toAccountId), ConnectionTypes.LINKED_TO, map(properties));
				edgeCount++;
			}
			progress();
		}
		
		return edgeCount;
	}
	
	private static Pattern fpNumber = Pattern.compile("^[-]?\\d+[\\.]\\d+$");
	private static Pattern intNumber = Pattern.compile("^[-]?\\d+$");
	private static Pattern hexNumber = Pattern.compile("^0x[0-9A-Fa-f]+$");
	
	private static Object objectFromProperty(String value) {
		if (fpNumber.matcher(value).matches()) {
			return Double.parseDouble(value);
		} else if (intNumber.matcher(value).matches()) {
			return Long.parseLong(value);
		} else if (hexNumber.matcher(value).matches()) {
			return Long.parseLong(value.substring(2), 16);
		} else {
			return value;
		}
	}
	
	public static void main(String[] args) {
		System.out.println(objectFromProperty("1234").getClass());
	}

	private Map<String, String> getConfig() {
        if (new File("batch.properties").exists()) {
			return MapUtil.loadStrictly(new File("batch.properties"));
        } else {
            return stringMap(
                    "dump_configuration", "true",
                    "cache_type", "none",
                    "neostore.propertystore.db.index.keys.mapped_memory", "5M",
                    "neostore.propertystore.db.index.mapped_memory", "5M",
                    "neostore.nodestore.db.mapped_memory", "200M",
                    "neostore.relationshipstore.db.mapped_memory", "1000M",
                    "neostore.propertystore.db.mapped_memory", "1000M",
                    "neostore.propertystore.db.strings.mapped_memory", "100M");
        }
    }
	
	private boolean progress() {
		if (++globalCount % progressEvery == 0) {
			int scale = (int) Math.log10(globalCount);
			int magnitude = (int) Math.pow(10, scale);
			progressEvery = magnitude;
			System.out.print((globalCount / magnitude) + MAGNITUDES[scale - 3]);
			return true;
		} else if (globalCount % (progressEvery / 5) == 0) {
			System.out.print('.');
		}
		return false;
	}
	
	private void resetProgress() {
		globalCount = 0;
		progressEvery = 1000;
	}
	
	public static int run(String nodes, String edges, String db, String[] nodeFields, String[] edgeFields) {
		Neo4jImportJob importer = new Neo4jImportJob(nodes, edges, db, nodeFields, edgeFields);
		return importer.runImport();
	}
}
