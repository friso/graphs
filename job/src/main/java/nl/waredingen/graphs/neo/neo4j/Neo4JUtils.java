package nl.waredingen.graphs.neo.neo4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.nioneo.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.nioneo.store.AbstractNameStore;
import org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicStringStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeStore;
import org.neo4j.kernel.impl.nioneo.store.StoreId;

public class Neo4JUtils {

	public static void writeNeostore(String output, Configuration conf) throws IOException {
		String neoOutput = output + "/neostore";
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream ndos = fs.create(new Path(neoOutput));

		StoreId store = new StoreId();
		ByteBuffer buffer = ByteBuffer.allocate(54);
		buffer.put(Record.IN_USE.byteValue()).putLong(store.getCreationTime());
		buffer.put(Record.IN_USE.byteValue()).putLong(store.getRandomId());
		buffer.put(Record.IN_USE.byteValue()).putLong(0);
		buffer.put(Record.IN_USE.byteValue()).putLong(1);
		buffer.put(Record.IN_USE.byteValue()).putLong(store.getStoreVersion());
		buffer.put(Record.IN_USE.byteValue()).putLong(-1);
		buffer.flip();
		ndos.write(buffer.array());
		
		String type = NeoStore.TYPE_DESCRIPTOR + " " + CommonAbstractStore.ALL_STORES_VERSION;
		ByteBuffer tailBuffer = ByteBuffer.allocate(type.length());
		tailBuffer.put(type.getBytes(Charset.forName("UTF-8")));
		tailBuffer.flip();
		ndos.write(tailBuffer.array());
		ndos.close();
		
		writeNeostoreId(output, conf);
	}

	private static void writeNeostoreId(String output, Configuration conf) throws IOException {
		String storeIdOutput = output + "/neostore.id";
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream ndos = fs.create(new Path(storeIdOutput));

		ByteBuffer nodeBuffer = ByteBuffer.allocate(9);
		nodeBuffer.put((byte) 0).putLong(6);
		nodeBuffer.flip();
		ndos.write(nodeBuffer.array());

		ndos.close();
	}

	public static void writeNodeIds(long lastNodeId, String output, Configuration conf) throws IOException {
		String nodeIdsOutput = output + "/neostore.nodestore.db.id";
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream ndos = fs.create(new Path(nodeIdsOutput));

		ByteBuffer nodeBuffer = ByteBuffer.allocate(9);
		nodeBuffer.put((byte) 0).putLong(lastNodeId);
		nodeBuffer.flip();
		ndos.write(nodeBuffer.array());

		ndos.close();
	}

	public static void writeEdgeIds(long lastEdgeId, String output, Configuration conf) throws IOException {
		String edgeIdsOutput = output + "/neostore.relationshipstore.db.id";
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream edos = fs.create(new Path(edgeIdsOutput));

		ByteBuffer edgeBuffer = ByteBuffer.allocate(9);
		edgeBuffer.put((byte) 0).putLong(lastEdgeId);
		edgeBuffer.flip();
		edos.write(edgeBuffer.array());

		edos.close();

	}

	public static void writeSingleTypeStore(String typeName, String output, Configuration conf) throws IOException {
		String typesOutput = output + "/neostore.relationshiptypestore.db";
		String typeNamesOutput = output + "/neostore.relationshiptypestore.db.names";
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream tdos = fs.create(new Path(typesOutput));

		tdos.write(getTypeRecordAsByteArray(1));

		String type = RelationshipTypeStore.TYPE_DESCRIPTOR + " " + CommonAbstractStore.ALL_STORES_VERSION;
		ByteBuffer tailBuffer = ByteBuffer.allocate(type.length());
		tailBuffer.put(type.getBytes(Charset.forName("UTF-8")));
		tailBuffer.flip();
		tdos.write(tailBuffer.array());

		tdos.close();

		writeTypeIds(1L, output, conf);

		FSDataOutputStream tndos = fs.create(new Path(typeNamesOutput));

		type = DynamicStringStore.VERSION;
		int blockSize = AbstractNameStore.NAME_STORE_BLOCK_SIZE + AbstractDynamicStore.BLOCK_HEADER_SIZE;
		
		ByteBuffer headBuffer = ByteBuffer.allocate(blockSize);
		headBuffer.putInt(blockSize);
//		headBuffer.flip();
		tndos.write(headBuffer.array());

		tndos.write(getNameRecordAsByteArray(1L, typeName.getBytes()));

		type = DynamicStringStore.TYPE_DESCRIPTOR + " " + CommonAbstractStore.ALL_STORES_VERSION;
		tailBuffer = ByteBuffer.allocate(type.length());
		tailBuffer.put(type.getBytes(Charset.forName("UTF-8")));
		tailBuffer.flip();
		tndos.write(tailBuffer.array());

		tndos.close();

		writeTypeNameIds(1L, output, conf);
	}

	public static void writeTypeIds(long lastTypeId, String output, Configuration conf) throws IOException {
		String typeIdsOutput = output + "/neostore.relationshiptypestore.db.id";
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream tdos = fs.create(new Path(typeIdsOutput));

		ByteBuffer typeBuffer = ByteBuffer.allocate(9);
		typeBuffer.put((byte) 0).putLong(lastTypeId);
		typeBuffer.flip();
		tdos.write(typeBuffer.array());

		tdos.close();

	}

	public static void writeTypeNameIds(long lastTypeNameId, String output, Configuration conf) throws IOException {
		String typeNamesOutput = output + "/neostore.relationshiptypestore.db.names.id";
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream tdos = fs.create(new Path(typeNamesOutput));

		ByteBuffer typeNameBuffer = ByteBuffer.allocate(9);
		typeNameBuffer.put((byte) 0).putLong(lastTypeNameId);
		typeNameBuffer.flip();
		tdos.write(typeNameBuffer.array());

		tdos.close();

	}

	public static byte[] getNodeAsByteArray(long id, long relnum, long prop) {
		ByteBuffer buffer = ByteBuffer.allocate(9);

		NodeRecord nr = new NodeRecord(id, relnum, prop);
		nr.setInUse(true);
		nr.setCreated();

		// LOG.debug(nr.toString());

		long nextRel = nr.getNextRel();
		long nextProp = nr.getNextProp();

		short relModifier = nextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0
				: (short) ((nextRel & 0x700000000L) >> 31);
		short propModifier = nextProp == Record.NO_NEXT_PROPERTY.intValue() ? 0
				: (short) ((nextProp & 0xF00000000L) >> 28);

		// [ , x] in use bit
		// [ ,xxx ] higher bits for rel id
		// [xxxx, ] higher bits for prop id
		short inUseUnsignedByte = (nr.inUse() ? Record.IN_USE : Record.NOT_IN_USE).byteValue();
		inUseUnsignedByte = (short) (inUseUnsignedByte | relModifier | propModifier);

		buffer.put((byte) inUseUnsignedByte).putInt((int) nextRel).putInt((int) nextProp);
		return buffer.array();

	}

	public static byte[] getEdgeAsByteArray(long relnum, long fromnode, long tonode, int type, long fromprev, long fromnext,
			long toprev, long tonext, long prop) {
		
		//TODO adjust nodeid's to match 1 more b/o root node!!
		// now doing it here
		long from = fromnode +1L;
		long to = tonode +1L;
		ByteBuffer buffer = ByteBuffer.allocate(33);

		RelationshipRecord rr = new RelationshipRecord(relnum, from, to, type);
		rr.setInUse(true);
		rr.setCreated();

		// LOG.debug(rr.toString());

		short fromMod = (short) ((from & 0x700000000L) >> 31);
		long toMod = (to & 0x700000000L) >> 4;
		long fromPrevRelMod = fromprev == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (fromprev & 0x700000000L) >> 7;
		long fromNextRelMod = fromnext == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (fromnext & 0x700000000L) >> 10;
		long toPrevRelMod = toprev == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (toprev & 0x700000000L) >> 13;
		long toNextRelMod = tonext == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (tonext & 0x700000000L) >> 16;
		long propMod = prop == Record.NO_NEXT_PROPERTY.intValue() ? 0 : (prop & 0xF00000000L) >> 28;

		// [ , x] in use flag
		// [ ,xxx ] first node high order bits
		// [xxxx, ] next prop high order bits
		short inUseUnsignedByte = (short) ((rr.inUse() ? Record.IN_USE : Record.NOT_IN_USE).byteValue() | fromMod | propMod);

		// [ xxx, ][ , ][ , ][ , ] second node high order bits, 0x70000000
		// [ ,xxx ][ , ][ , ][ , ] first prev rel high order bits, 0xE000000
		// [ , x][xx , ][ , ][ , ] first next rel high order bits, 0x1C00000
		// [ , ][ xx,x ][ , ][ , ] second prev rel high order bits, 0x380000
		// [ , ][ , xxx][ , ][ , ] second next rel high order bits, 0x70000
		// [ , ][ , ][xxxx,xxxx][xxxx,xxxx] type
		int typeInt = (int) (rr.getType() | toMod | fromPrevRelMod | fromNextRelMod | toPrevRelMod | toNextRelMod);

		buffer.put((byte) inUseUnsignedByte).putInt((int) from).putInt((int) to).putInt(typeInt).putInt((int) fromprev)
				.putInt((int) fromnext).putInt((int) toprev).putInt((int) tonext).putInt((int) prop);
		return buffer.array();

	}

	public static byte[] getTypeRecordAsByteArray(int id) {
		ByteBuffer buffer = ByteBuffer.allocate(5);
		buffer.put(Record.IN_USE.byteValue()).putInt(id);
		buffer.flip();

		return buffer.array();
	}

	public static byte[] getNameRecordAsByteArray(long id, byte[] name) {
		List<DynamicRecord> recordList = new LinkedList<DynamicRecord>();

		long nextBlock = id;
		int srcOffset = 0;
		int dataSize = 30 /* blocksize */- 8 /* headersize */;
		do {
			DynamicRecord record = new DynamicRecord(nextBlock);
			record.setCreated();
			record.setInUse(true);
			if (name.length - srcOffset > dataSize) {
				byte data[] = new byte[dataSize];
				System.arraycopy(name, srcOffset, data, 0, dataSize);
				record.setData(data);
				record.setNextBlock(nextBlock++);
				srcOffset += dataSize;
			} else {
				byte data[] = new byte[name.length - srcOffset];
				System.arraycopy(name, srcOffset, data, 0, data.length);
				record.setData(data);
				nextBlock = Record.NO_NEXT_BLOCK.intValue();
				record.setNextBlock(nextBlock);
			}
			recordList.add(record);
		} while (nextBlock != Record.NO_NEXT_BLOCK.intValue());

		ByteBuffer buffer =ByteBuffer.allocate(30*recordList.size());
		
		for (DynamicRecord dynamicRecord : recordList) {

			long nextProp = dynamicRecord.getNextBlock();
			int nextModifier = nextProp == Record.NO_NEXT_BLOCK.intValue() ? 0 : (int) ((nextProp & 0xF00000000L) >> 8);
			nextModifier |= (Record.IN_USE.byteValue() << 28);

			/*
			 * 
			 * [ x, ][ , ][ , ][ , ] inUse [ ,xxxx][ , ][ , ][ , ] high next
			 * block bits [ , ][xxxx,xxxx][xxxx,xxxx][xxxx,xxxx] nr of bytes
			 */
			int mostlyNrOfBytesInt = dynamicRecord.getLength();
			assert mostlyNrOfBytesInt < (1 << 24) - 1;

			mostlyNrOfBytesInt |= nextModifier;

			buffer.putInt(mostlyNrOfBytesInt).putInt((int) nextProp);
			if (!dynamicRecord.isLight()) {
				buffer.put(dynamicRecord.getData());
			}
		}
		
		buffer.flip();
		return buffer.array();
	}
}
