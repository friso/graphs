package nl.waredingen.graphs.neo.neo4j;

import static java.lang.System.arraycopy;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import nl.waredingen.graphs.neo.mapreduce.input.AbstractMetaData;
import nl.waredingen.graphs.neo.mapreduce.input.HardCodedMetaDataImpl;
import nl.waredingen.graphs.neo.mapreduce.input.MetaData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.nioneo.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.nioneo.store.AbstractNameStore;
import org.neo4j.kernel.impl.nioneo.store.CommonAbstractStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicArrayStore;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicStringStore;
import org.neo4j.kernel.impl.nioneo.store.LongerShortString;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeStore;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.util.Bits;

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
		// headBuffer.flip();
		tndos.write(headBuffer.array());

		tndos.write(getStringRecordAsByteArray(1L, typeName.getBytes(), blockSize));

		type = DynamicStringStore.TYPE_DESCRIPTOR + " " + CommonAbstractStore.ALL_STORES_VERSION;
		tailBuffer = ByteBuffer.allocate(type.length());
		tailBuffer.put(type.getBytes(Charset.forName("UTF-8")));
		tailBuffer.flip();
		tndos.write(tailBuffer.array());

		tndos.close();

		writeTypeNameIds(2L, output, conf);
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

	public static byte[] getEdgeAsByteArray(long relnum, long fromnode, long tonode, int type, long fromprev,
			long fromnext, long toprev, long tonext, long prop) {

		// TODO adjust nodeid's to match 1 more b/o root node!!
		// now doing it here
		long from = fromnode + 1L;
		long to = tonode + 1L;
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

	public static byte[] getStringRecordAsByteArray(long id, byte[] name, int blockSize) {
		List<DynamicRecord> recordList = new LinkedList<DynamicRecord>();

		long nextBlock = id;
		int srcOffset = 0;
		int dataSize = blockSize - DynamicStringStore.BLOCK_HEADER_SIZE;
		do {
			DynamicRecord record = new DynamicRecord(nextBlock);
			record.setCreated();
			record.setInUse(true);
			if (name.length - srcOffset > dataSize) {
				byte data[] = new byte[dataSize];
				System.arraycopy(name, srcOffset, data, 0, dataSize);
				record.setData(data);
				record.setNextBlock(++nextBlock);
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

		return getDynamicRecordsAsByteArray(recordList, blockSize);
	}

	public static byte[] getDynamicRecordsAsByteArray(List<DynamicRecord> recordList, int blockSize) {
		ByteBuffer buffer = ByteBuffer.allocate(blockSize * recordList.size());

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

	public static void encodeValue(PropertyBlock block, int keyId, Object value, long stringBlockId) {
		if (value instanceof String) { // Try short string first, i.e. inlined
										// in the property block
			String string = (String) value;
			if (LongerShortString.encode(keyId, string, block, PropertyType.getPayloadSize()))
				return;

			// Fall back to dynamic string store
			//long stringBlockId = 0L;// nextStringBlockId();
			setSingleBlockValue(block, keyId, PropertyType.STRING, stringBlockId);
			byte[] encodedString = encodeString(string);
			Collection<DynamicRecord> valueRecords = allocateRecords(stringBlockId, encodedString);
			for (DynamicRecord valueRecord : valueRecords) {
				valueRecord.setType(PropertyType.STRING.intValue());
				block.addValueRecord(valueRecord);
			}
		} else if (value instanceof Integer)
			setSingleBlockValue(block, keyId, PropertyType.INT, ((Integer) value).longValue());
		else if (value instanceof Boolean)
			setSingleBlockValue(block, keyId, PropertyType.BOOL, (((Boolean) value).booleanValue() ? 1L : 0L));
		else if (value instanceof Float)
			setSingleBlockValue(block, keyId, PropertyType.FLOAT, Float.floatToRawIntBits(((Float) value).floatValue()));
		else if (value instanceof Long) {
			long keyAndType = keyId | (((long) PropertyType.LONG.intValue()) << 24);
			if (ShortArray.LONG.getRequiredBits((Long) value) <= 35) { // We
																		// only
																		// need
																		// one
																		// block
																		// for
																		// this
																		// value,
																		// special
																		// layout
																		// compared
																		// to,
																		// say,
																		// an
																		// integer
				block.setSingleBlock(keyAndType | (1L << 28) | (((Long) value).longValue() << 29));
			} else { // We need two blocks for this value
				block.setValueBlocks(new long[] { keyAndType, ((Long) value).longValue() });
			}
		} else if (value instanceof Double)
			block.setValueBlocks(new long[] { keyId | (((long) PropertyType.DOUBLE.intValue()) << 24),
					Double.doubleToRawLongBits(((Double) value).doubleValue()) });
		else if (value instanceof Byte)
			setSingleBlockValue(block, keyId, PropertyType.BYTE, ((Byte) value).longValue());
		else if (value instanceof Character)
			setSingleBlockValue(block, keyId, PropertyType.CHAR, ((Character) value).charValue());
		else if (value instanceof Short)
			setSingleBlockValue(block, keyId, PropertyType.SHORT, ((Short) value).longValue());
		else if (value.getClass().isArray()) { // Try short array first, i.e.
												// inlined in the property block
			if (ShortArray.encode(keyId, value, block, PropertyType.getPayloadSize()))
				return;

			// Fall back to dynamic array store
			long arrayBlockId = 0L;// nextArrayBlockId();
			setSingleBlockValue(block, keyId, PropertyType.ARRAY, arrayBlockId);
			Collection<DynamicRecord> arrayRecords = allocateArrayRecords(arrayBlockId, value);
			for (DynamicRecord valueRecord : arrayRecords) {
				valueRecord.setType(PropertyType.ARRAY.intValue());
				block.addValueRecord(valueRecord);
			}
		} else {
			throw new IllegalArgumentException("Unknown property type on: " + value + ", " + value.getClass());
		}
	}

	private static void setSingleBlockValue(PropertyBlock block, int keyId, PropertyType type, long longValue) {
		block.setSingleBlock(keyId | (((long) type.intValue()) << 24) | (longValue << 28));
	}

	private static byte[] encodeString(String string) {
		return UTF8.encode(string);
	}

	private static Collection<DynamicRecord> allocateRecords(long startBlock, byte src[]) {

		List<DynamicRecord> recordList = new LinkedList<DynamicRecord>();
		long nextBlock = startBlock;
		int srcOffset = 0;
		int dataSize = 128 - AbstractDynamicStore.BLOCK_HEADER_SIZE;
		do {
			DynamicRecord record = new DynamicRecord(nextBlock);
			record.setCreated();
			record.setInUse(true);
			if (src.length - srcOffset > dataSize) {
				byte data[] = new byte[dataSize];
				System.arraycopy(src, srcOffset, data, 0, dataSize);
				record.setData(data);
				nextBlock++;// = nextBlockId();
				record.setNextBlock(nextBlock);
				srcOffset += dataSize;
			} else {
				byte data[] = new byte[src.length - srcOffset];
				System.arraycopy(src, srcOffset, data, 0, data.length);
				record.setData(data);
				nextBlock = Record.NO_NEXT_BLOCK.intValue();
				record.setNextBlock(nextBlock);
			}
			recordList.add(record);
		} while (nextBlock != Record.NO_NEXT_BLOCK.intValue());
		return recordList;
	}

	private static Collection<DynamicRecord> allocateFromNumbers(long startBlock, Object array) {
		Class<?> componentType = array.getClass().getComponentType();
		boolean isPrimitiveByteArray = componentType.equals(Byte.TYPE);
		boolean isByteArray = componentType.equals(Byte.class) || isPrimitiveByteArray;
		byte[] bytes = null;
		ShortArray type = ShortArray.typeOf(array);
		if (type == null)
			throw new IllegalArgumentException(array + " not a valid array type.");

		int arrayLength = Array.getLength(array);
		int requiredBits = isByteArray ? Byte.SIZE : type.calculateRequiredBitsForArray(array, arrayLength);
		int totalBits = requiredBits * arrayLength;
		int numberOfBytes = (totalBits - 1) / 8 + 1;
		int bitsUsedInLastByte = totalBits % 8;
		bitsUsedInLastByte = bitsUsedInLastByte == 0 ? 8 : bitsUsedInLastByte;
		numberOfBytes += 3;// DynamicArrayStore.NUMBER_HEADER_SIZE; // type +
							// rest + requiredBits header. TODO no need to use
							// full bytes
		int length = arrayLength;
		if (isByteArray) {
			bytes = new byte[3/* DynamicArrayStore.NUMBER_HEADER_SIZE */+ length];
			bytes[0] = (byte) type.intValue();
			bytes[1] = (byte) bitsUsedInLastByte;
			bytes[2] = (byte) requiredBits;
			if (isPrimitiveByteArray)
				arraycopy((byte[]) array, 0, bytes, 3/*
													 * DynamicArrayStore.
													 * NUMBER_HEADER_SIZE
													 */, length);
			else {
				Byte[] source = (Byte[]) array;
				for (int i = 0; i < source.length; i++)
					bytes[3/* DynamicArrayStore.NUMBER_HEADER_SIZE */+ i] = source[i].byteValue();
			}
		} else {
			Bits bits = Bits.bits(numberOfBytes);
			bits.put((byte) type.intValue());
			bits.put((byte) bitsUsedInLastByte);
			bits.put((byte) requiredBits);
			type.writeAll(array, length, requiredBits, bits);
			bytes = bits.asBytes();
		}
		return allocateRecords(startBlock, bytes);
	}

	private static Collection<DynamicRecord> allocateFromString(long startBlock, String[] array) {
		List<byte[]> stringsAsBytes = new ArrayList<byte[]>();
		int totalBytesRequired = 5;// DynamicArrayStore.STRING_HEADER_SIZE; //
									// 1b type + 4b array length
		for (String string : array) {
			byte[] bytes = PropertyStore.encodeString(string);
			stringsAsBytes.add(bytes);
			totalBytesRequired += 4/* byte[].length */+ bytes.length;
		}

		ByteBuffer buf = ByteBuffer.allocate(totalBytesRequired);
		buf.put(PropertyType.STRING.byteValue());
		buf.putInt(array.length);
		for (byte[] stringAsBytes : stringsAsBytes) {
			buf.putInt(stringAsBytes.length);
			buf.put(stringAsBytes);
		}
		return allocateRecords(startBlock, buf.array());
	}

	private static Collection<DynamicRecord> allocateArrayRecords(long startBlock, Object array) {
		if (!array.getClass().isArray()) {
			throw new IllegalArgumentException(array + " not an array");
		}

		Class<?> type = array.getClass().getComponentType();
		if (type.equals(String.class)) {
			return allocateFromString(startBlock, (String[]) array);
		} else {
			return allocateFromNumbers(startBlock, array);
		}
	}

	public static byte[] getPropertyReferenceAsByteArray(PropertyRecord record) {
		ByteBuffer buffer = ByteBuffer.allocate(41);

		// Set up the record header
		short prevModifier = record.getPrevProp() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (short) ((record
				.getPrevProp() & 0xF00000000L) >> 28);
		short nextModifier = record.getNextProp() == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (short) ((record
				.getNextProp() & 0xF00000000L) >> 32);
		byte modifiers = (byte) (prevModifier | nextModifier);
		/*
		 * [pppp,nnnn] previous, next high bits
		 */
		buffer.put(modifiers);
		buffer.putInt((int) record.getPrevProp()).putInt((int) record.getNextProp());

		// Then go through the blocks
		int longsAppended = 0; // For marking the end of blocks
		for (PropertyBlock block : record.getPropertyBlocks()) {
			long[] propBlockValues = block.getValueBlocks();
			for (int k = 0; k < propBlockValues.length; k++) {
				buffer.putLong(propBlockValues[k]);
			}

			longsAppended += propBlockValues.length;
		}
		if (longsAppended < PropertyType.getPayloadSizeLongs()) {
			buffer.putLong(0);
		}
		buffer.flip();
		return buffer.array();
	}

	public static void writePropertyKeyStore(String output, Configuration conf) throws IOException {

		MetaData metaData = getMetaData(conf);
		
		String indexOutput = output + "/neostore.propertystore.db.index";
		String keysOutput = output + "/neostore.propertystore.db.index.keys";

		int lastUsedIndexId = 0;
		int nextKeyBlockId = 1;
		int blockSize = AbstractNameStore.NAME_STORE_BLOCK_SIZE + AbstractDynamicStore.BLOCK_HEADER_SIZE;
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream idos = fs.create(new Path(indexOutput));
		FSDataOutputStream kdos = fs.create(new Path(keysOutput));

        int endHeaderSize = blockSize;
        ByteBuffer buffer = ByteBuffer.allocate( endHeaderSize );
        buffer.putInt( blockSize );
        kdos.write(buffer.array());

        for ( String propertyName : metaData.getNodeTypeNames()) {
		
			nextKeyBlockId += writePropertyKey(nextKeyBlockId, blockSize, idos, kdos, propertyName);
			lastUsedIndexId++;
		}

        for ( String propertyName : metaData.getEdgeTypeNames()) {
        	// skip from and to
        	if (!("from".equals(propertyName) || "to".equals(propertyName))) {
        		nextKeyBlockId += writePropertyKey(nextKeyBlockId, blockSize, idos, kdos, propertyName);
        		lastUsedIndexId++;
        	}
        }
        
		String type = PropertyIndexStore.TYPE_DESCRIPTOR + " " + CommonAbstractStore.ALL_STORES_VERSION;
		byte[] encodedType = UTF8.encode(type);
		
		ByteBuffer tailBuffer = ByteBuffer.allocate(encodedType.length);
		tailBuffer.put(encodedType);
		tailBuffer.flip();
		idos.write(tailBuffer.array());

		type = DynamicStringStore.TYPE_DESCRIPTOR + " " + CommonAbstractStore.ALL_STORES_VERSION;
		encodedType = UTF8.encode(type);
		tailBuffer = ByteBuffer.allocate(encodedType.length);
		tailBuffer.put(encodedType);
		tailBuffer.flip();
		kdos.write(tailBuffer.array());

		idos.close();
		kdos.close();

		writePropertyIds(lastUsedIndexId, indexOutput, conf);
		writePropertyIds(nextKeyBlockId, keysOutput, conf);

		
	}

	private static int writePropertyKey(int nextKeyBlockId, int blockSize, FSDataOutputStream idos, FSDataOutputStream kdos, String propertyName)
			throws IOException {
		ByteBuffer indexBuffer = ByteBuffer.allocate(9);
		indexBuffer.put(Record.IN_USE.byteValue());
		indexBuffer.putInt(0);
		indexBuffer.putInt(nextKeyBlockId);

		idos.write(indexBuffer.array());
		
		byte[] name = getStringRecordAsByteArray(nextKeyBlockId, propertyName.getBytes(), blockSize );
		
		kdos.write(name, 0, name.length);
		return (name.length / blockSize);
	}

	public static void writePropertyIds(long lastTypeId, String output, Configuration conf) throws IOException {
		String idsOutput = output + ".id";
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream idos = fs.create(new Path(idsOutput));

		ByteBuffer typeBuffer = ByteBuffer.allocate(9);
		typeBuffer.put((byte) 0).putLong(lastTypeId);
		typeBuffer.flip();
		idos.write(typeBuffer.array());

		idos.close();

	}

	public static void writePropertyStoreFooter(String propertiesOutput, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream fdos = fs.create(new Path(propertiesOutput + "/neostore.propertystore.db.footer"));

		String type = PropertyStore.TYPE_DESCRIPTOR + " " + CommonAbstractStore.ALL_STORES_VERSION;
		ByteBuffer tailBuffer = ByteBuffer.allocate(type.length());
		tailBuffer.put(type.getBytes(Charset.forName("UTF-8")));
		tailBuffer.flip();
		fdos.write(tailBuffer.array());
		fdos.close();
	}

	public static void writePropertyStringStoreHeader(String propertiesOutput, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream hdos = fs.create(new Path(propertiesOutput + "/neostore.propertystore.db.strings.header"));

		int blockSize = 120 + AbstractDynamicStore.BLOCK_HEADER_SIZE;

		ByteBuffer headBuffer = ByteBuffer.allocate(blockSize);
		headBuffer.putInt(blockSize);
		// headBuffer.flip();
		hdos.write(headBuffer.array());
		hdos.close();
		
	}

	public static void writePropertyStringStoreFooter(String propertiesOutput, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream fdos = fs.create(new Path(propertiesOutput + "/neostore.propertystore.db.strings.footer"));

		String type = DynamicStringStore.TYPE_DESCRIPTOR + " " + CommonAbstractStore.ALL_STORES_VERSION;
		ByteBuffer tailBuffer = ByteBuffer.allocate(type.length());
		tailBuffer.put(type.getBytes(Charset.forName("UTF-8")));
		tailBuffer.flip();
		fdos.write(tailBuffer.array());

		fdos.close();
		
	}

	public static void writeEmptArrayStore(String propertiesOutput, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream ados = fs.create(new Path(propertiesOutput + "/neostore.propertystore.db.arrays"));

		int blockSize = 120 + AbstractDynamicStore.BLOCK_HEADER_SIZE;

		ByteBuffer headBuffer = ByteBuffer.allocate(blockSize);
		headBuffer.putInt(blockSize);
		// headBuffer.flip();
		ados.write(headBuffer.array());

		String type = DynamicArrayStore.TYPE_DESCRIPTOR + " " + CommonAbstractStore.ALL_STORES_VERSION;
		ByteBuffer tailBuffer = ByteBuffer.allocate(type.length());
		tailBuffer.put(type.getBytes(Charset.forName("UTF-8")));
		tailBuffer.flip();
		ados.write(tailBuffer.array());

		ados.close();
		
		writePropertyIds(0L, propertiesOutput + "/neostore.propertystore.db.arrays", conf);
		
		
	}
	
	public static MetaData getMetaData(Configuration conf) {
		MetaData md = new HardCodedMetaDataImpl(conf);
		@SuppressWarnings("unchecked")
		Class<MetaData> mdClass = (Class<MetaData>) conf.getClass(AbstractMetaData.METADATA_CLASS, HardCodedMetaDataImpl.class);
		try {
			try {
				Constructor<MetaData> constructor = mdClass.getConstructor(Configuration.class);
				md = constructor.newInstance(conf);
			} catch (NoSuchMethodException nsme) {
				md = mdClass.newInstance();
			}
		} catch (Exception e) {
			// ignore any exceptions. MetaData's default is already instantiated
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		return md;
	}

}
