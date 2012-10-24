package nl.waredingen.graphs.neo.neo4j;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.Record;

public class Neo4jUtilsTest {

	@Test
	public void testPropertyStoreShortestStringValue() {
		String s = "kort";
		PropertyBlock block = new PropertyBlock(); 
		Neo4JUtils.encodeValue(block, 0, s, -1L);
		assertNotNull(block);
		assertEquals(0, block.getKeyIndexId());
		assertEquals(PropertyType.SHORT_STRING, block.getType());
		assertEquals(s, PropertyType.SHORT_STRING.getValue(block, null));
	}

	@Test
	public void testPropertyStoreShortStringValue() {
		String s = "nietzoheelkortMaarTochbestwelkort120";
		PropertyBlock block = new PropertyBlock(); 
		Neo4JUtils.encodeValue(block, 0, s, -1L);
		assertNotNull(block);
		assertEquals(0, block.getKeyIndexId());
		assertEquals(PropertyType.SHORT_STRING, block.getType());
		assertEquals(s, PropertyType.SHORT_STRING.getValue(block, null));
	}

	@Test
	public void testPropertyStoreLongStringValue() {
		String s = "nietzoheelkortMaarTochbestwelkortmaarlangerdan120zodatditineenapartenamesstorekomt";
		PropertyBlock block = new PropertyBlock(); 
		Neo4JUtils.encodeValue(block, 0, s, 201L);
		assertNotNull(block);
		assertEquals(0, block.getKeyIndexId());
		assertEquals(PropertyType.STRING, block.getType());
		assertEquals(201L, block.getSingleValueLong());
		long[] valueBlocks = block.getValueBlocks();
		assertEquals(1, valueBlocks.length);
		for (long l : valueBlocks) {
			
			//keyId | (((long) type.intValue()) << 24) | (longValue << 28)
			long expected = block.getKeyIndexId() | (((long) PropertyType.STRING.intValue()) << 24) | (201L <<28) ;
			assertEquals(expected, l);
		}
		List<DynamicRecord> valueRecords = block.getValueRecords();
		assertEquals(1, valueRecords.size());
		DynamicRecord record = valueRecords.get(0);
		assertNotNull(record);
		assertEquals(PropertyType.STRING.intValue(),record.getType());
		assertEquals(Record.NO_NEXT_BLOCK.intValue(), record.getNextBlock());
		assertEquals(201L, record.getId());
		assertEquals(82, record.getLength());
		assertEquals(201L, record.getLongId());
		assertEquals(s, new String(record.getData()));

	}

	@Test
	public void testPropertyStoreVeryLongStringValue() {
		String s = 
"Bestwelheeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeelerglang.iniedergevalzolangdathetde120tekensoverschrijdt.ditomtetestenofookeenlangestringmetallerleinextblockswerkt.";
		PropertyBlock block = new PropertyBlock(); 
		Neo4JUtils.encodeValue(block, 22, s, 202L);
		assertNotNull(block);
		assertEquals(22, block.getKeyIndexId());
		assertEquals(PropertyType.STRING, block.getType());
		assertEquals(202L, block.getSingleValueLong());
		long[] valueBlocks = block.getValueBlocks();
		assertEquals(1, valueBlocks.length);
		for (long l : valueBlocks) {
			
			//keyId | (((long) type.intValue()) << 24) | (longValue << 28)
			long expected = block.getKeyIndexId() | (((long) PropertyType.STRING.intValue()) << 24) | (202L <<28) ;
			assertEquals(expected, l);
		}
		List<DynamicRecord> valueRecords = block.getValueRecords();
		assertEquals(2, valueRecords.size());
		String result = "";
		for (DynamicRecord record : valueRecords) {
			assertNotNull(record);
			assertEquals(PropertyType.STRING.intValue(),record.getType());
			result += new String(record.getData());
		}
		
		DynamicRecord record = valueRecords.get(0);
		assertEquals(203, record.getNextBlock());
		assertEquals(202L, record.getId());
		assertEquals(120, record.getLength());
		assertEquals(202L, record.getLongId());
		
		record = valueRecords.get(1);
		assertEquals(Record.NO_NEXT_BLOCK.intValue(), record.getNextBlock());
		assertEquals(203L, record.getId());
		assertEquals(93, record.getLength());
		assertEquals(203L, record.getLongId());
		assertEquals(s, result);


	}

	@Test
	public void testPropertyStoreReallyVeryLongStringValue() {
		String s = 
"Bestwelheeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeelerglang.iniedergevalzolangdathetde120tekensoverschrijdt.ditomtetestenofookeenlangestringmetallerleinextblockswerkt.Endanookmetechtveelblockswant2isnatuurlijknietgenoeg";
		PropertyBlock block = new PropertyBlock(); 
		Neo4JUtils.encodeValue(block, 0, s, 203L);
		assertNotNull(block);
		assertEquals(0, block.getKeyIndexId());
		assertEquals(PropertyType.STRING, block.getType());
		assertEquals(203L, block.getSingleValueLong());
		long[] valueBlocks = block.getValueBlocks();
		assertEquals(1, valueBlocks.length);
		for (long l : valueBlocks) {
			
			//keyId | (((long) type.intValue()) << 24) | (longValue << 28)
			long expected = block.getKeyIndexId() | (((long) PropertyType.STRING.intValue()) << 24) | (203L <<28) ;
			assertEquals(expected, l);
		}
		List<DynamicRecord> valueRecords = block.getValueRecords();
		assertEquals(3, valueRecords.size());
		String result = "";
		for (DynamicRecord record : valueRecords) {
			assertNotNull(record);
			assertEquals(PropertyType.STRING.intValue(),record.getType());
			result += new String(record.getData());
		}
		
		DynamicRecord record = valueRecords.get(0);
		assertEquals(204, record.getNextBlock());
		assertEquals(203L, record.getId());
		assertEquals(120, record.getLength());
		assertEquals(203L, record.getLongId());
		
		record = valueRecords.get(1);
		assertEquals(205, record.getNextBlock());
		assertEquals(204L, record.getId());
		assertEquals(120, record.getLength());
		assertEquals(204L, record.getLongId());

		record = valueRecords.get(2);
		assertEquals(Record.NO_NEXT_BLOCK.intValue(), record.getNextBlock());
		assertEquals(205L, record.getId());
		assertEquals(25, record.getLength());
		assertEquals(205L, record.getLongId());

		assertEquals(s, result);


	}

	@Test
	public void testPropertyStoreLongValue() {
		long input = 123L;
		PropertyBlock block = new PropertyBlock(); 
		Neo4JUtils.encodeValue(block, 0, input, -1L);
		assertNotNull(block);
		assertEquals(0, block.getKeyIndexId());
		assertEquals(PropertyType.LONG, block.getType());
		
		long[] valueBlocks = block.getValueBlocks();
		assertEquals(1, valueBlocks.length);
		assertEquals(input, PropertyType.LONG.getValue(block, null));

		List<DynamicRecord> valueRecords = block.getValueRecords();
		assertEquals(0, valueRecords.size());

	}

	@Test
	public void testPropertyStoreLargeLongValue() {
		long input = Long.MAX_VALUE -1;
		PropertyBlock block = new PropertyBlock(); 
		Neo4JUtils.encodeValue(block, 0, input, -1L);
		assertNotNull(block);
		assertEquals(0, block.getKeyIndexId());
		assertEquals(PropertyType.LONG, block.getType());
		
		long[] valueBlocks = block.getValueBlocks();
		assertEquals(2, valueBlocks.length);
		assertEquals(input, PropertyType.LONG.getValue(block, null));
		
		long expected = block.getKeyIndexId() | (((long) PropertyType.LONG.intValue()) << 24) | (0L <<28) ;

		assertEquals(expected, valueBlocks[0]);
		assertEquals(input, valueBlocks[1]);

		List<DynamicRecord> valueRecords = block.getValueRecords();
		assertEquals(0, valueRecords.size());

	}

	@Test
	public void testPropertyStoreIntValue() {
		int input = 1234;
		PropertyBlock block = new PropertyBlock(); 
		Neo4JUtils.encodeValue(block, 0, input, -1L);
		assertNotNull(block);
		assertEquals(0, block.getKeyIndexId());
		assertEquals(PropertyType.INT, block.getType());
		
		long[] valueBlocks = block.getValueBlocks();
		assertEquals(1, valueBlocks.length);
		long expected = block.getKeyIndexId() | (((long) PropertyType.INT.intValue()) << 24) | (1234L <<28) ;

		assertEquals(expected, valueBlocks[0]);
		assertEquals(input, PropertyType.INT.getValue(block, null));

		List<DynamicRecord> valueRecords = block.getValueRecords();
		assertEquals(0, valueRecords.size());

	}

	@Test
	public void testPropertyStoreShortValue() {
		short input = 42;
		PropertyBlock block = new PropertyBlock(); 
		Neo4JUtils.encodeValue(block, 0, input, -1L);
		assertNotNull(block);
		assertEquals(0, block.getKeyIndexId());
		assertEquals(PropertyType.SHORT, block.getType());
		
		long[] valueBlocks = block.getValueBlocks();
		assertEquals(1, valueBlocks.length);
		long expected = block.getKeyIndexId() | (((long) PropertyType.SHORT.intValue()) << 24) | (42L <<28) ;

		assertEquals(expected, valueBlocks[0]);
		assertEquals(input, PropertyType.SHORT.getValue(block, null));

		List<DynamicRecord> valueRecords = block.getValueRecords();
		assertEquals(0, valueRecords.size());

	}

	@Test
	public void testPropertyStoreByteValue() {
		byte input = 127;
		PropertyBlock block = new PropertyBlock(); 
		Neo4JUtils.encodeValue(block, 0, input, -1L);
		assertNotNull(block);
		assertEquals(0, block.getKeyIndexId());
		assertEquals(PropertyType.BYTE, block.getType());
		
		long[] valueBlocks = block.getValueBlocks();
		assertEquals(1, valueBlocks.length);
		assertEquals(input, PropertyType.BYTE.getValue(block, null));

		List<DynamicRecord> valueRecords = block.getValueRecords();
		assertEquals(0, valueRecords.size());

	}

	@Test
	public void testPropertyStoreFloatValue() {
		float input = 123456.78F;
		PropertyBlock block = new PropertyBlock(); 
		Neo4JUtils.encodeValue(block, 0, input, -1L);
		assertNotNull(block);
		assertEquals(0, block.getKeyIndexId());
		assertEquals(PropertyType.FLOAT, block.getType());
		
		long[] valueBlocks = block.getValueBlocks();
		assertEquals(1, valueBlocks.length);
		assertEquals(input, PropertyType.FLOAT.getValue(block, null));

		List<DynamicRecord> valueRecords = block.getValueRecords();
		assertEquals(0, valueRecords.size());

	}

	@Test
	public void testPropertyStoreLargeFloatValue() {
		float input = Float.MAX_VALUE - 0.000000000001F;
		PropertyBlock block = new PropertyBlock(); 
		Neo4JUtils.encodeValue(block, 0, input, -1L);
		assertNotNull(block);
		assertEquals(0, block.getKeyIndexId());
		assertEquals(PropertyType.FLOAT, block.getType());
		
		long[] valueBlocks = block.getValueBlocks();
		assertEquals(1, valueBlocks.length);
		assertEquals(input, PropertyType.FLOAT.getValue(block, null));

		List<DynamicRecord> valueRecords = block.getValueRecords();
		assertEquals(0, valueRecords.size());

	}

	@Test
	public void testPropertyStoreDoubleValue() {
		double input = 1.99D;
		PropertyBlock block = new PropertyBlock(); 
		Neo4JUtils.encodeValue(block, 0, input, -1L);
		assertNotNull(block);
		assertEquals(0, block.getKeyIndexId());
		assertEquals(PropertyType.DOUBLE, block.getType());
		
		long[] valueBlocks = block.getValueBlocks();
		assertEquals(2, valueBlocks.length);

		long expected = block.getKeyIndexId() | (((long) PropertyType.DOUBLE.intValue()) << 24) | (0L <<28) ;
		
		assertEquals(expected, valueBlocks[0]);
		assertEquals(input, PropertyType.DOUBLE.getValue(block, null));

		List<DynamicRecord> valueRecords = block.getValueRecords();
		assertEquals(0, valueRecords.size());

	}

	@Test
	public void testPropertyStoreLargeDoubleValue() {
		double input = Double.MAX_VALUE -0.0000000000000001D;
		PropertyBlock block = new PropertyBlock(); 
		Neo4JUtils.encodeValue(block, 0, input, -1L);
		assertNotNull(block);
		assertEquals(0, block.getKeyIndexId());
		assertEquals(PropertyType.DOUBLE, block.getType());
		
		long[] valueBlocks = block.getValueBlocks();
		assertEquals(2, valueBlocks.length);
		
		long expected = block.getKeyIndexId() | (((long) PropertyType.DOUBLE.intValue()) << 24) | (0L <<28) ;

		assertEquals(expected, valueBlocks[0]);
		assertEquals(input, PropertyType.DOUBLE.getValue(block, null));

		List<DynamicRecord> valueRecords = block.getValueRecords();
		assertEquals(0, valueRecords.size());

	}


}
