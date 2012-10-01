package nl.waredingen.graphs.neo;

import static org.junit.Assert.assertNotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Test;
import org.neo4j.kernel.impl.nioneo.store.IdGeneratorImpl;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;

public class PropertyTest {

	// @Test
	// public void test() {
	// long tst = hexToLong("0000000016000000".getBytes());
	// System.out.println(tst);
	// long propBlock = Long.parseLong("0000000016000000", 16);
	// System.out.println(propBlock);
	// int type = (int) ((propBlock & 0x000000000F000000L) >> 24);
	// System.out.println(type);
	// }
	//
	// @Test
	// public void testBlock1() {
	// String[] stringBuffer = { "0000000016000000", "bae2608c7b000001",
	// "00000000000003de", "0000000000000000" };
	// System.out.println(toBlock(stringBuffer));
	//
	// }
	//
	// @Test
	// public void testBlock2() {
	// String[] stringBuffer = { "0000000036000000", "bae2608e7b000001",
	// "000000000001e77f", "0000000000000000" };
	// System.out.println(toBlock(stringBuffer));
	//
	// }
	//
	// @Test
	// public void testBlock3() {
	// String[] stringBuffer = { "0000000056000000", "7cf2608c7b000001",
	// "000000000000078e", "0000000000000000" };
	// System.out.println(toBlock(stringBuffer));
	//
	// }
	//
	// @Test
	// public void testBlock4() {
	// String[] stringBuffer = { "0000000076000000", "79e2608c7b000001",
	// "00000000000007be", "0000000000000000" };
	// System.out.println(toBlock(stringBuffer));
	//
	// }
	//
	// @Test
	// public void testBlock5() {
	// String[] stringBuffer = { "0000000096000000", "3dea608c7b000001",
	// "000000000000076e", "0000000000000000" };
	// System.out.println(toBlock(stringBuffer));
	//
	// }
	//
	// @Test
	// public void testBlock6() {
	// String[] stringBuffer = { "0000065396000000", "0000000000000000",
	// "0000000000000000", "0000000000000000" };
	// PropertyBlock block = toBlock(stringBuffer);
	// System.out.println(block);
	// System.out.println(block.getKeyIndexId());
	// System.out.println(block.getSingleValueLong() >>> 1);
	//
	// }
	//
	// @Test
	// public void testBlock7() {
	// String[] stringBuffer = { "000001dd16000000", "0000000000000000",
	// "0000000000000000", "0000000000000000" };
	// System.out.println(toBlock(stringBuffer));
	//
	// }
	//
	// @Test
	// public void testBlock8() {
	// String[] stringBuffer = { "0000039276000000", "0000000000000000",
	// "0000000000000000", "0000000000000000" };
	// System.out.println(toBlock(stringBuffer));
	//
	// }
	//
	// @Test
	// public void testBlock9() {
	// String[] stringBuffer = { "0000039276000000", "0000000000000000",
	// "0000000000000000", "0000000000000000" };
	// System.out.println(toBlock(stringBuffer));
	//
	// }
	//
	// @Test
	// public void testBlock10() {
	// String[] stringBuffer = { "0000009f36000000", "0000000000000000",
	// "0000000000000000", "0000000000000000" };
	// System.out.println(toBlock(stringBuffer));
	//
	// }
	//
	// @Test
	// public void testBlock11() {
	// String[] stringBuffer = { "0000039276000000", "0000000000000000",
	// "0000000000000000", "0000000000000000" };
	// System.out.println(toBlock(stringBuffer));
	//
	// }
	//
	// @Test
	// public void testBlock12() {
	// String[] stringBuffer = { "000000823b000000", "0000000000000000",
	// "0000000000000000", "0000000000000000" };
	// System.out.println(toBlock(stringBuffer));
	//
	// }
	//
	// @Test
	// public void testBlock13() {
	// String[] stringBuffer = { "db0dc08a7b000001", "0000000000000012",
	// "0000000000000000", "0000000000000000" };
	// System.out.println(toBlock(stringBuffer));
	//
	// }
	//
	// @Test
	// public void testBlock14() {
	// String[] stringBuffer = { "000001023b000002", "0000000000000000",
	// "0000000000000000", "0000000000000000" };
	// System.out.println(toBlock(stringBuffer));
	//
	// }
	//
	// @Test
	// public void testBlock15() {
	// String[] stringBuffer = { "0000000009000003", "0000000000000000",
	// "0000000000000000", "0000000000000000" };
	// System.out.println(toBlock(stringBuffer));
	//
	// }
	//
	// @Test
	// public void testBlock16() {
	// String[] stringBuffer = { "000001823b000004", "0000000000000000",
	// "0000000000000000", "0000000000000000" };
	// System.out.println(toBlock(stringBuffer));
	//
	// }
	//
	// @Test
	// public void testBlock17() {
	// String[] stringBuffer = { "db0dc18a7b000005", "0000000000000012",
	// "0000000000000000", "0000000000000000" };
	// System.out.println(toBlock(stringBuffer));
	//
	// }
	//
	// @Test
	// public void testBlock18() {
	// String[] stringBuffer = { "000000823b000000", "0000000000000000",
	// "0000000000000000", "0000000000000000" };
	// System.out.println(toBlock(stringBuffer));
	//
	// }
	//
	// @Test
	// public void testBlock19() {
	// String[] stringBuffer = { "db0dc08a7b000001", "0000000000000012",
	// "0000000000000000", "0000000000000000" };
	// System.out.println(toBlock(stringBuffer));
	//
	// }
	//
	// @Test
	// public void testBlock20() {
	// String[] stringBuffer = { "000001023b000002", "0000000000000000",
	// "0000000000000000", "0000000000000000" };
	// System.out.println(toBlock(stringBuffer));
	//
	// }
	//
	// @Test
	// public void testBlock21() {
	// String[] stringBuffer = { "0000000019000003", "0000000000000000",
	// "0000000000000000", "0000000000000000" };
	// System.out.println(toBlock(stringBuffer));
	//
	// }
	//
	// @Test
	// public void testBlock22() {
	// String[] stringBuffer = { "000001823b000004", "0000000000000000",
	// "0000000000000000", "0000000000000000" };
	// System.out.println(toBlock(stringBuffer));
	//
	// }
	//
	// @Test
	// public void testBlock23() {
	// String[] stringBuffer = { "db0dc18a7b000005", "0000000000000012",
	// "0000000000000000", "0000000000000000" };
	// System.out.println(toBlock(stringBuffer));
	//
	// }
	//
	// @Test
	// public void testBlock24() {
	// String[] stringBuffer = {
	// "000000823b000000","db0dc08a7b000001","0000000000000012",
	// "0000000000000000" };
	// System.out.println(toBlock(stringBuffer));
	//
	// }
	//
	// @Test
	// public void testBlock25() {
	// String[] stringBuffer = {
	// "000001023b000000","0000000019000001","0000000000000000",
	// "0000000000000000" };
	// System.out.println(toBlock(stringBuffer));
	//
	// }
	//
	// @Test
	// public void testBlock26() {
	// String[] stringBuffer = {
	// "000001823b000000","db0dc18a7b000001","0000000000000012",
	// "0000000000000000"};
	// System.out.println(toBlock(stringBuffer));
	//
	// }

	@Test
	public void testInuseOfDynamicStore2011() {
		String stringBuffer = "00ffffffffffffffff0008a39c7b00000004974365d2e94eb19a1a1110ab0000010000002221299938";
		ByteBuffer buf = getBufferFromIntLikeString(stringBuffer);
		System.out.println(buf);
		byte modifiers = buf.get();
		long prevMod = ((modifiers & 0xF0L) << 28);
		long nextMod = ((modifiers & 0x0FL) << 32);
		long prevProp = buf.getInt() & 0xFFFFFFFFL;
		long nextProp = buf.getInt() & 0xFFFFFFFFL;
		long recordPrevProp = longFromIntAndMod(prevProp, prevMod);
		long recordNextProp = longFromIntAndMod(nextProp, nextMod);

		System.out.println(recordPrevProp);
		System.out.println(recordNextProp);
		System.out.println(toBlock(buf));
	}

	@Test
	public void testInuseOfDynamicStoreNeo() {
		String stringBuffer = "00000000c6ffffffff000005021b000001000000000000000000000000000000000000000000000000";
		ByteBuffer buf = getBufferFromIntLikeString(stringBuffer);
		System.out.println(buf);
		byte modifiers = buf.get();
		long prevMod = ((modifiers & 0xF0L) << 28);
		long nextMod = ((modifiers & 0x0FL) << 32);
		long prevProp = buf.getInt() & 0xFFFFFFFFL;
		long nextProp = buf.getInt() & 0xFFFFFFFFL;
		long recordPrevProp = longFromIntAndMod(prevProp, prevMod);
		long recordNextProp = longFromIntAndMod(nextProp, nextMod);

		System.out.println(recordPrevProp);
		System.out.println(recordNextProp);
		System.out.println(toBlock(buf));
	}

	protected long longFromIntAndMod(long base, long modifier) {
		return modifier == 0 && base == IdGeneratorImpl.INTEGER_MINUS_ONE ? -1 : base | modifier;
	}

	private ByteBuffer getBufferFromIntLikeString(String s) {
		int len = s.length();
		ByteBuffer buf = ByteBuffer.allocate(len / 2);

		byte[] data = new byte[len / 2];
		for (int i = 0; i < len; i += 2) {
			data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
		}
		buf.put(data);
		buf.flip();
		return buf;
	}

	private PropertyBlock toBlock(String[] stringBuffer) {
		long header = hexToLong(stringBuffer[0].getBytes());
		PropertyType type = PropertyType.getPropertyType(header, true);

		assertNotNull(type);

		PropertyBlock toReturn = new PropertyBlock();
		// toReturn.setInUse( true );
		int numBlocks = type.calculateNumberOfBlocksUsed(header);
		long[] blockData = new long[numBlocks];
		blockData[0] = header; // we already have that
		for (int i = 1; i < numBlocks; i++) {
			blockData[i] = hexToLong(stringBuffer[1].getBytes());
		}
		toReturn.setValueBlocks(blockData);
		return toReturn;
	}

	private PropertyBlock toBlock(ByteBuffer buffer) {
		long header = buffer.getLong();
		PropertyType type = PropertyType.getPropertyType(header, true);

		assertNotNull(type);

		PropertyBlock toReturn = new PropertyBlock();
		// toReturn.setInUse( true );
		int numBlocks = type.calculateNumberOfBlocksUsed(header);
		long[] blockData = new long[numBlocks];
		blockData[0] = header; // we already have that
		for (int i = 1; i < numBlocks; i++) {
			blockData[i] = buffer.getLong();
		}
		toReturn.setValueBlocks(blockData);
		return toReturn;
	}

	private long hexToLong(byte[] bytes) {

		if (bytes.length > 16) {
			throw new IllegalArgumentException("Byte array too long (max 16 elements)");
		}
		long v = 0;
		for (int i = 0; i < bytes.length; i += 2) {
			byte b1 = (byte) (bytes[i] & 0xFF);

			b1 -= 48;
			if (b1 > 9)
				b1 -= 39;

			if (b1 < 0 || b1 > 15) {
				throw new IllegalArgumentException("Illegal hex value: " + bytes[i]);
			}

			b1 <<= 4;

			byte b2 = (byte) (bytes[i + 1] & 0xFF);
			b2 -= 48;
			if (b2 > 9)
				b2 -= 39;

			if (b2 < 0 || b2 > 15) {
				throw new IllegalArgumentException("Illegal hex value: " + bytes[i + 1]);
			}

			v |= (((b1 & 0xF0) | (b2 & 0x0F))) & 0x00000000000000FFL;

			if (i + 2 < bytes.length)
				v <<= 8;
		}

		return v;
	}

	private byte[] longToHex(final long l) {
		long v = l & 0xFFFFFFFFFFFFFFFFL;

		byte[] result = new byte[16];
		Arrays.fill(result, 0, result.length, (byte) 0);

		for (int i = 0; i < result.length; i += 2) {
			byte b = (byte) ((v & 0xFF00000000000000L) >> 56);

			byte b2 = (byte) (b & 0x0F);
			byte b1 = (byte) ((b >> 4) & 0x0F);

			if (b1 > 9)
				b1 += 39;
			b1 += 48;

			if (b2 > 9)
				b2 += 39;
			b2 += 48;

			result[i] = (byte) (b1 & 0xFF);
			result[i + 1] = (byte) (b2 & 0xFF);

			v <<= 8;
		}

		return result;
	}

}
