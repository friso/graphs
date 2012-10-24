package nl.waredingen.graphs.neo.mapreduce.input;

import static org.junit.Assert.assertEquals;

import java.util.Scanner;

import org.junit.Test;

public class InputTypeConversionTest {

	private Scanner getScanner(String s) {
		Scanner scanner = new Scanner(s);
		scanner.useDelimiter("\t");
		scanner.useRadix(10);
		
		return scanner;
	}
	
	@Test
	public void testScanning() {
		Scanner scanner = getScanner("This\tis\ta\ttest\twith\t12\t129\t260\t"+(Integer.MAX_VALUE - 3)+"\t"+(Long.MAX_VALUE - 3)+"\tand some extra fields:\teen\t\t\tvier\t\ttrue\tfalse");
		while(scanner.hasNext()) {
//			System.out.println(scanner.next());
			if (scanner.hasNextByte()) {
				System.out.println("Byte : "+scanner.nextByte());
			}
			else if (scanner.hasNextShort()) {
				System.out.println("Short : "+scanner.nextShort());
			}
			else if (scanner.hasNextInt()) {
				System.out.println("Int : "+scanner.nextInt());
			}
			else if (scanner.hasNextLong()) {
				System.out.println("Long : "+scanner.nextLong());
			}
			else if (scanner.hasNextFloat()) {
				System.out.println("Float : "+scanner.nextFloat());
			}
			else if (scanner.hasNextDouble()) {
				System.out.println("Double : "+scanner.nextDouble());
			}
			else if (scanner.hasNextBoolean()) {
				System.out.println("Boolean : "+scanner.nextBoolean());
			}
			else {
				System.out.println("String : "+scanner.next());
			}
		}
		assertEquals("Testing 123", "Testing 123");
	}

}
