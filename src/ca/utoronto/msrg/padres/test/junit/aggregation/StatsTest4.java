package ca.utoronto.msrg.padres.test.junit.aggregation;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import ca.utoronto.msrg.padres.broker.aggregation.utility.Stats;
import junit.framework.TestCase;

public class StatsTest4 extends TestCase {


	OutputStream outStream;
	Stats stats;

	protected void setUp() throws Exception {
		outStream = new ByteArrayOutputStream();
		stats = Stats.getInstance(outStream);

	}

	public void testFloatPrinting() {
		stats.addStatsField("cpu.history3");
		stats.addValue("cpu.history3", 1.0);
		stats.addStatsField("cpu.history2");
		stats.addValue("cpu.history2", 2.0);
		stats.addValue("cpu.history2", 3);
	
		try {
			stats.printResult();
			assertEquals("1cpu:1history3-2history2\n1.0-5.0\n", stats.getStream()
					.toString());
			System.out.println(outStream.toString());
		} catch (IOException e) {

		}

	}
}
