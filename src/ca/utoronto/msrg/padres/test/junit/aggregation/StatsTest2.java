package ca.utoronto.msrg.padres.test.junit.aggregation;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import junit.framework.TestCase;

import ca.utoronto.msrg.padres.broker.aggregation.utility.Stats;

public class StatsTest2 extends TestCase {

	OutputStream outStream;
	Stats stats;

	protected void setUp() throws Exception {
		outStream = new ByteArrayOutputStream();
		stats = Stats.getInstance(outStream);

	}

	public void testHeaderPrinting2() {
		stats.addStatsField("cpu.history1");
		stats.addValue("cpu.history1", 1);
		stats.addStatsField("cpu.history2");
		stats.addValue("cpu.history2", 2);
		try {
			stats.printResult();
			assertEquals("1cpu:1history1-2history2\n1-2\n", stats.getStream()
					.toString());
			System.out.println(outStream.toString());
		} catch (IOException e) {

		}

	}
}
