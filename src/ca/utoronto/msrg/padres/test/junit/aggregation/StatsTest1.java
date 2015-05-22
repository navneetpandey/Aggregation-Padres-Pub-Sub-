package ca.utoronto.msrg.padres.test.junit.aggregation;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import ca.utoronto.msrg.padres.broker.aggregation.utility.Stats;
import junit.framework.TestCase;

public class StatsTest1 extends TestCase{
	
	OutputStream outStream;
	Stats stats;
	
	 
	protected void setUp() throws Exception {
		outStream= new ByteArrayOutputStream();
		stats = Stats.getInstance(outStream);
	
	}
	public void testHeaderPrinting1(){
		
		stats.addStatsField("cpu.history1");
		stats.addValue("cpu.history", 1);
		try {
			stats.printResult();
			assertEquals("1cpu:1history1\n0\n", outStream.toString());
			System.out.println(outStream.toString());
		} catch (IOException e) {
			 
		}
		stats.addValue("cpu.history1", 1);
		try {
			 stats.printResult();
			 assertEquals("1cpu:1history1\n0\n1\n", outStream.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
	

}
