package ca.utoronto.msrg.padres.test.junit.aggregation;

import junit.framework.Test;
import junit.framework.TestSuite;

public class AllTest4Stats {
	public static Test suite() {
		
		TestSuite suite = new TestSuite(AllTest4Stats.class.getName());
		//$JUnit-BEGIN$
		suite.addTestSuite(StatsTest2.class);
		suite.addTestSuite(StatsTest1.class);
		//$JUnit-END$
		return suite;
	}
	
}
