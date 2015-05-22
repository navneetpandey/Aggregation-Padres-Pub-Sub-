package ca.utoronto.msrg.padres.test.junit.aggregation;

import junit.framework.Test;
import junit.framework.TestSuite;

public class AllTests {

	public static Test suite() {
		System.setProperty("padres.aggregation.implementation","ADAPTIVE_AGGREGATION");
		TestSuite suite = new TestSuite(AllTests.class.getName());
		//$JUnit-BEGIN$
		suite.addTestSuite(AggregationTest.class);
		suite.addTestSuite(CollectorTest.class);
		suite.addTestSuite(RoutingTableTest.class);
		suite.addTestSuite(AdaptationEngineTest.class);
		suite.addTestSuite(StartTimeOfTheCurrentWindowTest.class);
		//$JUnit-END$
		return suite;
	}

}
