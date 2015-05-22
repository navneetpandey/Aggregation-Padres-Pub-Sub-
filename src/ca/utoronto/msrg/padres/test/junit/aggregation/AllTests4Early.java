package ca.utoronto.msrg.padres.test.junit.aggregation;

import junit.framework.Test;
import junit.framework.TestSuite;

public class AllTests4Early extends TestSuite {

	
	
	
	public static Test suite() {
		System.setProperty("padres.aggregation.implementation", "EARLY_AGGREGATION");
		System.setProperty("aggregation.client", "OFF");
		TestSuite suite = new TestSuite(AllTests4Early.class.getName());
		//$JUnit-BEGIN$
		suite.addTestSuite(AggregationTest.class);
		suite.addTestSuite(AggregatorTest.class);
		suite.addTestSuite(CollectorTest.class);
		suite.addTestSuite(RoutingTableTest.class);
		suite.addTestSuite(StarCaseTest.class);
		suite.addTestSuite(StartTimeOfTheCurrentWindowTest.class);
		suite.addTestSuite(OnePublisherTwoSubscriberTest.class);
		suite.addTestSuite(LinearCase.class);
		//$JUnit-END$
		return suite;
	}
	
	
}
