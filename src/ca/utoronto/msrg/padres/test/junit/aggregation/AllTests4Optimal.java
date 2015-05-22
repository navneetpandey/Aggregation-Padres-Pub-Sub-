package ca.utoronto.msrg.padres.test.junit.aggregation;

import junit.framework.Test;
import junit.framework.TestSuite;

public class AllTests4Optimal {
	
	public static Test suite() {
		System.setProperty("padres.aggregation.implementation", "OPTIMAL_AGGREGATION");
		System.setProperty("aggregation.client", "OFF");
		
		TestSuite suite = new TestSuite(AllTests4Optimal.class.getName());
		//$JUnit-BEGIN$
		suite.addTestSuite(OptimalAggregationEngineTest.class);
		suite.addTestSuite(StarCaseTest.class);
		//suite.addTestSuite(StartTimeOfTheCurrentWindowTest.class);
		suite.addTestSuite(OnePublisherTwoSubscriberTest.class);
		suite.addTestSuite(LinearCase.class);
		//$JUnit-END$
		return suite;
	}
	
	
}
