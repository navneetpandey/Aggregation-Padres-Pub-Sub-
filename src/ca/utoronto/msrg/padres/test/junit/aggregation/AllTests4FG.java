package ca.utoronto.msrg.padres.test.junit.aggregation;

import junit.framework.Test;
import junit.framework.TestSuite;

public class AllTests4FG {
	public static Test suite() {
		System.setProperty("padres.aggregation.implementation","FG_AGGREGATION");
		TestSuite suite = new TestSuite(AllTests4FG.class.getName());
		//$JUnit-BEGIN$
		suite.addTestSuite(AggregationTest.class);
		suite.addTestSuite(StarCaseTest.class);
		suite.addTestSuite(OnePublisherTwoSubscriberTest.class);
		suite.addTestSuite(LinearCase.class);
		suite.addTestSuite(StartTimeOfTheCurrentWindowTest.class);
		suite.addTestSuite(AdaptationEngineTest.class);
		//$JUnit-END$
		return suite;
	}
}
