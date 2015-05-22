package ca.utoronto.msrg.padres.test.junit.aggregation;

import junit.framework.TestCase;
import ca.utoronto.msrg.padres.broker.aggregation.AdaptiveAggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationMode;
import ca.utoronto.msrg.padres.broker.aggregation.message.AggregatedPublication;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.message.Message;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.parser.MessageFactory;
import ca.utoronto.msrg.padres.common.message.parser.ParseException;
import ca.utoronto.msrg.padres.test.junit.aggregation.utility.TestClient;

public class LinearCase extends TestCase {
	BrokerCore A, B, center;
	TestClient publisherA, publisherB, publisherCenter;
	TestClient subscriberA, subscriberB, subscriberCenter;

	final String BROKER_CENTER = "rmi://localhost:1099/BrokerCenter";

	final String BROKER_A = "rmi://localhost:1098/BrokerA";
	final String BROKER_B = "rmi://localhost:1097/BrokerB";

	/*
	 * 
	 * A -- Center -- B
	 */

	@Override
	protected void setUp() throws Exception {

		// ////////////////////////////////////////////
		// System.setProperty("padres.aggregation.implementation",
		// "OPTIMAL_AGGREGATION");
		// //////////////////////////////////////////////

		//System.setProperty("padres.aggregation.implementation", "FG_AGGREGATION");

		System.setProperty("aggregation.client", "OFF");

		if (System.getProperty("padres.aggregation.implementation") == null)
			assertTrue(false);

		center = new BrokerCore("-uri " + BROKER_CENTER);
		center.initialize();

		publisherCenter = new TestClient("Publisher CENTER");
		subscriberCenter = new TestClient("Subscriber CENTER");
		publisherCenter.connect(BROKER_CENTER);
		subscriberCenter.connect(BROKER_CENTER);

		A = new BrokerCore("-uri " + BROKER_A + " -n " + BROKER_CENTER);
		A.initialize();

		publisherA = new TestClient("Publisher A");
		subscriberA = new TestClient("Subscriber A");
		publisherA.connect(BROKER_A);
		subscriberA.connect(BROKER_A);

		B = new BrokerCore("-uri " + BROKER_B + " -n " + BROKER_CENTER);
		B.initialize();

		publisherB = new TestClient("Publisher B");
		subscriberB = new TestClient("Subscriber B");
		publisherB.connect(BROKER_B);
		subscriberB.connect(BROKER_B);

		super.setUp();
	}

	@Override
	protected void tearDown() throws Exception {

		A.shutdown();
		B.shutdown();
		center.shutdown();
		Thread.sleep(2000);
		super.tearDown();
	}

	public void testB() throws ClientException, ParseException, InterruptedException {

	//	System.setProperty("padres.aggregation.implementation", "EARLY_AGGREGATION");
		System.setProperty("aggregation.client", "OFF");

		publisherA.advertise(MessageFactory.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"), BROKER_A);
		publisherB.advertise(MessageFactory.createAdvertisementFromString("[class,eq,STOCK],[value,>,30]"), BROKER_B);

		Thread.sleep(1000L);
		subscriberA.subscribe("[class,eq,STOCK],[value,>,15],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'5'],[NTF,eq,'5']", BROKER_A);
		subscriberB.subscribe("[class,eq,STOCK],[value,>,15],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'5'],[NTF,eq,'5']", BROKER_B);

		/*
		 * if (System.getProperty("padres.aggregation.implementation").equals(
		 * "OPTIMAL_AGGREGATION")) { // artificially speeding the broker
		 * A.getAggregatorTimer().setOptimalSecondPassTime(200);
		 * B.getAggregatorTimer().setOptimalSecondPassTime(200);
		 * center.getAggregatorTimer().setOptimalSecondPassTime(200);
		 * 
		 * A.getAggregatorTimer().updateWindowTimeGCD(1, 1);
		 * B.getAggregatorTimer().updateWindowTimeGCD(1, 1);
		 * center.getAggregatorTimer().updateWindowTimeGCD(1, 1); // --- }
		 */

		Thread.sleep(5000L - System.currentTimeMillis() % 5000);

		for (int i = 0; i < 10; i++) {
			publisherA.publish(MessageFactory.createPublicationFromString("[class,STOCK],[value,80]"), BROKER_A);
			publisherB.publish(MessageFactory.createPublicationFromString("[class,STOCK],[value,800]"), BROKER_B);

			// Thread.sleep(90);
		}
		Thread.sleep(15000); // 3 brokers to go ... 5 sec for each ...

		System.out.println("\nA\n\n");
		int res = 0;

		// assertEquals(2, subscriberA.getReceivedMessage().size());

		int nm = 0;
		for (Message m : subscriberA.getAllReceivedMessage()) {
			System.out.println(" m " + nm++);
			System.out.println(((AggregatedPublication) ((PublicationMessage) m).getPublication()).getAggResult());
			res += Integer.parseInt(((AggregatedPublication) ((PublicationMessage) m).getPublication()).getAggResult());
		}
		//Thread.sleep(10000);
		assertEquals(8800, res);
		res = 0;
		System.out.println("\nB\n\n");

		for (Message m : subscriberB.getAllReceivedMessage()) {
			System.out.println(((AggregatedPublication) ((PublicationMessage) m).getPublication()).getAggResult());
			res += Integer.parseInt(((AggregatedPublication) ((PublicationMessage) m).getPublication()).getAggResult());
		}
		assertEquals(8800, res);

		// assertEquals(1, subscriberB.getReceivedMessage().size());

	}
}
