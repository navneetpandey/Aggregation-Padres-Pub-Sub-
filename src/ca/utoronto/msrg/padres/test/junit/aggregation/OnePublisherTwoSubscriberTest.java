package ca.utoronto.msrg.padres.test.junit.aggregation;

import junit.framework.TestCase;
import ca.utoronto.msrg.padres.broker.aggregation.message.AggregatedPublication;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.message.Message;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.parser.MessageFactory;
import ca.utoronto.msrg.padres.common.message.parser.ParseException;
import ca.utoronto.msrg.padres.test.junit.aggregation.utility.TestClient;

public class OnePublisherTwoSubscriberTest extends TestCase {
	BrokerCore A, B, center;
	TestClient publisherA;
	TestClient subscriberA, subscriberB;

	final String BROKER_CENTER = "rmi://localhost:1099/BrokerCenter";

	final String BROKER_A = "rmi://localhost:1098/BrokerA";
	final String BROKER_B = "rmi://localhost:1097/BrokerB";

	/*
	 * 
	 * A -- Center -- B
	 */

	@Override
	protected void setUp() throws Exception {

		//System.setProperty("padres.aggregation.implementation","OPTIMAL_AGGREGATION");
		//System.setProperty("padres.aggregation.implementation","EARLY_AGGREGATION");
		System.setProperty("aggregation.client", "OFF");
		if(System.getProperty("padres.aggregation.implementation")==null)assertTrue(false);
		
		
		center = new BrokerCore("-uri " + BROKER_CENTER);
		center.initialize();

		A = new BrokerCore("-uri " + BROKER_A + " -n " + BROKER_CENTER);
		A.initialize();

		publisherA = new TestClient("Publisher A");
		subscriberA = new TestClient("Subscriber A");
		publisherA.connect(BROKER_A);
		subscriberA.connect(BROKER_A);

		B = new BrokerCore("-uri " + BROKER_B + " -n " + BROKER_CENTER);
		B.initialize();

		subscriberB = new TestClient("Subscriber B");
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
		System.out.println("OnePublisherTwoSubscribers TESTB");
		publisherA.advertise(MessageFactory.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"), BROKER_A);

		Thread.sleep(1000L);
		subscriberA.subscribe("[class,eq,STOCK],[value,>,15],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'5']", BROKER_A);
		subscriberB.subscribe("[class,eq,STOCK],[value,>,15],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'5']", BROKER_B);


		


		Thread.sleep(1000L);

		Thread.sleep(5000 - (System.currentTimeMillis() % 5000));

		for (int i = 0; i < 1; i++) {
			publisherA.publish(MessageFactory.createPublicationFromString("[class,STOCK],[value,80]"), BROKER_A);

			Thread.sleep(90);
		}
		Thread.sleep(6000);

		System.out.println("\nA\n\n");
		int res = 0;
		for (Message m : subscriberA.getAllReceivedMessage()) {
			System.out.println(((AggregatedPublication) ((PublicationMessage) m).getPublication()).getAggResult() + " From "
					+ m.getNextHopID().toString() + " To " + m.getLastHopID().toString());
			res += Integer.parseInt(((AggregatedPublication) ((PublicationMessage) m).getPublication()).getAggResult());
		}
		assertEquals(80, res);
		res = 0;
		System.out.println("\nB\n\n");

		for (Message m : subscriberB.getAllReceivedMessage()) {
			System.out.println(((AggregatedPublication) ((PublicationMessage) m).getPublication()).getAggResult() + " From "
					+ m.getNextHopID().toString() + " To " + m.getLastHopID().toString());
			res += Integer.parseInt(((AggregatedPublication) ((PublicationMessage) m).getPublication()).getAggResult());
		}
		assertEquals(80, res);

		assertEquals(1, subscriberB.getAllReceivedMessage().size());

	}
}
