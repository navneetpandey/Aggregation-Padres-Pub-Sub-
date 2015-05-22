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

public class BrokerPair extends TestCase {

	BrokerCore A, B;
	TestClient publisherA, publisherB;
	TestClient subscriberA, subscriberB;

	final String BROKER_A = "rmi://localhost:1098/BrokerA";
	final String BROKER_B = "rmi://localhost:1097/BrokerB";

	/*
	 * 
	 * A  -- B
	 */

	@Override
	protected void setUp() throws Exception {

		// ////////////////////////////////////////////
		// System.setProperty("padres.aggregation.implementation",
		// "OPTIMAL_AGGREGATION");
		// //////////////////////////////////////////////

		A = new BrokerCore("-uri " + BROKER_A + " -n " + BROKER_B);
		A.initialize();

		publisherA = new TestClient("Publisher A");
		subscriberA = new TestClient("Subscriber A");
		publisherA.connect(BROKER_A);
		subscriberA.connect(BROKER_A);

		B = new BrokerCore("-uri " + BROKER_B + " -n " + BROKER_A);
		B.initialize();

		publisherB = new TestClient("Publisher B");
		subscriberB = new TestClient("Subscriber B");
		publisherB.connect(BROKER_B);
		subscriberB.connect(BROKER_B);

		super.setUp();
	}

	public void testB() throws ClientException, ParseException,
			InterruptedException {

		publisherA
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						BROKER_A);
		publisherB
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,30]"),
						BROKER_B);

		Thread.sleep(1000);
		subscriberA
				.subscribe(
						"[class,eq,STOCK],[value,>,15],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'5'],[NTF,eq,'5']",
						BROKER_A);
		subscriberB
				.subscribe(
						"[class,eq,STOCK],[value,>,15],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'5'],[NTF,eq,'5']",
						BROKER_B);
		Thread.sleep(4000);


		for (int i = 0; i < 1; i++) {
			publisherA.publish(MessageFactory
					.createPublicationFromString("[class,STOCK],[value,80]"),
					BROKER_A);
			publisherB.publish(MessageFactory
					.createPublicationFromString("[class,STOCK],[value,800]"),
					BROKER_B);

			// Thread.sleep(90);
		}
		Thread.sleep(1000);

		System.out.println("\nA\n\n");
		int res = 0;

		// assertEquals(2, subscriberA.getReceivedMessage().size());

		for (Message m : subscriberA.getAllReceivedMessage()) {
			System.out
					.println(((AggregatedPublication) ((PublicationMessage) m)
							.getPublication()).getAggResult());
			res += Integer
					.parseInt(((AggregatedPublication) ((PublicationMessage) m)
							.getPublication()).getAggResult());
		}
		Thread.sleep(10000);
		assertEquals(880, res);
		res = 0;
		System.out.println("\nB\n\n");

		for (Message m : subscriberB.getAllReceivedMessage()) {
			System.out
					.println(((AggregatedPublication) ((PublicationMessage) m)
							.getPublication()).getAggResult());
			res += Integer
					.parseInt(((AggregatedPublication) ((PublicationMessage) m)
							.getPublication()).getAggResult());
		}
		assertEquals(880, res);

		assertEquals(1, subscriberB.getAllReceivedMessage().size());

	}

}
