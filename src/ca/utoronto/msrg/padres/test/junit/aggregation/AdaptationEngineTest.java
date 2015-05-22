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

public class AdaptationEngineTest extends TestCase {

	BrokerCore broker1;
	BrokerCore broker2;
	TestClient publisher;
	TestClient subscriber;

	String BROKER1_ADDRESS = "rmi://127.0.0.1:8555/broker1";
	String BROKER2_ADDRESS = "rmi://127.0.0.1:5445/broker2";

	@Override
	protected void setUp() throws Exception {

		// System.setProperty("padres.aggregation.implementation","ADAPTIVE_AGGREGATION");
		System.setProperty("aggregation.client", "OFF");
		broker1 = new BrokerCore("-uri " + BROKER1_ADDRESS);
		broker1.initialize();
		broker2 = new BrokerCore("-uri " + BROKER2_ADDRESS + " -n " + BROKER1_ADDRESS);
		broker2.initialize();
		publisher = new TestClient("Publisher");
		publisher.connect(BROKER1_ADDRESS);
		subscriber = new TestClient("Subscriber");
		subscriber.connect(BROKER2_ADDRESS);

		super.setUp();
	}

	public void testRAWMODE() throws ClientException, ParseException, InterruptedException {

		if (System.getProperty("padres.aggregation.implementation").equals("ADAPTIVE_AGGREGATION")) {
			((AdaptiveAggregationEngine) broker1.getRouter().getPostProcessor().getAggregationEngine()).getAdaptationEngine().setForcedMode(
					AggregationMode.RAW_MODE);
			((AdaptiveAggregationEngine) broker2.getRouter().getPostProcessor().getAggregationEngine()).getAdaptationEngine().setForcedMode(
					AggregationMode.RAW_MODE);
		}
		publisher.advertise(MessageFactory.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"), BROKER1_ADDRESS);
		Thread.sleep(1000);
		subscriber.subscribe("[class,eq,STOCK],[value,>,15],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'5'],[NTF,eq,'5']", BROKER2_ADDRESS);

		Thread.sleep(5000);
		for (int i = 0; i < 10; i++) {
			publisher.publish(MessageFactory.createPublicationFromString("[class,STOCK],[value,80]"), BROKER1_ADDRESS);
		}
		Thread.sleep(1000);
		assertEquals(10, subscriber.getAllReceivedMessage().size());

	}

	public void testRAWAGGMODE() throws ClientException, ParseException, InterruptedException {
		if (System.getProperty("padres.aggregation.implementation").equals("ADAPTIVE_AGGREGATION")) {
			((AdaptiveAggregationEngine) broker1.getRouter().getPostProcessor().getAggregationEngine()).getAdaptationEngine().setForcedMode(
					AggregationMode.RAW_MODE);
			((AdaptiveAggregationEngine) broker2.getRouter().getPostProcessor().getAggregationEngine()).getAdaptationEngine().setForcedMode(
					AggregationMode.AGG_MODE);
		}
		publisher.advertise(MessageFactory.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"), BROKER1_ADDRESS);
		Thread.sleep(1000);
		subscriber.subscribe("[class,eq,STOCK],[value,>,15],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'5'],[NTF,eq,'5']", BROKER2_ADDRESS);

		Thread.sleep(1000);
		for (int i = 0; i < 10; i++) {
			publisher.publish(MessageFactory.createPublicationFromString("[class,STOCK],[value,80]"), BROKER1_ADDRESS);
		}
		Thread.sleep(5000);
		assertEquals(1, subscriber.getAllReceivedMessage().size());

	}

	public void testAGG2RAW() throws ClientException, ParseException, InterruptedException {
		if (System.getProperty("padres.aggregation.implementation").equals("ADAPTIVE_AGGREGATION")) {
			((AdaptiveAggregationEngine) broker1.getRouter().getPostProcessor().getAggregationEngine()).getAdaptationEngine().setForcedMode(null);
			((AdaptiveAggregationEngine) broker2.getRouter().getPostProcessor().getAggregationEngine()).getAdaptationEngine().setForcedMode(null);
		}
		publisher.advertise(MessageFactory.createAdvertisementFromString("[class,eq,STOCK],[value,>,0]"), BROKER1_ADDRESS);
		Thread.sleep(1000);
		subscriber.subscribe("[class,eq,STOCK],[value,>,0],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'5']", BROKER2_ADDRESS);

		Thread.sleep(1000);

		try {
			Thread.sleep(5000 - (System.currentTimeMillis() % (5000)));
		} catch (InterruptedException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

		for (int i = 0; i < 10; i++) {
			publisher.publish(MessageFactory.createPublicationFromString("[class,STOCK],[value," + i + "]"), BROKER1_ADDRESS);
			Thread.sleep(100);
		}
		if (System.getProperty("padres.aggregation.implementation").equals("ADAPTIVE_AGGREGATION")) {
			assertEquals(AggregationMode.AGG_MODE, ((AdaptiveAggregationEngine) broker1.getRouter().getPostProcessor().getAggregationEngine())
					.getAdaptationEngine().getSwitchStatus());
		}
		Thread.sleep(5000);

		for (int i = 0; i < 10; i++) {
			publisher.publish(MessageFactory.createPublicationFromString("[class,STOCK],[value," + 10 + "]"), BROKER1_ADDRESS);
			Thread.sleep(6000);
		}
		if (System.getProperty("padres.aggregation.implementation").equals("ADAPTIVE_AGGREGATION")) {
			assertEquals(AggregationMode.RAW_MODE, ((AdaptiveAggregationEngine) broker1.getRouter().getPostProcessor().getAggregationEngine())
					.getAdaptationEngine().getSwitchStatus());
		}
		Thread.sleep(1);
		for (int i = 0; i < 20; i++) {
			publisher.publish(MessageFactory.createPublicationFromString("[class,STOCK],[value," + 100 + "]"), BROKER1_ADDRESS);
			Thread.sleep(500);
		}
		if (System.getProperty("padres.aggregation.implementation").equals("ADAPTIVE_AGGREGATION")) {
			assertEquals(AggregationMode.AGG_MODE, ((AdaptiveAggregationEngine) broker1.getRouter().getPostProcessor().getAggregationEngine())
					.getAdaptationEngine().getSwitchStatus());
		}
		boolean toRaw = false, toAgg = false;

		// assertTrue(((PublicationMessage)subscriber.getReceivedMessage().get(0)).getPublication()
		// instanceof AggregatedPublication);

		Thread.sleep(5000);

		for (Message m : subscriber.getAllReceivedMessage()) {
			if (!toRaw && !(((PublicationMessage) m).getPublication() instanceof AggregatedPublication))
				toRaw = true;
			if (toRaw && (((PublicationMessage) m).getPublication() instanceof AggregatedPublication)) {
				toAgg = true;
				break;
			}
		}

		assertTrue(toRaw);
		assertTrue(toAgg);

	}

}
