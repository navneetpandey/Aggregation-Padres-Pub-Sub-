package ca.utoronto.msrg.padres.test.junit.aggregation;

import java.io.IOException;

import junit.framework.TestCase;
import ca.utoronto.msrg.padres.broker.aggregation.utility.Stats;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCoreException;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.common.message.parser.MessageFactory;
import ca.utoronto.msrg.padres.common.message.parser.ParseException;
import ca.utoronto.msrg.padres.test.junit.aggregation.utility.TestClient;

public class TestNormalSubscription extends TestCase {

	final String Broker1 = "rmi://localhost:1098/Broker1";

	public void testOptimal() throws BrokerCoreException, ClientException, ParseException, InterruptedException, IOException {
		System.setProperty("padres.aggregation.implementation", "OPTIMAL_AGGREGATION");
		System.setProperty("aggregation.client", "OFF");

		onePub();
	}

	public void testAdaptive() throws BrokerCoreException, ClientException, ParseException, InterruptedException, IOException {
		System.setProperty("padres.aggregation.implementation", "ADAPTIVE_AGGREGATION");
		System.setProperty("aggregation.client", "OFF");

		onePub();
	}

	public void testEarly() throws BrokerCoreException, ClientException, ParseException, InterruptedException, IOException {
		System.setProperty("padres.aggregation.implementation", "EARLY_AGGREGATION");
		System.setProperty("aggregation.client", "OFF");

		onePub();
	}

	public void testLate() throws BrokerCoreException, ClientException, ParseException, InterruptedException, IOException {
		System.setProperty("padres.aggregation.implementation", "LATE_AGGREGATION");
		System.setProperty("aggregation.client", "OFF");

		onePub();
	}

	public void testFG() throws BrokerCoreException, ClientException, ParseException, InterruptedException, IOException {
		System.setProperty("padres.aggregation.implementation", "FG_AGGREGATION");
		System.setProperty("aggregation.client", "OFF");

		onePub();
	}
	
	private void onePub() throws BrokerCoreException, ClientException, ParseException, InterruptedException, IOException {
		TestClient publisher = new TestClient("Publisher");
		TestClient subscriber = new TestClient("Normal Subscriber");
		TestClient aggSubscriber = new TestClient("Aggregate Subscriber");
		BrokerCore bc1 = new BrokerCore("-uri " + Broker1);
		bc1.initialize();

		publisher.connect(Broker1);
		publisher.advertise(MessageFactory.createAdvertisementFromString("[class,eq,STOCK],[value,>,2]"), Broker1);
		Thread.sleep(100);
		subscriber.connect(Broker1);
		subscriber.subscribe("[class,eq,STOCK],[value,>,115]", Broker1);

		Publication p = MessageFactory.createPublicationFromString("[class,STOCK],[value,180]");
		// PublicationMessage pMsg= new PublicationMessage(p);

		publisher.publish(p);

		Thread.sleep(2000);

		assertEquals(1, subscriber.getAllReceivedMessage().size());

		subscriber.getAllReceivedMessage().clear();

		p = MessageFactory.createPublicationFromString("[class,STOCK],[value,110]");

		publisher.publish(p);

		Thread.sleep(500);

		assertEquals(0, subscriber.getAllReceivedMessage().size());

		Stats.getInstance().printResult();

		//aggSubscriber.connect(Broker1);
		subscriber.subscribe("[class,eq,STOCK],[value,>,115],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'4'],[NTF,eq,'4']", Broker1);

		p = MessageFactory.createPublicationFromString("[class,STOCK],[value,180]");

		publisher.publish(p);

		Thread.sleep(2000);

		if(System.getProperty("padres.aggregation.implementation").equals("OPTIMAL_AGGREGATION"))
			assertEquals(1, subscriber.getAllReceivedMessage().size());
		else
			assertEquals(2, subscriber.getAllReceivedMessage().size());

		subscriber.getAllReceivedMessage().clear();

		p = MessageFactory.createPublicationFromString("[class,STOCK],[value,110]");

		publisher.publish(p);

		Thread.sleep(500);

		assertEquals(0, subscriber.getAllReceivedMessage().size());

		Stats.getInstance().printResult();

		subscriber.getAllReceivedMessage().clear();

		for (int i = 0; i < 100; i++) {
			p = MessageFactory.createPublicationFromString("[class,STOCK],[value," + (180 + i) + "]");
			publisher.publish(p);
		}

		Thread.sleep(2000);
		
		if(System.getProperty("padres.aggregation.implementation").equals("OPTIMAL_AGGREGATION"))
			assertEquals(100, subscriber.getAllReceivedMessage().size());
		else
			assertEquals(101, subscriber.getAllReceivedMessage().size());
		
		Stats.getInstance().printResult();
		
		subscriber.getAllReceivedMessage().clear();

		for (int i = 0; i < 100; i++) {
			p = MessageFactory.createPublicationFromString("[class,STOCK],[value," + (110 - i) + "]");
			publisher.publish(p);
		}

		Thread.sleep(500);

		assertEquals(0, subscriber.getAllReceivedMessage().size());

		Stats.getInstance().printResult();

		
		
	}

}
