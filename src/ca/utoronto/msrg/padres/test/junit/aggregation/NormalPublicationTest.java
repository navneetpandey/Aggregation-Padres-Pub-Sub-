package ca.utoronto.msrg.padres.test.junit.aggregation;

import junit.framework.TestCase;
import ca.utoronto.msrg.padres.broker.aggregation.message.AggregatedPublication;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCoreException;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.parser.MessageFactory;
import ca.utoronto.msrg.padres.common.message.parser.ParseException;
import ca.utoronto.msrg.padres.test.junit.aggregation.utility.TestClient;

public class NormalPublicationTest extends TestCase{
	BrokerCore brokerCore1;
	BrokerCore brokerCore2;
	BrokerCore brokerCore3;
	BrokerCore brokerCore4;

	TestClient clientPub1;
	//TestClient clientPub2;
	TestClient clientSub1;
	TestClient clientSub2;



	@Override
	protected void setUp() throws Exception {


		String broker1url = "-uri rmi://localhost:1101/Broker1";
		String broker2url = "-uri rmi://localhost:1102/Broker2 -n rmi://localhost:1101/Broker1";
		String broker3url = "-uri rmi://localhost:1103/Broker3 -n rmi://localhost:1102/Broker2";
		String broker4url = "-uri rmi://localhost:1104/Broker4 -n rmi://localhost:1103/Broker3";

		brokerCore1 = new BrokerCore(broker1url);
		brokerCore2 = new BrokerCore(broker2url);
		brokerCore3 = new BrokerCore(broker3url);
		brokerCore4 = new BrokerCore(broker4url);
		
		brokerCore1.initialize();
		brokerCore2.initialize();
		brokerCore3.initialize();
		brokerCore4.initialize();


		
		super.setUp();
	}



	public void testAggregatedMessageRecievedOneBroker() throws ClientException, ParseException, BrokerCoreException, InterruptedException   {


		clientPub1 = new TestClient("ClientPub1");
		clientPub1.connect("rmi://localhost:1101/Broker1");

		clientSub1 = new TestClient("clientSub1");
		clientSub1.connect("rmi://localhost:1101/Broker1");

		clientPub1.advertise(MessageFactory.createAdvertisementFromString(
				"[class,eq,STOCK],[value,>,50]"), "rmi://localhost:1101/Broker1");
		Thread.sleep(1000L);
		clientSub1.subscribe(
				"[class,eq,STOCK],[value,>,60]", "rmi://localhost:1101/Broker1");
		Thread.sleep(1000L);
		clientPub1.publish(MessageFactory.createPublicationFromString(
				"[class,STOCK],[value,80]"),"rmi://localhost:1101/Broker1");
 
		Thread.sleep(10000L);
		 
		assertTrue( clientSub1.isMessageReceived());



	}
	

		public void testAggregatedMessageRecievedTwoBroker() throws ClientException, ParseException, BrokerCoreException, InterruptedException   {
	

			clientPub1 = new TestClient("ClientPub1");
			clientPub1.connect("rmi://localhost:1101/Broker1");

			clientSub1 = new TestClient("clientSub1");
			clientSub1.connect("rmi://localhost:1102/Broker2");

			clientPub1.advertise(MessageFactory.createAdvertisementFromString(
					"[class,eq,STOCK],[value,>,50]"), "rmi://localhost:1101/Broker1");
			Thread.sleep(1000L);
			clientSub1.subscribe(
					"[class,eq,STOCK],[value,>,60]", "rmi://localhost:1102/Broker2");
			Thread.sleep(1000L);
			clientPub1.publish(MessageFactory.createPublicationFromString(
					"[class,STOCK],[value,80]"),"rmi://localhost:1101/Broker1");
	 
			Thread.sleep(5000L);
			 
			assertTrue( clientSub1.isMessageReceived());
		}
		
		
		public void testOverlapped() throws ClientException, ParseException, InterruptedException {
			
			clientPub1 = new TestClient("ClientPub1");
			clientPub1.connect("rmi://localhost:1101/Broker1");

			clientSub1 = new TestClient("clientSub1");
			clientSub1.connect("rmi://localhost:1104/Broker4");
			

			clientSub2 = new TestClient("clientSub2");
			clientSub2.connect("rmi://localhost:1104/Broker4");

			clientPub1.advertise(MessageFactory.createAdvertisementFromString(
					"[class,eq,STOCK],[value,>,50]"), "rmi://localhost:1101/Broker1");
			Thread.sleep(1000L);
			
			
			clientSub1.subscribe(
					"[class,eq,STOCK],[value,>,60]", "rmi://localhost:1104/Broker4");
			clientSub2.subscribe("[class,eq,STOCK],[value,>,60],[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'5'],[NTF,eq,'1']", "rmi://localhost:1104/Broker4");
			
			Thread.sleep(1000L);
			
			
			clientPub1.publish(MessageFactory.createPublicationFromString(
					"[class,STOCK],[value,80]"),"rmi://localhost:1101/Broker1");
	 
			Thread.sleep(5000L);
			 
			assertTrue( clientSub1.isMessageReceived());
			assertEquals(1, clientSub1.getAllReceivedMessage().size());
			assertFalse(((PublicationMessage)clientSub1.getAllReceivedMessage().get(0)).getPublication() instanceof AggregatedPublication);  
			assertTrue( clientSub2.isMessageReceived());
			assertEquals(1, clientSub2.getAllReceivedMessage().size());
			assertTrue(((PublicationMessage)clientSub2.getAllReceivedMessage().get(0)).getPublication() instanceof AggregatedPublication);
		}
		
		
}