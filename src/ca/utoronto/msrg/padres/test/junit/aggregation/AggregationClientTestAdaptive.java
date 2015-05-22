package ca.utoronto.msrg.padres.test.junit.aggregation;

import java.net.UnknownHostException;

import junit.framework.TestCase;
import ca.utoronto.msrg.padres.broker.aggregation.AdaptiveAggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationMode;
import ca.utoronto.msrg.padres.broker.aggregation.message.AggregatedPublication;
import ca.utoronto.msrg.padres.broker.aggregation.operator.OperatorType;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCoreException;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.Subscription;
import ca.utoronto.msrg.padres.common.message.parser.MessageFactory;
import ca.utoronto.msrg.padres.common.message.parser.ParseException;
import ca.utoronto.msrg.padres.test.junit.aggregation.utility.TestAggregatorClient;
import ca.utoronto.msrg.padres.test.junit.aggregation.utility.TestClient;

public class AggregationClientTestAdaptive extends TestCase {

	BrokerCore brokerCore1;
	BrokerCore brokerCore2;
	BrokerCore brokerCore3;
	BrokerCore brokerCore4;

	TestAggregatorClient clientPub1;
	// TestClient clientPub2;
	TestAggregatorClient clientSub1;
	
	TestAggregatorClient clientSub2;

	// TestClient clientSub2;

	@Override
	protected void setUp() throws Exception {

		/*System.setProperty("padres.aggregation.implementation",
				"EARLY_AGGREGATION");*/
		System.setProperty("padres.aggregation.implementation",
				"ADAPTIVE_AGGREGATION");
		/*System.setProperty("padres.aggregation.implementation",
				"OPTIMAL_AGGREGATION");*/
		System.setProperty("aggregation.client", "ON");
		if(System.getProperty("padres.aggregation.implementation")==null)assertTrue(false);
		
		String broker1url = "-uri rmi://localhost:1101/Broker1";
		/*String broker2url = "-uri rmi://localhost:1102/Broker2 -n rmi://localhost:1101/Broker1";
		String broker3url = "-uri rmi://localhost:1103/Broker3 -n rmi://localhost:1102/Broker2";
		String broker4url = "-uri rmi://localhost:1104/Broker4 -n rmi://localhost:1103/Broker3";*/

		brokerCore1 = new BrokerCore(broker1url);
		/*brokerCore2 = new BrokerCore(broker2url);
		brokerCore3 = new BrokerCore(broker3url);
		brokerCore4 = new BrokerCore(broker4url);*/

		brokerCore1.initialize();
		/*brokerCore2.initialize();
		brokerCore3.initialize();
		brokerCore4.initialize();*/

		brokerCore1.getBrokerConfig().getAggregationInfo().setClientAggregation(true);
		/*brokerCore2.getBrokerConfig().getAggregationInfo().setClientAggregation(true);
		brokerCore3.getBrokerConfig().getAggregationInfo().setClientAggregation(true);
		brokerCore4.getBrokerConfig().getAggregationInfo().setClientAggregation(true);*/
		super.setUp();
	}

		
	public void testMessageRecievedOneBrokerWithSinglePublication()
			throws ClientException, ParseException, BrokerCoreException,
			InterruptedException, UnknownHostException {

		clientPub1 = new TestAggregatorClient("ClientPub1");
		clientPub1.connect("rmi://localhost:1101/Broker1");
 
		clientSub1 = new TestAggregatorClient("clientSub1");
		clientSub1.connect("rmi://localhost:1101/Broker1");

		clientSub2 = new TestAggregatorClient("clientSub2");
		clientSub2.connect("rmi://localhost:1101/Broker1");
		clientPub1
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1101/Broker1");
		Thread.sleep(1000-System.currentTimeMillis()%1000);
		clientSub1.subscribe(
				"[class,eq,STOCK],[value,>,60],[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'5'],[NTF,eq,'1']", "rmi://localhost:1101/Broker1");
		Thread.sleep(100L);

		clientSub2.subscribe(
				"[class,eq,STOCK],[value,>,70],[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'5'],[NTF,eq,'5']", "rmi://localhost:1101/Broker1");
		Thread.sleep(100L);
		Thread.sleep(1000-System.currentTimeMillis()%1000);
		
		Publication  pub= MessageFactory
						.createPublicationFromString("[class,STOCK],[value,80]");
		
		clientPub1.publish(pub);
		Thread.sleep(10000L);
		assertEquals(5,clientSub1.getAggregateMessageReceived());
		assertEquals(1,clientSub2.getAggregateMessageReceived());
		assertEquals(0,clientSub1.getNonAggregateMessageReceived());
		assertEquals(0,clientSub2.getNonAggregateMessageReceived());

	}
	
	public void testMessageRecievedOneBrokerWithAggregateMode()
			throws ClientException, ParseException, BrokerCoreException,
			InterruptedException, UnknownHostException {

		clientPub1 = new TestAggregatorClient("ClientPub1");
		clientPub1.connect("rmi://localhost:1101/Broker1");
 
		clientSub1 = new TestAggregatorClient("clientSub1");
		clientSub1.connect("rmi://localhost:1101/Broker1");

		/*clientSub2 = new TestAggregatorClient("clientSub2");
		clientSub2.connect("rmi://localhost:1101/Broker1");
		*/
		clientPub1
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1101/Broker1");
		
		clientSub1.subscribe(
				"[class,eq,STOCK],[value,>,60],[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'5'],[NTF,eq,'1']", "rmi://localhost:1101/Broker1");
		Thread.sleep(5100L);

		Thread.sleep(1000-System.currentTimeMillis()%1000);
		
		for(int i=0;i<7;i++){
			clientPub1.publish(MessageFactory.createPublicationFromString(
					"[class,STOCK],[value,"+((i+1)*70)+"]"));
			Thread.sleep(50);
		}
		Thread.sleep(30000);
		assertEquals(0,clientSub1.getNonAggregateMessageReceived());
		assertEquals(5, clientSub1.getAggregateMessageReceived());
	
		clientSub1.resetAggregateMessageReceivedCounter();
		
//
//		for(int i=1;i<10;i++){
//			clientPub1.publish(MessageFactory.createPublicationFromString(
//					"[class,STOCK],[value,"+i*70+"]"));
//			Thread.sleep(50);
//		}
//		Thread.sleep(25000);
//		assertEquals(7, clientSub1.getAggregateMessageReceived());
//		
//
//		assertEquals(0,clientSub1.getNonAggregateMessageReceived());
//		assertEquals(0,clientSub2.getNonAggregateMessageReceived());
	}
	

	public void testMessageRecievedOneBrokerWithForwardMode()
			throws ClientException, ParseException, BrokerCoreException,
			InterruptedException, UnknownHostException {
		((AdaptiveAggregationEngine) brokerCore1.getRouter().getPostProcessor().getAggregationEngine()).getAdaptationEngine().setForcedMode(AggregationMode.RAW_MODE);
		clientPub1 = new TestAggregatorClient("ClientPub1");
		clientPub1.connect("rmi://localhost:1101/Broker1");
 
		clientSub1 = new TestAggregatorClient("clientSub1");
		clientSub1.connect("rmi://localhost:1101/Broker1");

		clientSub2 = new TestAggregatorClient("clientSub2");
		clientSub2.connect("rmi://localhost:1101/Broker1");
		
		clientPub1
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1101/Broker1");
		
		clientSub1.subscribe(
				"[class,eq,STOCK],[value,>,60],[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'5'],[NTF,eq,'1']", "rmi://localhost:1101/Broker1");
		Thread.sleep(1000L);

		for(int i=1;i<2;i++){
			clientPub1.publish(MessageFactory.createPublicationFromString(
					"[class,STOCK],[value,"+i*70+"]"));
			Thread.sleep(100);
		}
		//Thread.sleep(5000);

		Thread.sleep(20000L);
		assertEquals(1,clientSub1.getAggregateMessageReceived());
		//assertEquals(0,clientSub1.getAggregateMessageReceived());
	}
	
}
