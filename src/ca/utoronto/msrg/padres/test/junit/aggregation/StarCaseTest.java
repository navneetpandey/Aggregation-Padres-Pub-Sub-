package ca.utoronto.msrg.padres.test.junit.aggregation;

import junit.framework.TestCase;
import ca.utoronto.msrg.padres.broker.aggregation.OptimalAggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.message.AggregatedPublication;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.message.Message;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.parser.MessageFactory;
import ca.utoronto.msrg.padres.common.message.parser.ParseException;
import ca.utoronto.msrg.padres.test.junit.aggregation.utility.TestClient;

public class StarCaseTest extends TestCase {

	BrokerCore A, B, C, center;
	TestClient publisherA, publisherB, publisherC, publisherCenter;
	TestClient subscriberA, subscriberB, subscriberC, subscriberCenter;
	
	final String BROKER_CENTER="rmi://localhost:1099/BrokerCenter";
	
	final String BROKER_A="rmi://localhost:1098/BrokerA";
	final String BROKER_B="rmi://localhost:1097/BrokerB";
	final String BROKER_C="rmi://localhost:1096/BrokerC";
	
	/*
	 * A
	 *  \
	 *    Center -- B
	 *  /
	 * C
	 * */

	@Override
	protected void setUp() throws Exception {
		System.setProperty("aggregation.client", "OFF");

		//System.setProperty("padres.aggregation.implementation", "OPTIMAL_AGGREGATION" );
		//System.setProperty("padres.aggregation.implementation", "EARLY_AGGREGATION" );
		
		System.setProperty("padres.aggregation.implementation", "FG_AGGREGATION" );
		
		
		if(System.getProperty("padres.aggregation.implementation")==null)assertTrue(false);
		center=new BrokerCore("-uri "+BROKER_CENTER);
		center.initialize();
		publisherCenter=new TestClient("Publisher CENTER");
		subscriberCenter=new TestClient("Subscriber CENTER");
		publisherCenter.connect(BROKER_CENTER);
		subscriberCenter.connect(BROKER_CENTER);
		
		A=new BrokerCore("-uri "+BROKER_A+" -n "+BROKER_CENTER);
		A.initialize();

		publisherA=new TestClient("Publisher A");
		subscriberA=new TestClient("Subscriber A");
		publisherA.connect(BROKER_A);
		subscriberA.connect(BROKER_A);
		
		B=new BrokerCore("-uri "+BROKER_B+" -n "+BROKER_CENTER);
		B.initialize();

		publisherB=new TestClient("Publisher B");
		subscriberB=new TestClient("Subscriber B");
		publisherB.connect(BROKER_B);
		subscriberB.connect(BROKER_B);
		
		C=new BrokerCore("-uri "+BROKER_C+" -n "+BROKER_CENTER);
		C.initialize();

		publisherC=new TestClient("Publisher C");
		subscriberC=new TestClient("Subscriber C");
		publisherC.connect(BROKER_C);
		subscriberC.connect(BROKER_C);
		
		
		
		
		super.setUp();
	}

	
	
	@Override
	protected void tearDown() throws Exception {
		
		A.shutdown();
		B.shutdown();
		C.shutdown();
		center.shutdown();
		Thread.sleep(2000);
		super.tearDown();
	}
	
	
	/*
	 * Pa - A
	 *       \
	 *        Center -- B - Sa/c
	 *       /
	 * Pc - C
	 * */
	public void testSimple() throws ClientException, ParseException, InterruptedException{
		if(System.getProperty("padres.aggregation.implementation").equals("OPTIMAL_AGGREGATION") ){
			((OptimalAggregationEngine)C.getRouter().getPostProcessor().getAggregationEngine()).COLLECTING_TIMEOUT=100;
			((OptimalAggregationEngine)A.getRouter().getPostProcessor().getAggregationEngine()).COLLECTING_TIMEOUT=100;
			((OptimalAggregationEngine)center.getRouter().getPostProcessor().getAggregationEngine()).COLLECTING_TIMEOUT=10;
			((OptimalAggregationEngine)B.getRouter().getPostProcessor().getAggregationEngine()).COLLECTING_TIMEOUT=100;
		}

		publisherA.advertise(MessageFactory.createAdvertisementFromString(
				"[class,eq,STOCK],[value,>,50]"), BROKER_A);
		
		publisherC.advertise(MessageFactory.createAdvertisementFromString(
				"[class,eq,STOCK],[value,>,10]"), BROKER_C);
		Thread.sleep(1000L);
		subscriberB.subscribe(
				"[class,eq,STOCK],[value,>,15],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'6'],[NTF,eq,'2']", BROKER_B);
		
		
		
		Thread.sleep(12000-System.currentTimeMillis()%6000L);
		
		for(int i=0;i<10;i++){
			publisherA.publish(MessageFactory.createPublicationFromString(
					"[class,STOCK],[value,80]"), BROKER_A);
			publisherC.publish(MessageFactory.createPublicationFromString(
					"[class,STOCK],[value,800]"), BROKER_C);
			Thread.sleep(30);
		}
		
		Thread.sleep(12000);
		
		
		System.out.println("\n\n\n");
		
		for(Message m:subscriberB.getAllReceivedMessage()){
			System.out.println(((AggregatedPublication)((PublicationMessage)m).getPublication()).getAggResult());
		}
		

		assertEquals(3, subscriberB.getAllReceivedMessage().size());
	}

	public void testB() throws ClientException, ParseException, InterruptedException{
		
		if(System.getProperty("padres.aggregation.implementation").equals("OPTIMAL_AGGREGATION") ){
			((OptimalAggregationEngine)C.getRouter().getPostProcessor().getAggregationEngine()).COLLECTING_TIMEOUT=150;
			((OptimalAggregationEngine)A.getRouter().getPostProcessor().getAggregationEngine()).COLLECTING_TIMEOUT=150;
			((OptimalAggregationEngine)center.getRouter().getPostProcessor().getAggregationEngine()).COLLECTING_TIMEOUT=100;
			((OptimalAggregationEngine)B.getRouter().getPostProcessor().getAggregationEngine()).COLLECTING_TIMEOUT=150;
		}
		
		System.out.println("StarCase: testB FOR "+System.getProperty("padres.aggregation.implementation"));
		
		publisherA.advertise(MessageFactory.createAdvertisementFromString(
				"[class,eq,STOCK],[value,>,50]"), BROKER_A);
		publisherB.advertise(MessageFactory.createAdvertisementFromString(
				"[class,eq,STOCK],[value,>,30]"), BROKER_B);
		publisherC.advertise(MessageFactory.createAdvertisementFromString(
				"[class,eq,STOCK],[value,>,10]"), BROKER_C);
		
		
		Thread.sleep(1000L);
		subscriberA.subscribe(
				"[class,eq,STOCK],[value,>,1],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'5'],[NTF,eq,'5']", BROKER_A);
		subscriberB.subscribe(
				"[class,eq,STOCK],[value,>,5],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'5'],[NTF,eq,'5']", BROKER_B);
		subscriberC.subscribe(
				"[class,eq,STOCK],[value,>,15],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'5'],[NTF,eq,'5']", BROKER_C);
		
		
		Thread.sleep(500L);
		
		Thread.sleep(5000-System.currentTimeMillis()%5000);
		
		for(int i=0;i<10;i++){
			publisherA.publish(MessageFactory.createPublicationFromString(
					"[class,STOCK],[value,100]"), BROKER_A);
			Thread.sleep(2);
			publisherB.publish(MessageFactory.createPublicationFromString(
					"[class,STOCK],[value,1000]"), BROKER_B);
			Thread.sleep(2);
			publisherC.publish(MessageFactory.createPublicationFromString(
					"[class,STOCK],[value,10000]"), BROKER_C);
			Thread.sleep(2);
		}
		
/*
		A.startAggregationTimers();
		B.startAggregationTimers();
		C.startAggregationTimers();
		center.startAggregationTimers();
*/		
	
		Thread.sleep(20000); // 10000 for optimal
		System.out.println("\nA\n\n");
		int res=0;
		for(Message m:subscriberA.getAllReceivedMessage()){
			assertTrue(((PublicationMessage)m).getPublication() instanceof AggregatedPublication);
			System.out.println("result "+( (AggregatedPublication)((PublicationMessage)m).getPublication()).getAggResult());
			System.err.println(m.getLastHopID().toString() + " content " + ((PublicationMessage)m).getPublication().toString());
			res+=Integer.parseInt(((AggregatedPublication)((PublicationMessage)m).getPublication()).getAggResult());
		}
		assertEquals(111000, res);
		res=0;
		System.out.println("\nB\n\n");
		
		for(Message m:subscriberB.getAllReceivedMessage()){
			System.out.println(((AggregatedPublication)((PublicationMessage)m).getPublication()).getAggResult());
			res+=Integer.parseInt(((AggregatedPublication)((PublicationMessage)m).getPublication()).getAggResult());
		}
		assertEquals(111000, res);
		res=0;
		System.out.println("\nC\n\n");
		
		for(Message m:subscriberC.getAllReceivedMessage()){
			System.out.println(((AggregatedPublication)((PublicationMessage)m).getPublication()).getAggResult());
			res+=Integer.parseInt(((AggregatedPublication)((PublicationMessage)m).getPublication()).getAggResult());
		}
		assertEquals(111000, res);

		if(System.getProperty("padres.aggregation.implementation").equals("OPTIMAL_AGGREGATION")||System.getProperty("padres.aggregation.implementation").equals("FG_AGGREGATION"))
			assertEquals(2, subscriberB.getAllReceivedMessage().size());
		else
			assertEquals(1, subscriberB.getAllReceivedMessage().size());
		
		
		
		
	}

}
