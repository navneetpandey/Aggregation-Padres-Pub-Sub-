package ca.utoronto.msrg.padres.test.junit.aggregation;

import java.util.Date;
import java.util.HashSet;

import junit.framework.TestCase;
import ca.utoronto.msrg.padres.broker.aggregation.FineGrainAdaptiveAggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.OptimalAggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.message.AggregatedPublication;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.message.Message;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.parser.MessageFactory;
import ca.utoronto.msrg.padres.common.message.parser.ParseException;
import ca.utoronto.msrg.padres.test.junit.aggregation.utility.TestClient;

public class FineGrainAggregationEngineTest extends TestCase {

	BrokerCore A, B, C, center;
	TestClient publisherA, publisherB, publisherC, publisherCenter;
	TestClient subscriberA, subscriberB, subscriberC, subscriberCenter;
	
	final String BROKER_CENTER="rmi://localhost:1099/BrokerCenter";
	
	final String BROKER_A="rmi://localhost:1098/BrokerA";
	final String BROKER_B="rmi://localhost:1097/BrokerB";
	final String BROKER_C="rmi://localhost:1096/BrokerC";
	
	@Override
	protected void setUp() throws Exception {
		System.setProperty("aggregation.client", "OFF");

		//System.setProperty("padres.aggregation.implementation", "OPTIMAL_AGGREGATION" );
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
	 * Sa - A
	 *       \
	 *        Center -- B - Pb/c
	 *       /
	 * Sc - C
	 * */
	public void testSimple() throws ClientException, ParseException, InterruptedException{
		

		publisherB.advertise(MessageFactory.createAdvertisementFromString(
				"[class,eq,STOCK],[value,>,0]"), BROKER_B);

		Thread.sleep(1000L);

		subscriberA.subscribe(
				"[class,eq,STOCK],[value,>,10],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'6'],[NTF,eq,'2']", BROKER_A);
		subscriberC.subscribe(
				"[class,eq,STOCK],[value,>,35],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'6'],[NTF,eq,'2']", BROKER_C);
		
		
		
		Thread.sleep(12000-System.currentTimeMillis()%6000L);
		
		for(int i=0;i<5;i++){
			publisherB.publish(MessageFactory.createPublicationFromString(
					"[class,STOCK],[value,"+(11+10*i)+"]"), BROKER_B);
			Thread.sleep(30);
		}
		
		Thread.sleep(12000);
		
		
		System.out.println("\n\n\n");
		
		assertEquals(3, subscriberA.getAllReceivedMessage().size());
		for(Message m:subscriberA.getAllReceivedMessage()){
			assertTrue(((PublicationMessage)m).getPublication() instanceof AggregatedPublication);
			assertEquals("155",((AggregatedPublication)((PublicationMessage)m).getPublication()).getAggResult());
			System.out.println(((AggregatedPublication)((PublicationMessage)m).getPublication()).getAggResult());
		}
		assertEquals(2, subscriberC.getAllReceivedMessage().size());
		for(Message m:subscriberC.getAllReceivedMessage()){
			assertFalse(((PublicationMessage)m).getPublication() instanceof AggregatedPublication);
			
		}

		//assertEquals(3, subscriberB.getReceivedMessage().size());
	}
	
	
	public void testLateMessage() throws ClientException, ParseException, InterruptedException{
		

		publisherB.advertise(MessageFactory.createAdvertisementFromString(
				"[class,eq,STOCK],[value,>,0]"), BROKER_B);

		Thread.sleep(1000L);

		subscriberA.subscribe(
				"[class,eq,STOCK],[value,>,10],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'6'],[NTF,eq,'2']", BROKER_A); 
		
		Thread.sleep(3000);
		FineGrainAdaptiveAggregationEngine FG = (FineGrainAdaptiveAggregationEngine) B.getRouter().getPostProcessor().getAggregationEngine();
		PublicationMessage pubM = new PublicationMessage(MessageFactory.createPublicationFromString( "[class,STOCK],[value,"+(11+10)+"]"));
		//set time stamp of 1 hour ago
		pubM.getPublication().setTimeStamp(new Date(System.currentTimeMillis() - (1 * 60 * 60 * 1000)));
		FG.processPublicationMessage(pubM,new HashSet<Message>());
	//	FG.getCurrentPublicationProfile().setPublicationMessage(pubM);
		for (MessageDestination md : FG.getCurrentPublicationProfile().getPureAggregateSubDestination()) {
	 
				FG.updateMatchedAggregatorSet();
				FG.aggregate(md);
		}
		FG.notifyFromAggregatorTimer(0);
		Thread.sleep(12000-System.currentTimeMillis()%6000L);
	 
	
		Thread.sleep(12000);
		
		
		System.out.println("\n\n\n");
		
		assertEquals(3, subscriberA.getAllReceivedMessage().size());
		for(Message m:subscriberA.getAllReceivedMessage()){
			assertTrue(((PublicationMessage)m).getPublication() instanceof AggregatedPublication);
			assertEquals("155",((AggregatedPublication)((PublicationMessage)m).getPublication()).getAggResult());
			System.out.println(((AggregatedPublication)((PublicationMessage)m).getPublication()).getAggResult());
		}
		assertEquals(2, subscriberC.getAllReceivedMessage().size());
		for(Message m:subscriberC.getAllReceivedMessage()){
			assertFalse(((PublicationMessage)m).getPublication() instanceof AggregatedPublication);
			
		}

		//assertEquals(3, subscriberB.getReceivedMessage().size());
	}
	
}
