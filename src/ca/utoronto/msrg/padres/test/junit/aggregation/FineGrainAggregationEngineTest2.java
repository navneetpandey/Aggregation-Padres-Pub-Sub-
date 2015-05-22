package ca.utoronto.msrg.padres.test.junit.aggregation;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import ca.utoronto.msrg.padres.broker.aggregation.FineGrainAdaptiveAggregationEngine; 
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.message.AggregatedPublication;
import ca.utoronto.msrg.padres.broker.aggregation.operator.OperatorType;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.message.Message;
import ca.utoronto.msrg.padres.common.message.Predicate;
import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.parser.MessageFactory;
import ca.utoronto.msrg.padres.common.message.parser.ParseException;
import ca.utoronto.msrg.padres.test.junit.aggregation.utility.TestClient;
import junit.framework.TestCase;

public class FineGrainAggregationEngineTest2 extends TestCase {
	BrokerCore A, B, center;
	TestClient publisherA;
	TestClient  subscriberB ;
	TestClient  subscriberC ;

	final String BROKER_CENTER = "rmi://localhost:1099/BrokerCenter";

	final String BROKER_A = "rmi://localhost:1098/BrokerA";
	final String BROKER_B = "rmi://localhost:1097/BrokerB"; 

	FineGrainAdaptiveAggregationEngine FGA;
	AggregationID aggID;
	Publication p;

	FineGrainAdaptiveAggregationEngine FGB;

	AggregationID id;

	@Override
	protected void setUp() throws Exception {
		System.setProperty("padres.aggregation.implementation", "FG_AGGREGATION");
		System.setProperty("aggregation.client", "OFF");
		 
		
		
		center = new BrokerCore("-uri " + BROKER_CENTER);
		center.initialize(); 


		A = new BrokerCore("-uri " + BROKER_A + " -n " + BROKER_CENTER);
		A.initialize();
		publisherA = new TestClient("Publisher A"); 
		publisherA.connect(BROKER_A); 

		
		B = new BrokerCore("-uri " + BROKER_B + " -n " + BROKER_CENTER);
		B.initialize(); 
		subscriberB = new TestClient("Subscriber B"); 
		subscriberB.connect(BROKER_B); 
		subscriberC = new TestClient("Subscriber C"); 
		subscriberC.connect(BROKER_B);

		FGA= (FineGrainAdaptiveAggregationEngine) A.getRouter().getPostProcessor().getAggregationEngine();

		FGB = (FineGrainAdaptiveAggregationEngine) B.getRouter().getPostProcessor().getAggregationEngine();

	
		Map<String, Predicate> map = new HashMap<String, Predicate>();
		id = new AggregationID(map, 6, 2, "", OperatorType.COUNT, "oue");

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
	
	public void advertiseSlidingSub() throws ClientException, ParseException, InterruptedException {
		publisherA.advertise(MessageFactory.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"), BROKER_A); 
		Thread.sleep(1000);
		subscriberB.subscribe("[class,eq,STOCK],[value,>,15],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'6'],[NTF,eq,'2']", BROKER_B);

		Thread.sleep(1000);
		aggID = FGA.getAggregationIDs().iterator().next();

	}
 
	public void advertiseTumblingSub() throws ClientException, ParseException, InterruptedException {
		publisherA.advertise(MessageFactory.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"), BROKER_A); 
		Thread.sleep(1000);
		subscriberB.subscribe("[class,eq,STOCK],[value,>,15],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'6'],[NTF,eq,'6']", BROKER_B);

		Thread.sleep(1000);
		aggID = FGA.getAggregationIDs().iterator().next();

	}
	
	public void advertiseOverlapSub() throws ClientException, ParseException, InterruptedException {
		
		publisherA.advertise(MessageFactory.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"), BROKER_A); 
		Thread.sleep(1000);
		subscriberB.subscribe("[class,eq,STOCK],[value,>,16]", BROKER_B);
		subscriberC.subscribe("[class,eq,STOCK],[value,>,15],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'6'],[NTF,eq,'6']", BROKER_B);
		

		Thread.sleep(2000);
		aggID = FGA.getAggregationIDs().iterator().next();

	}
	
 
	public void testProcessPublicationMessage_fwdDecision()  {

		try{
		advertiseSlidingSub(); 

		HashSet<AggregationID> aggIDs = new HashSet<AggregationID>();
		aggIDs.add(aggID); 
		
		Thread.sleep(aggID.getWindowSize() * 3000 - System.currentTimeMillis() % (aggID.getWindowSize() * 1000));
		p = MessageFactory.createPublicationFromString("[class,STOCK],[value,80]"); 

		PublicationMessage pm = new PublicationMessage(p);

		pm.setLastHopID(A.getBrokerDestination());
		pm.setAggregationRequired(true); 
		
		FGA.processPublicationMessage(pm,new HashSet<Message>());

	
		assertFalse(FGA.getCurrentPublicationProfile().getMatchedAggregationSet().isEmpty());

		assertTrue(aggIDs.contains(FGA.getCurrentPublicationProfile().getMatchedAggregationSet().iterator().next()));
		
		
		Thread.sleep(10000);
		assertEquals(1, subscriberB.getAllReceivedMessage().size());
		for(Message m:subscriberB.getAllReceivedMessage()){
			assertFalse(((PublicationMessage)m).getPublication() instanceof AggregatedPublication); 
		} 
 
 
		}
		catch(NullPointerException e){
			e.printStackTrace();
			assertTrue(false);
		}catch(Exception e){
			e.printStackTrace();
			assertTrue(false);
		}
	}
	
	public void testProcessPublicationMessage_OverlappedSub()  {

		try{
		advertiseOverlapSub();

		HashSet<AggregationID> aggIDs = new HashSet<AggregationID>();
		aggIDs.add(aggID); 
		
		Thread.sleep(aggID.getWindowSize() * 3000 - System.currentTimeMillis() % (aggID.getWindowSize() * 1000));
		p = MessageFactory.createPublicationFromString("[class,STOCK],[value,80]"); 

		PublicationMessage pm = new PublicationMessage(p);

		pm.setLastHopID(A.getBrokerDestination());
		pm.setAggregationRequired(true); 
		
		FGA.processPublicationMessage(pm,new HashSet<Message>());

	
		assertFalse(FGA.getCurrentPublicationProfile().getMatchedAggregationSet().isEmpty());

		assertTrue(aggIDs.contains(FGA.getCurrentPublicationProfile().getMatchedAggregationSet().iterator().next()));
		
		
		Thread.sleep(10000);
//		assertEquals(1, subscriberB.getAllReceivedMessage().size());
//		for(Message m:subscriberB.getAllReceivedMessage()){
//			assertFalse(((PublicationMessage)m).getPublication() instanceof AggregatedPublication); 
//		} 
 
		assertEquals(1, subscriberC.getAllReceivedMessage().size());
		for(Message m:subscriberC.getAllReceivedMessage()){
			assertTrue(((PublicationMessage)m).getPublication() instanceof AggregatedPublication); 
		} 
 
		}
		catch(NullPointerException e){
			e.printStackTrace();
			assertTrue(false);
		}catch(Exception e){
			e.printStackTrace();
			assertTrue(false);
		}
	}
	
	public void testProcessPublicationMessage_AGRDecision()  {

		try{
		advertiseSlidingSub(); 

		HashSet<AggregationID> aggIDs = new HashSet<AggregationID>();
		aggIDs.add(aggID); 
		
		Thread.sleep(aggID.getShiftSize() * 1000 - System.currentTimeMillis() % (aggID.getShiftSize() * 1000));
		for(int i=0;i<5;i++){
			PublicationMessage pm = new PublicationMessage(MessageFactory.createPublicationFromString(
					"[class,STOCK],[value,"+(11+10*i)+"]"));

			pm.setLastHopID(A.getBrokerDestination());
			pm.setAggregationRequired(true); 
			
			FGA.processPublicationMessage(pm,new HashSet<Message>()); 
			Thread.sleep(20);
		} 

		

	 
		
		
		Thread.sleep(10000);
		assertEquals(3, subscriberB.getAllReceivedMessage().size());
		for(Message m:subscriberB.getAllReceivedMessage()){
			assertTrue(((PublicationMessage)m).getPublication() instanceof AggregatedPublication);
			assertEquals("144",((AggregatedPublication)((PublicationMessage)m).getPublication()).getAggResult());
			System.out.println(((AggregatedPublication)((PublicationMessage)m).getPublication()).getAggResult());
		} 
 
 
		}
		catch(NullPointerException e){
			e.printStackTrace();
			assertTrue(false);
		}catch(Exception e){
			e.printStackTrace();
			assertTrue(false);
		}
	}
	
	public void testProcessPublicationMessage_LatePub()  {

		try{
		advertiseSlidingSub(); 

		HashSet<AggregationID> aggIDs = new HashSet<AggregationID>();
		aggIDs.add(aggID); 
		
		Thread.sleep(aggID.getShiftSize() * 1000 - System.currentTimeMillis() % (aggID.getShiftSize() * 1000));
		for(int i=0;i<5;i++){
			Publication p = MessageFactory.createPublicationFromString(
					"[class,STOCK],[value,"+(11+10*i)+"]");
			p.setTimeStamp(new Date(System.currentTimeMillis() - (1 * 60 * 60 * 1000)));
			PublicationMessage pm = new PublicationMessage(p);

			pm.setLastHopID(A.getBrokerDestination());
			pm.setAggregationRequired(true); 
			
			FGA.processPublicationMessage(pm,new HashSet<Message>()); 
			Thread.sleep(20);
		} 

		

	 
		
		
		Thread.sleep(10000);
		assertEquals(4, subscriberB.getAllReceivedMessage().size());
		for(Message m:subscriberB.getAllReceivedMessage()){
			assertFalse(((PublicationMessage)m).getPublication() instanceof AggregatedPublication); 
		} 
 
 
		}
		catch(NullPointerException e){
			e.printStackTrace();
			assertTrue(false);
		}catch(Exception e){
			e.printStackTrace();
			assertTrue(false);
		}
	}

}
