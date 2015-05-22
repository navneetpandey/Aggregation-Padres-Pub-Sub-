package ca.utoronto.msrg.padres.test.junit.aggregation;

import java.util.HashSet;
import java.util.Set;

import junit.framework.TestCase;
import ca.utoronto.msrg.padres.broker.aggregation.AdaptiveAggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.Notifier;
import ca.utoronto.msrg.padres.broker.aggregation.PublicationProfile;
import ca.utoronto.msrg.padres.broker.aggregation.adaptation.AdaptationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationMode;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationType;
import ca.utoronto.msrg.padres.broker.aggregation.aggregator.Aggregator;
import ca.utoronto.msrg.padres.broker.aggregation.operator.OperatorType;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.message.Message;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.Subscription;
import ca.utoronto.msrg.padres.common.message.SubscriptionMessage;
import ca.utoronto.msrg.padres.common.message.parser.MessageFactory;
import ca.utoronto.msrg.padres.common.message.parser.ParseException;
import ca.utoronto.msrg.padres.test.junit.aggregation.utility.TestClient;

public class AdaptiveAggregationTest extends TestCase {

	BrokerCore A, B, C, center;
	TestClient publisherA, publisherB, publisherC, publisherCenter;
	TestClient subscriberA, subscriberB, subscriberC, subscriberCenter;

	final String BROKER_CENTER = "rmi://localhost:1099/BrokerCenter";

	final String BROKER_A = "rmi://localhost:1098/BrokerA";
	final String BROKER_B = "rmi://localhost:1097/BrokerB";
	final String BROKER_C = "rmi://localhost:1096/BrokerC";

	@Override
	protected void setUp() throws Exception {
		System.setProperty("padres.aggregation.implementation",
				"ADAPTIVE_AGGREGATION");
		System.setProperty("aggregation.client", "OFF");

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

		C = new BrokerCore("-uri " + BROKER_C + " -n " + BROKER_CENTER);
		C.initialize();

		publisherC = new TestClient("Publisher C");
		subscriberC = new TestClient("Subscriber C");
		publisherC.connect(BROKER_C);
		subscriberC.connect(BROKER_C);

		super.setUp();
	}

	public void testInit() throws ClientException, ParseException,
			InterruptedException {

		assertTrue(B.getRouter().getPostProcessor().getAggregationEngine() instanceof AdaptiveAggregationEngine);

		AdaptiveAggregationEngine adaptiveAggregationEngine = (AdaptiveAggregationEngine) B.getRouter().getPostProcessor().getAggregationEngine();
		
		assertTrue(B.getBrokerConfig()
				.isAdaptationForAggregation());
		
		assertTrue(adaptiveAggregationEngine.getAdaptationEngine().getForcedMode()==null);
	}

	
	public void testAdaptationEngine() throws ParseException {
	   AdaptationEngine  adaptationEngine = new AdaptationEngine(true, null);
	   
	   //no immediate adaptation
	   assertTrue(!adaptationEngine.checkForAggregationTrigger());
	   
	   PublicationMessage pubMsg1 = new PublicationMessage(MessageFactory.createPublicationFromString("[class,STOCK],[value,80]"));
	   PublicationMessage pubMsg2 = new PublicationMessage(MessageFactory.createPublicationFromString("[class,STOCK],[value,80]"));
	   PublicationMessage pubMsg3 = new PublicationMessage(MessageFactory.createPublicationFromString("[class,STOCK],[value,80]"));
	   Subscription newSub = MessageFactory.createSubscriptionFromString("[class,eq,STOCK],[value,>,15]");
	   AggregationID aggID1 = new AggregationID(newSub.getPredicateMap(), 2, 2, "dummy",OperatorType.AVG, "BROKER") ;
	  // AggregationID aggID2 = new AggregationID(null, 5, 3, "dummy",OperatorType.AVG, null) ;
	   
	   assertTrue(adaptationEngine.getAdaptationSwitch().getAdapStatus().equals(AggregationMode.AGG_MODE));
	  
	   adaptationEngine.addNotificationPerAggregator(aggID1);
	   adaptationEngine.addNotificationPerAggregator(aggID1);
	   adaptationEngine.addPubMessage(pubMsg1);
	   assertTrue(adaptationEngine.getAdaptationSwitch().checkForAggregationTrigger());
	   assertTrue(!adaptationEngine.getAdaptationSwitch().checkForAggregationTrigger());
	  
	   
	   adaptationEngine.addPubMessage(pubMsg1);
	   adaptationEngine.addPubMessage(pubMsg1);
	   adaptationEngine.addPubMessage(pubMsg1);
	   assertTrue(adaptationEngine.getAdaptationSwitch().checkForAggregationTrigger());
	   assertTrue(!adaptationEngine.getAdaptationSwitch().checkForAggregationTrigger());

	 
	}
	
	public void testTransitionForTumblingWindowUnit() throws ParseException {
		Subscription dummySub = MessageFactory.createSubscriptionFromString("[class,eq,STOCK],[value,>,15]");
		AggregationID aggID1 = new AggregationID(dummySub.getPredicateMap(), 2, 2, "dummy",OperatorType.AVG, "BROKER") ;
		AggregationID aggID2 = new AggregationID(dummySub.getPredicateMap(), 2, 2, "dummy",OperatorType.AVG, "BROKER") ;
		AggregationID aggID3 = new AggregationID(dummySub.getPredicateMap(), 2, 2, "dummy",OperatorType.AVG, "BROKER") ;
		Notifier dummyNotifier = new Notifier(center);
		Aggregator aggregator1 = new Aggregator(dummyNotifier, aggID1,
				AggregationType.TIMEBASED, 0L, new MessageDestination("dummy")); 
		Aggregator aggregator2 = new Aggregator(dummyNotifier, aggID2,
				AggregationType.TIMEBASED, 0L, new MessageDestination("dummy")); 
		Aggregator aggregator3 = new Aggregator(dummyNotifier, aggID3,
						AggregationType.TIMEBASED, 0L, new MessageDestination("dummy")); 
		
		aggregator1.switchMode(AggregationMode.RAW_MODE);
		assertTrue(!aggregator1.isAggregationMode());
		aggregator1.switchMode(AggregationMode.AGG_MODE);
		assertTrue(aggregator1.isAggregationMode());
		
		
		Set<Aggregator> matchedAggregatorSet = new HashSet<Aggregator>();
		aggregator1.switchMode(AggregationMode.RAW_MODE);
		aggregator2.switchMode(AggregationMode.AGG_MODE);
		aggregator3.switchMode(AggregationMode.AGG_MODE);
		matchedAggregatorSet.add(aggregator1);
		matchedAggregatorSet.add(aggregator2);
		matchedAggregatorSet.add(aggregator3);

		PublicationMessage pubMsg = new PublicationMessage(MessageFactory.createPublicationFromString("[class,STOCK],[value,80]"));

		AdaptiveAggregationEngine adaptiveAggregationEngine1 = (AdaptiveAggregationEngine) B.getRouter().getPostProcessor().getAggregationEngine();

		PublicationProfile publicationProfile = adaptiveAggregationEngine1.getCurrentPublicationProfile();
		adaptiveAggregationEngine1.setMatchedAggregatorSet(matchedAggregatorSet);
		publicationProfile.setPublicationMessage(pubMsg);
		adaptiveAggregationEngine1.transition();
		assertTrue(pubMsg.getOnlyForTheseAggregators()!=null);
		assertTrue(pubMsg.getOnlyForTheseAggregators().size()==1);
		assertTrue(pubMsg.getOnlyForTheseAggregators().contains(aggregator1.getAggregationID()));
		
		adaptiveAggregationEngine1.getAdaptationEngine().setTransitionStartTime(System.currentTimeMillis());
		aggregator1.setTransitionON();
		assertTrue(!aggregator1.isAggregationMode());
		adaptiveAggregationEngine1.getNotificationFromAggregator(aggregator1);
		assertTrue(aggregator1.isAggregationMode());
		assertTrue(!aggregator1.isInTransitionMode());
		
	
		
		
		
	}
	
	public void testTransitionForTumblingWindowEndtoEnd() throws ParseException{
		Subscription aggSub1 = MessageFactory.createSubscriptionFromString("[class,eq,STOCK],[value,>,15]");
		aggSub1.setSubscriptionID("sub1");
		aggSub1.setAggregationID(new AggregationID(aggSub1.getPredicateMap(), 2, 2, "fieldname1",OperatorType.AVG, "BROKER")) ;
		aggSub1.setAggregation(true);
		
		Subscription aggSub2 = MessageFactory.createSubscriptionFromString("[class,eq,STOCK],[value,>,17]");
		aggSub2.setSubscriptionID("sub2");
		aggSub2.setAggregationID(new AggregationID(aggSub2.getPredicateMap(), 2, 2, "fieldname2",OperatorType.AVG, "BROKER")) ;
		aggSub2.setAggregation(true);
		
		//subscription as forwardMessage 
		SubscriptionMessage aggSubFwd = new SubscriptionMessage( MessageFactory.createSubscriptionFromString("[class,eq,STOCK],[value,>,17]"),"fwdSub");
		aggSubFwd.setNextHopID(new MessageDestination("nextBroker1"));
		Set<Message> messageSet = new HashSet<Message>();
		messageSet.add(aggSubFwd);
		
		AdaptiveAggregationEngine adaptiveAggregationEngine2 = (AdaptiveAggregationEngine) B.getRouter().getPostProcessor().getAggregationEngine();
		
		adaptiveAggregationEngine2.processSubscriptionMessage(new SubscriptionMessage(aggSub1, "sub1"), messageSet);
		
		aggSubFwd.setNextHopID(new MessageDestination("nextBroker2"));
		messageSet.clear();
		messageSet.add(aggSubFwd);
		
		adaptiveAggregationEngine2.processSubscriptionMessage(new SubscriptionMessage(aggSub2, "sub2"), messageSet);
		
		HashSet<Aggregator> aggregators = adaptiveAggregationEngine2.getAggregatorMap().get(aggSub1.getAggregationID());
		AggregationID rawAggregationID = null;
		Set<Aggregator> matchedAggregatorSet = new HashSet<Aggregator>();
		for( Aggregator aggregator : aggregators)  {
			aggregator.setTransitionON();
			aggregator.switchMode(AggregationMode.RAW_MODE);
			rawAggregationID = aggregator.getAggregationID();
			matchedAggregatorSet.add(aggregator);
		}	 
		
		aggregators = adaptiveAggregationEngine2.getAggregatorMap().get(aggSub2.getAggregationID());
		for( Aggregator aggregator : aggregators)  {
			aggregator.setTransitionOFF();
			matchedAggregatorSet.add(aggregator);
		}
		
		PublicationMessage pubMsg2 = new PublicationMessage(MessageFactory.createPublicationFromString("[class,STOCK],[value,80]"));
		
		PublicationProfile publicationProfile = adaptiveAggregationEngine2.getCurrentPublicationProfile();
		adaptiveAggregationEngine2.setMatchedAggregatorSet(matchedAggregatorSet);
		publicationProfile.setPublicationMessage(pubMsg2);

		adaptiveAggregationEngine2.transition();
		
		assertTrue(pubMsg2.getOnlyForTheseAggregators()!=null);
		assertTrue(pubMsg2.getOnlyForTheseAggregators().size()==1);
		assertTrue(pubMsg2.getOnlyForTheseAggregators().contains(rawAggregationID));
	
		
	}
	
	public void testTransitionForSlidingWindow() throws ParseException {
		Subscription dummySub = MessageFactory.createSubscriptionFromString("[class,eq,STOCK],[value,>,15]");
		AggregationID aggID1 = new AggregationID(dummySub.getPredicateMap(), 6, 2, "dummy",OperatorType.AVG, "BROKER") ;
		AggregationID aggID2 = new AggregationID(dummySub.getPredicateMap(), 4, 2, "dummy",OperatorType.AVG, "BROKER") ;
		AggregationID aggID3 = new AggregationID(dummySub.getPredicateMap(), 8, 2, "dummy",OperatorType.AVG, "BROKER") ;
		Notifier dummyNotifier = new Notifier(center);
		Aggregator aggregator1 = new Aggregator(dummyNotifier, aggID1,
				AggregationType.TIMEBASED, 0L, new MessageDestination("dummy")); 
		Aggregator aggregator2 = new Aggregator(dummyNotifier, aggID2,
				AggregationType.TIMEBASED, 0L, new MessageDestination("dummy")); 
		Aggregator aggregator3 = new Aggregator(dummyNotifier, aggID3,
						AggregationType.TIMEBASED, 0L, new MessageDestination("dummy")); 
		
		Set<Aggregator> matchedAggregatorSet = new HashSet<Aggregator>();
		aggregator1.switchMode(AggregationMode.RAW_MODE);
		aggregator2.switchMode(AggregationMode.AGG_MODE);
		aggregator3.switchMode(AggregationMode.AGG_MODE);
		matchedAggregatorSet.add(aggregator1);
		matchedAggregatorSet.add(aggregator2);
		matchedAggregatorSet.add(aggregator3);

		PublicationMessage pubMsg = new PublicationMessage(MessageFactory.createPublicationFromString("[class,STOCK],[value,80]"));

		AdaptiveAggregationEngine adaptiveAggregationEngine = (AdaptiveAggregationEngine) B.getRouter().getPostProcessor().getAggregationEngine();
		PublicationProfile publicationProfile = adaptiveAggregationEngine.getCurrentPublicationProfile();
		adaptiveAggregationEngine.setMatchedAggregatorSet(matchedAggregatorSet);
		publicationProfile.setPublicationMessage(pubMsg);
		adaptiveAggregationEngine.transition();
		//assertTrue(pubMsg.getOnlyForTheseAggregators()!=null);
	//	assertTrue(pubMsg.getOnlyForTheseAggregators().size()==1);
	//	assertTrue(pubMsg.getOnlyForTheseAggregators().contains(aggregator1.getAggregationID()));
		
		
	}
	
	 
	
	
//		publisherA
//				.advertise(
//						MessageFactory
//								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
//						BROKER_A);
//
//		publisherC
//				.advertise(
//						MessageFactory
//								.createAdvertisementFromString("[class,eq,STOCK],[value,>,10]"),
//						BROKER_C);
//		
//		
//		Thread.sleep(1000L);
//		
//		
//		subscriberB
//				.subscribe(
//						"[class,eq,STOCK],[value,>,15],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'4'],[NTF,eq,'2']",
//						BROKER_B);
//		long t = System.currentTimeMillis();
//		Thread.sleep(2000 - t % 2000L);
//		for (int i = 0; i < 10; i++) {
//			publisherA.publish(MessageFactory
//					.createPublicationFromString("[class,STOCK],[value,80]"),
//					BROKER_A);
//			publisherC.publish(MessageFactory
//					.createPublicationFromString("[class,STOCK],[value,800]"),
//					BROKER_C);
//			Thread.sleep(90);
//		}
//
//		Thread.sleep(10000);
//
//		System.out.println("\n\n\n");
//
//		int i = 0;
//
//		for(Message m:subscriberB.getReceivedMessage()){
//			System.out.println(i++);
//			assertTrue(m instanceof PublicationMessage);
//			assertTrue(((PublicationMessage)m).getPublication() instanceof AggregatedPublication);
//			System.out.println(((AggregatedPublication)((PublicationMessage)m).getPublication()).getAggResult());
//		}
//		System.out.println(i+" DONE");
//		
//		assertEquals(2, subscriberB.getReceivedMessage().size());
		
	
}
