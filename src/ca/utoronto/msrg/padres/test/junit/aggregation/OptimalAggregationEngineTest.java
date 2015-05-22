package ca.utoronto.msrg.padres.test.junit.aggregation;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;
import no.uio.ifi.graph.aggregation.TimeConstraintBipartiteGraph;
import ca.utoronto.msrg.padres.broker.aggregation.NWR;
import ca.utoronto.msrg.padres.broker.aggregation.OptimalAggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.OptimalAggregationGraphNode;
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

public class OptimalAggregationEngineTest extends TestCase {

	BrokerCore A, B, C, center;
	TestClient publisherA, publisherB, publisherC, publisherCenter;
	TestClient subscriberA, subscriberB, subscriberC, subscriberCenter;

	final String BROKER_CENTER = "rmi://localhost:1099/BrokerCenter";

	final String BROKER_A = "rmi://localhost:1098/BrokerA";
	final String BROKER_B = "rmi://localhost:1097/BrokerB";
	final String BROKER_C = "rmi://localhost:1096/BrokerC";

	OptimalAggregationEngine opt;
	AggregationID aggID, aggID2;
	Publication p;

	OptimalAggregationEngine optB;

	AggregationID id;

	@Override
	protected void setUp() throws Exception {
		System.setProperty("padres.aggregation.implementation", "OPTIMAL_AGGREGATION");
		System.setProperty("aggregation.client", "OFF");
		
		OptimalAggregationEngine.DELAY=0L;
		OptimalAggregationEngine.COLLECTING_TIMEOUT=200;
		
		
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

		opt = (OptimalAggregationEngine) A.getRouter().getPostProcessor().getAggregationEngine();

		optB = (OptimalAggregationEngine) B.getRouter().getPostProcessor().getAggregationEngine();

	
		Map<String, Predicate> map = new HashMap<String, Predicate>();
		id = new AggregationID(map, 6, 2, "", OperatorType.COUNT, "oue");

		super.setUp();
	}
//This is require somewhere pm.setAggregationRequired(true); 

	@Override
	protected void tearDown() throws Exception {
		
		A.shutdown();
		B.shutdown();
		C.shutdown();
		center.shutdown();
		Thread.sleep(2000);
		super.tearDown();
	}
	

	
	public void advertiseSub() throws ClientException, ParseException, InterruptedException {
		publisherA.advertise(MessageFactory.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"), BROKER_A);
		publisherC.advertise(MessageFactory.createAdvertisementFromString("[class,eq,STOCK],[value,>,20]"), BROKER_C);
		subscriberB.subscribe("[class,eq,STOCK],[value,>,15],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'4'],[NTF,eq,'2']", BROKER_B);

		Thread.sleep(2000);
		aggID = opt.getAggregationIDs().iterator().next();

	}

	public void sub2() throws ClientException, ParseException, InterruptedException {

		subscriberC.subscribe("[class,eq,STOCK],[value,>,15],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'6'],[NTF,eq,'2']", BROKER_C);

		Thread.sleep(4000);
		Iterator<AggregationID> it = opt.getAggregationIDs().iterator();
		aggID2 = it.next();
		if (aggID2.equals(aggID))
			aggID2 = it.next();

	}

	private void publish() throws ClientException, ParseException, InterruptedException {
		p = MessageFactory.createPublicationFromString("[class,STOCK],[value,80]");
		Thread.sleep(aggID.getWindowSize() * 1000 - System.currentTimeMillis() % (aggID.getWindowSize() * 1000));
		p.setTimeStamp(new Date(System.currentTimeMillis() + 100));
		opt.getCurrentPublicationProfile().setPublicationMessage(new PublicationMessage(p));
	}

	public void testWindowTime() throws ClientException, ParseException, InterruptedException {
		advertiseSub();

		assertEquals(8000, opt.getWindowStartTime(8500, aggID));
		assertEquals(10000, opt.getShiftTime(8500, aggID));

		assertEquals(10000, opt.getNextNotificationTime(8500, aggID));

		assertEquals(8000, opt.getWindowStartTime(8500, id));

		publish();
		NWR n = new NWR(p, id);
		long curTime = System.currentTimeMillis();
		n.setTimeStamp(new Date(curTime +1000)); // NWR expires in 1 sec
		List<NWR> nwrs = new ArrayList<NWR>();
		nwrs.add(n);
		// This test fails because of wrong initialization. Should be fixed asap
		opt.getDelay().put(id, 0L);
		assertFalse(opt.isPublicationLate(nwrs));
		Thread.sleep(500);
		assertFalse(opt.isPublicationLate(nwrs));
		Thread.sleep(501);
		assertTrue(opt.isPublicationLate(nwrs));
		//assertTrue(opt.isPublicationLate(curTime - 6100, nwrs));
	}

	public void testProcessSubscriptionMessage() throws ClientException, ParseException, InterruptedException {


		assertTrue(opt.getGraphs().isEmpty());

		advertiseSub();

		assertEquals(1, opt.getAggregationIDs().size());
		assertEquals(1, opt.getGraphs().size());
		assertNull(opt.getGraphs().get(A.getBrokerDestination()));
		assertNull(opt.getGraphs().get(B.getBrokerDestination()));
		assertNull(opt.getGraphs().get(C.getBrokerDestination()));
		assertNotNull(opt.getGraphs().get(center.getBrokerDestination()));
		assertEquals((long) OptimalAggregationEngine.DEFAULT_TIMEOUT, (long) opt.getDelay().get(aggID));

	}

	public void testGetNWR() throws ClientException, ParseException, InterruptedException {

		advertiseSub();

		publish();
		
		NWR n = opt.getNWR(System.currentTimeMillis() - (System.currentTimeMillis() % (aggID.getWindowSize() * 1000))
				+ (aggID.getWindowSize() * 1000), aggID, center.getBrokerDestination());

		assertNotNull(n);
		assertEquals(System.currentTimeMillis() - (System.currentTimeMillis() % (aggID.getWindowSize() * 1000)) + aggID.getWindowSize() * 1000, n
				.getTimeStamp().getTime());

		assertEquals(n.getMessageDestination(), center.getBrokerDestination());
	//	assertTrue(opt.getCurrentNWRs().iterator().next().get().equals(n));

	}

	public void testGetNWRs() throws ClientException, ParseException, InterruptedException {

		advertiseSub();
		publish();


		PublicationMessage pm = new PublicationMessage(p);
		long pubTime = System.currentTimeMillis();
		pm.setLastHopID(A.getBrokerDestination());
		pm.setNextHopID(B.getBrokerDestination());

		opt.getCurrentPublicationProfile().setPublicationMessage(pm);
		opt.getCurrentPublicationProfile().getMatchedAggregationSet().add(aggID);
		
		List<NWR> listNWR = opt.getNWRs(aggID);

		assertNotNull(listNWR);
		assertFalse(listNWR.isEmpty());

		assertEquals(2, listNWR.size());

		NWR n = listNWR.get(0);

	
		assertNotNull(n);
		assertEquals(opt.getNextNotificationTime(pubTime, aggID), n.getTimeStamp().getTime());

		assertEquals(n.getMessageDestination(), center.getBrokerDestination());
		//assertTrue(cw.contains(n));

		n = listNWR.get(1);

		assertNotNull(n);
		assertEquals(opt.getNextNotificationTime(pubTime, aggID) + aggID.getShiftSize() * 1000, n.getTimeStamp().getTime());

		assertEquals(n.getMessageDestination(), center.getBrokerDestination());
		//assertTrue(cw.contains(n));

		Thread.sleep(6000);

		opt.getCurrentPublicationProfile().setPublicationMessage(pm);
		listNWR = opt.getNWRs(aggID);

		assertNotNull(listNWR);
		assertEquals(2, listNWR.size());

		
		/************************** 
		 * getNWRs should never be called for aggregate publication anymore
		AggregatedPublication agg = new AggregatedPublication(MessageFactory.createPublicationFromString("[class,STOCK],[value,80]"), aggID);
		agg.setAgrResult("123");

		pm = new PublicationMessage(agg);
		pm.setLastHopID(A.getBrokerDestination());
		pm.setNextHopID(center.getBrokerDestination());

		opt.getCurrentPublicationProfile().setPublicationMessage(pm);
		listNWR = opt.getNWRs(aggID);

		assertNotNull(listNWR);
		assertEquals(0, listNWR.size());
		***************************/




		// If we generate NWR even for older AggPub, no more the case in the
		// current design, therefore tests are commented
		// assertEquals(1, listNWR.size());
		// assertEquals(agg.getTimeStamp(), listNWR.get(0).getTimeStamp());

		sub2();

		publish();
		pm = new PublicationMessage(p);
		pm.setLastHopID(A.getBrokerDestination());
		pm.setNextHopID(center.getBrokerDestination());

		
		opt.getCurrentPublicationProfile().setPublicationMessage(pm);
		listNWR = opt.getNWRs(aggID2);
		assertNotNull(listNWR);
		assertEquals(3, listNWR.size());

	}

	public void testProcessPublicationMessage() throws ClientException, ParseException, InterruptedException {

		advertiseSub();

		publish();

		
		
		PublicationMessage pm = new PublicationMessage(p);

		pm.setLastHopID(A.getBrokerDestination());
		// pm.setNextHopID(center.getBrokerDestination());
		opt.processPublicationMessage(pm, new HashSet<Message >());
		//opt.aggregatePublicationMessage(pm,new HashSet<MessageDestination>());

		assertFalse(opt.getCurrentPublicationProfile().getMatchedAggregationSet().isEmpty());
		assertEquals(opt.getCurrentPublicationProfile().getMatchedAggregationSet().iterator().next(), aggID);
		assertEquals(p.toString(), opt.getGraphs().get(center.getBrokerDestination()).getPartition2().iterator().next().getElement().toString());
		assertEquals(1, opt.getGraphs().get(center.getBrokerDestination()).getPartition2().size());
		assertEquals(2, opt.getGraphs().get(center.getBrokerDestination()).getPartition1().size());
		assertEquals(2, opt.getGraphs().get(center.getBrokerDestination()).edgeSet().size());
		assertEquals(3, opt.getGraphs().get(center.getBrokerDestination()).getEdgeMap().size());
		
		
		pm = new PublicationMessage(MessageFactory.createPublicationFromString("[class,STOCK],[value,80]"));
		pm.setLastHopID(A.getBrokerDestination());
		// pm.setNextHopID(center.getBrokerDestination());
		opt.processPublicationMessage(pm, new HashSet<Message >());
		//opt.aggregatePublicationMessage(pm,new HashSet<MessageDestination>());

		assertFalse(opt.getCurrentPublicationProfile().getMatchedAggregationSet().isEmpty());
		assertEquals(opt.getCurrentPublicationProfile().getMatchedAggregationSet().iterator().next(), aggID);
		assertEquals(2, opt.getGraphs().get(center.getBrokerDestination()).getPartition2().size());
		assertEquals(2, opt.getGraphs().get(center.getBrokerDestination()).getPartition1().size());

		assertEquals(4, opt.getGraphs().get(center.getBrokerDestination()).edgeSet().size());
		assertEquals(4, opt.getGraphs().get(center.getBrokerDestination()).getEdgeMap().size());
		
		AggregatedPublication agg = new AggregatedPublication(MessageFactory.createPublicationFromString("[class,STOCK],[value,80]"), aggID);
		agg.setAgrResult("123");
		agg.setTimeStamp(new Date(opt.getNextNotificationTime(System.currentTimeMillis(), aggID)));

		pm = new PublicationMessage(agg);
		pm.setLastHopID(A.getBrokerDestination());
		pm.setNextHopID(center.getBrokerDestination());
		opt.processPublicationMessage( pm,new HashSet<Message>());

		assertFalse(opt.getCurrentPublicationProfile().getMatchedAggregationSet().isEmpty());
		assertEquals(opt.getCurrentPublicationProfile().getMatchedAggregationSet().iterator().next(), aggID);
		assertEquals(3, opt.getGraphs().get(center.getBrokerDestination()).getPartition2().size());
		assertEquals(2, opt.getGraphs().get(center.getBrokerDestination()).getPartition1().size());
		assertEquals(5, opt.getGraphs().get(center.getBrokerDestination()).edgeSet().size());
		assertEquals(5, opt.getGraphs().get(center.getBrokerDestination()).getEdgeMap().size());

		pm = new PublicationMessage(MessageFactory.createPublicationFromString("[class,STOCK],[value,80]"));
		pm.setLastHopID(A.getBrokerDestination());
		pm.getPublication().setTimeStamp(new Date(100000));
		// pm.setNextHopID(center.getBrokerDestination());
		opt.processPublicationMessage(pm,new HashSet<Message>());

		assertFalse(opt.getCurrentPublicationProfile().getMatchedAggregationSet().isEmpty());
		assertEquals(opt.getCurrentPublicationProfile().getMatchedAggregationSet().iterator().next(), aggID);
		assertEquals(3, opt.getGraphs().get(center.getBrokerDestination()).getPartition2().size());
		assertEquals(2, opt.getGraphs().get(center.getBrokerDestination()).getPartition1().size());
		assertEquals(5, opt.getGraphs().get(center.getBrokerDestination()).edgeSet().size());
		assertEquals(5, opt.getGraphs().get(center.getBrokerDestination()).getEdgeMap().size());

		Thread.sleep(10000);
		for(TimeConstraintBipartiteGraph<OptimalAggregationGraphNode, Publication> g : opt.getGraphs().values()){
			System.out.println(g);
			assertTrue(g.isEmpty());
		}
	}

	public void testProcessPublicationMessage_2SUB()  {

		try{
		advertiseSub();
		sub2();
		assertNotNull(aggID2);
		assertNotSame(aggID2, aggID);

		HashSet<AggregationID> aggIDs = new HashSet<AggregationID>();
		aggIDs.add(aggID);
		aggIDs.add(aggID2);

		publish();

		PublicationMessage pm = new PublicationMessage(p);

		pm.setLastHopID(A.getBrokerDestination());
		// pm.setNextHopID(center.getBrokerDestination());

		opt.processPublicationMessage(pm,new HashSet<Message>());

		assertFalse(opt.getCurrentPublicationProfile().getMatchedAggregationSet().isEmpty());

		assertTrue(aggIDs.contains(opt.getCurrentPublicationProfile().getMatchedAggregationSet().iterator().next()));

		Iterator<AggregationID> it = opt.getCurrentPublicationProfile().getMatchedAggregationSet().iterator();
		it.next();
		assertTrue(aggIDs.contains(it.next()));

		assertEquals(p.toString(), opt.getGraphs().get(center.getBrokerDestination()).getPartition2().iterator().next().getElement().toString());
		assertEquals(5, opt.getGraphs().get(center.getBrokerDestination()).getPartition1().size());
		assertEquals(5, opt.getGraphs().get(center.getBrokerDestination()).edgeSet().size());
		
		pm = new PublicationMessage(MessageFactory.createPublicationFromString("[class,STOCK],[value,80]"));
		pm.setLastHopID(A.getBrokerDestination());
		// pm.setNextHopID(center.getBrokerDestination());
		opt.processPublicationMessage(pm,new HashSet<Message>());

		assertFalse(opt.getCurrentPublicationProfile().getMatchedAggregationSet().isEmpty());
		assertTrue(aggIDs.contains(opt.getCurrentPublicationProfile().getMatchedAggregationSet().iterator().next()));

		it = opt.getCurrentPublicationProfile().getMatchedAggregationSet().iterator();
		it.next();
		assertTrue(aggIDs.contains(it.next()));

		assertEquals(2, opt.getGraphs().get(center.getBrokerDestination()).getPartition2().size());
		assertEquals(5, opt.getGraphs().get(center.getBrokerDestination()).getPartition1().size());
		assertEquals(10, opt.getGraphs().get(center.getBrokerDestination()).edgeSet().size());
		
		AggregatedPublication agg = new AggregatedPublication(MessageFactory.createPublicationFromString("[class,STOCK],[value,80]"), aggID);
		agg.setAgrResult("123");
		agg.setTimeStamp(new Date(opt.getNextNotificationTime(System.currentTimeMillis(), aggID)));

		pm = new PublicationMessage(agg);
		pm.setLastHopID(A.getBrokerDestination());
		pm.setNextHopID(center.getBrokerDestination());
		opt.processPublicationMessage(pm,new HashSet<Message>());

		assertFalse(opt.getCurrentPublicationProfile().getMatchedAggregationSet().isEmpty());
		assertTrue(aggIDs.contains(opt.getCurrentPublicationProfile().getMatchedAggregationSet().iterator().next()));

		it = opt.getCurrentPublicationProfile().getMatchedAggregationSet().iterator();
		//it.next();
		assertTrue(aggIDs.contains(it.next()));
		assertEquals(3, opt.getGraphs().get(center.getBrokerDestination()).getPartition2().size());
		assertEquals(5, opt.getGraphs().get(center.getBrokerDestination()).getPartition1().size());
		assertEquals(11, opt.getGraphs().get(center.getBrokerDestination()).edgeSet().size());

		pm = new PublicationMessage(MessageFactory.createPublicationFromString("[class,STOCK],[value,80]"));
		pm.setLastHopID(A.getBrokerDestination());
		pm.getPublication().setTimeStamp(new Date(System.currentTimeMillis() - 6500));
		// pm.setNextHopID(center.getBrokerDestination());
		/* Thread.sleep(5000); */
		opt.processPublicationMessage(pm,new HashSet<Message>());

		assertFalse(opt.getCurrentPublicationProfile().getMatchedAggregationSet().isEmpty());
		assertTrue(aggIDs.contains(opt.getCurrentPublicationProfile().getMatchedAggregationSet().iterator().next()));

		it = opt.getCurrentPublicationProfile().getMatchedAggregationSet().iterator();
		it.next();
		assertTrue(aggIDs.contains(it.next()));

		assertEquals(3, opt.getGraphs().get(center.getBrokerDestination()).getPartition2().size());
		assertEquals(5, opt.getGraphs().get(center.getBrokerDestination()).getPartition1().size());
		}
		catch(NullPointerException e){
			e.printStackTrace();
			assertTrue(false);
		}catch(Exception e){
			e.printStackTrace();
			assertTrue(false);
		}
	}

	public void testInit() throws ClientException, ParseException, InterruptedException {

		assertTrue(B.getRouter().getPostProcessor().getAggregationEngine() instanceof OptimalAggregationEngine);

		publisherA.advertise(MessageFactory.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"), BROKER_A);

		publisherC.advertise(MessageFactory.createAdvertisementFromString("[class,eq,STOCK],[value,>,10]"), BROKER_C);
		Thread.sleep(1000L);
		subscriberB.subscribe("[class,eq,STOCK],[value,>,15],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'4'],[NTF,eq,'2']", BROKER_B);
		long t = System.currentTimeMillis();
		Thread.sleep(2000 - t % 2000L);
		for (int i = 0; i < 10; i++) {
			publisherA.publish(MessageFactory.createPublicationFromString("[class,STOCK],[value,80]"), BROKER_A);
			publisherC.publish(MessageFactory.createPublicationFromString("[class,STOCK],[value,800]"), BROKER_C);
			Thread.sleep(30);
		}

		Thread.sleep(10000);

		System.out.println("\n\n\n");

		int i = 0;

		for (Message m : subscriberB.getAllReceivedMessage()) {
			System.out.println(i++);
			assertTrue(m instanceof PublicationMessage);
			assertTrue(((PublicationMessage) m).getPublication() instanceof AggregatedPublication);
			System.out.println(((AggregatedPublication) ((PublicationMessage) m).getPublication()).getAggResult());
		}
		System.out.println(i + " DONE");

		// TODO check that 2 is the correct answer
		assertEquals(2, subscriberB.getAllReceivedMessage().size());

	}

	public void testNormal() throws ClientException, ParseException, InterruptedException {
		assertTrue(B.getRouter().getPostProcessor().getAggregationEngine() instanceof OptimalAggregationEngine);

		publisherA.advertise(MessageFactory.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"), BROKER_A);

		publisherC.advertise(MessageFactory.createAdvertisementFromString("[class,eq,STOCK],[value,>,10]"), BROKER_C);
		Thread.sleep(1000L);
		subscriberB.subscribe("[class,eq,STOCK],[value,>,15],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'3'],[NTF,eq,'1']", BROKER_B);
		subscriberB.getAllReceivedMessage().clear();
		
		long t = System.currentTimeMillis();
		Thread.sleep(3010 - t % 3000L);
		((OptimalAggregationEngine) B.getRouter().getPostProcessor().getAggregationEngine()).setDelay(10);
		for (int i = 0; i < 5; i++) {
			publisherA.publish(MessageFactory.createPublicationFromString("[class,STOCK],[value,80]"), BROKER_A);
			//Thread.sleep(2000);
			publisherC.publish(MessageFactory.createPublicationFromString("[class,STOCK],[value,800]"), BROKER_C);
			Thread.sleep(3010 - System.currentTimeMillis() % 3000L);
			//Thread.sleep(3001);
		}

		Thread.sleep(10000);

		System.out.println("\n\n\n");

		int i = 0;

		//assertEquals(10, subscriberB.getReceivedMessage().size());
		
		for (Message m : subscriberB.getAllReceivedMessage()) {
			System.out.println(i++);
			assertTrue(m instanceof PublicationMessage);
			if(i!=9){
			//	assertFalse(((PublicationMessage) m).getPublication() instanceof AggregatedPublication);
			}
			else{
			//	assertTrue(((PublicationMessage) m).getPublication() instanceof AggregatedPublication);
			}
		}
		System.out.println(i + " DONE");

		assertEquals(10, subscriberB.getAllReceivedMessage().size());

	}

	
	public void testCollectionUnit() throws ClientException, ParseException, InterruptedException {
		AggregatedPublication agg1, agg2;
		
		Publication pub1,pub2;
		
		advertiseSub();
		
		
		pub1=MessageFactory.createPublicationFromString("[class,STOCK],[value,80]");
		pub2=MessageFactory.createPublicationFromString("[class,STOCK],[value,800]");
		
		agg1=new AggregatedPublication(pub1,aggID);
		agg1.setAgrResult("10");
		agg1.setTimeStamp(new Date(10));
		agg2=new AggregatedPublication(pub2,aggID);
		agg2.setAgrResult("20");
		agg2.setTimeStamp(new Date(10));
		
		PublicationMessage pm=new PublicationMessage(agg1);
		pm.setLastHopID(A.getBrokerDestination());
		opt.processPublicationMessage(pm,new HashSet<Message>());
		
		pm=new PublicationMessage(agg2);
		pm.setLastHopID(A.getBrokerDestination());
		opt.processPublicationMessage(pm,new HashSet<Message>());
		
		//Thread.sleep(100);
		assertEquals(2, opt.getGraphs().get(center.getBrokerDestination()).getPartition2().size());
		assertEquals(1, opt.getGraphs().get(center.getBrokerDestination()).getPartition1().size());
		assertEquals(2, opt.getGraphs().get(center.getBrokerDestination()).edgeSet().size());
		assertEquals(3, opt.getGraphs().get(center.getBrokerDestination()).getEdgeMap().size());
		
	}
	
	
	public void testCollection() throws ClientException, ParseException, InterruptedException {
		
		assertTrue(B.getRouter().getPostProcessor().getAggregationEngine() instanceof OptimalAggregationEngine);

		publisherA.advertise(MessageFactory.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"), BROKER_A);

		publisherC.advertise(MessageFactory.createAdvertisementFromString("[class,eq,STOCK],[value,>,10]"), BROKER_C);
		Thread.sleep(1000L);
		subscriberB.subscribe("[class,eq,STOCK],[value,>,15],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']", BROKER_B);
		long t = System.currentTimeMillis();
		Thread.sleep(2000 - t % 2000L);
		((OptimalAggregationEngine) B.getRouter().getPostProcessor().getAggregationEngine()).setDelay(10);
		//((OptimalAggregationEngine) B.getRouter().getPostProcessor().getAggregationEngine()).COLLECTING_TIMEOUT=1000;
		for (int i = 0; i < 5; i++) {
			publisherA.publish(MessageFactory.createPublicationFromString("[class,STOCK],[value,80]"), BROKER_A);
			publisherC.publish(MessageFactory.createPublicationFromString("[class,STOCK],[value,800]"), BROKER_C);
			Thread.sleep(10);
		}

		Thread.sleep(10000);

		System.out.println("\n\n\n");

		int i = 0;

		
		
		assertEquals(1, subscriberB.getAllReceivedMessage().size());
		for (Message m : subscriberB.getAllReceivedMessage()) {
			System.out.println(i++);
			assertTrue(m instanceof PublicationMessage);
			assertTrue(((PublicationMessage) m).getPublication() instanceof AggregatedPublication);
			assertEquals("4400", ((AggregatedPublication)((PublicationMessage) m).getPublication()).getAggResult());
		}
		System.out.println(i + " DONE");

		
	}
	
	
	public void testPerformance() throws ClientException, ParseException, InterruptedException {
		assertTrue(B.getRouter().getPostProcessor().getAggregationEngine() instanceof OptimalAggregationEngine);
		
		int nsub = 21;

		Thread.sleep(2000);
		
		publisherA.advertise(MessageFactory.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"), BROKER_A);

		publisherC.advertise(MessageFactory.createAdvertisementFromString("[class,eq,STOCK],[value,>,10]"), BROKER_C);
		Thread.sleep(5000L);
		for (int i = 0; i < nsub; i++) {
			//subscriberB = new TestClient("Subscriber B" + i);
			//subscriberB.connect(BROKER_B);
			subscriberB.subscribe("[class,eq,STOCK],[value,>," + i + "],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']", BROKER_B);
			/*
			 * subscriberB .subscribe( "[class,eq,STOCK],[value,>,"+i+"]",
			 * BROKER_B);
			 */
		}

		
		/*if (System.getProperty("padres.aggregation.implementation").equals("OPTIMAL_AGGREGATION")) {
			// artificially speeding the broker
			A.getAggregatorTimer().updateWindowTimeGCD(1, 1);
			B.getAggregatorTimer().updateWindowTimeGCD(1, 1);
			C.getAggregatorTimer().updateWindowTimeGCD(1, 1);
			center.getAggregatorTimer().updateWindowTimeGCD(1, 1);
			// ---
		}*/
		
		
		Thread.sleep(2000L);
		// Thread.sleep(6000-System.currentTimeMillis()%6000L);
		long t=System.currentTimeMillis();
		for (int i = 0; i < 120; i++) {
			publisherA.publish(MessageFactory.createPublicationFromString("[class,STOCK],[value,100]"), BROKER_A);
			publisherC.publish(MessageFactory.createPublicationFromString("[class,STOCK],[value,100000]"), BROKER_C);
			Thread.sleep(10);
		}
		System.out.println("It took "+(System.currentTimeMillis()-t)+" ms to generate all pub");
		long n;
		Thread.sleep(2500L);

		n = subscriberB.getAllReceivedMessage().size();

		// assertEquals(2000, subscriberB.getReceivedMessage().size());
		Thread.sleep(30000L);
		long value = 0;
		int agg = 0;
		int raw =0;
		for (Message m : subscriberB.getAllReceivedMessage()) {
			if (((PublicationMessage) m).getPublication() instanceof AggregatedPublication) {
				value += Long.parseLong(((AggregatedPublication) ((PublicationMessage) m).getPublication()).getAggResult());
				agg++;
			}
			else raw++;
		}

		System.out.println("Value= " + value + "\nPercentage= " + 100 * value / (nsub * 12012000.0) + "%");
		System.out.println(n + " messages sent \n" + subscriberB.getAllReceivedMessage().size() + "\nagg: " + agg+"\nraw: "+raw);
		assertEquals(n, subscriberB.getAllReceivedMessage().size());
		
	}

}
