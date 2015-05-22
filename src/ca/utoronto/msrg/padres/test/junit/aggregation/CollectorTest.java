package ca.utoronto.msrg.padres.test.junit.aggregation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;
import ca.utoronto.msrg.padres.broker.aggregation.AbstractAggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.AdaptiveAggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.AggregationRoutingTable;
import ca.utoronto.msrg.padres.broker.aggregation.CollectionBasedAggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationMode;
import ca.utoronto.msrg.padres.broker.aggregation.collector.CollectorAtBroker;
import ca.utoronto.msrg.padres.broker.aggregation.collector.OutgoingWindow;
import ca.utoronto.msrg.padres.broker.aggregation.message.AggregatedPublication;
import ca.utoronto.msrg.padres.broker.aggregation.operator.OperatorType;
import ca.utoronto.msrg.padres.broker.aggregation.utility.RoutingTable;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCoreException;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.Predicate;
import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.Subscription;
import ca.utoronto.msrg.padres.common.message.SubscriptionMessage;
import ca.utoronto.msrg.padres.common.message.parser.MessageFactory;
import ca.utoronto.msrg.padres.common.message.parser.ParseException;
import ca.utoronto.msrg.padres.test.junit.aggregation.utility.DummyAggregationEngine;
import ca.utoronto.msrg.padres.test.junit.aggregation.utility.DummyNotifier;
import ca.utoronto.msrg.padres.test.junit.aggregation.utility.TestClient;

public class CollectorTest extends TestCase {

	CollectorAtBroker c;
	ArrayList<PublicationMessage> messageSent;
	AggregationID aggID;
	
	long time;
	
	MessageDestination broker1Dest = new MessageDestination("Broker1");
	MessageDestination broker2Dest = new MessageDestination("Broker2");
	MessageDestination broker3Dest = new MessageDestination("Broker3");
	MessageDestination broker4Dest = new MessageDestination("Broker4");
	
	
	AbstractAggregationEngine aggEngine;
	AggregationRoutingTable rt;
	@Override
	protected void setUp() throws Exception {
		
		System.setProperty("padres.aggregation.implementation","EARLY_AGGREGATION");
		//System.setProperty("padres.aggregation.implementation","OPTIMAL_AGGREGATION");
		System.setProperty("aggregation.client", "OFF");
		if(System.getProperty("padres.aggregation.implementation")==null)assertTrue(false);
		
		BrokerCore bc1=new BrokerCore("-uri rmi://localhost:1098/Broker1");// + " -n " + BROKER_CENTER);
		bc1.initialize();
		
		aggEngine=(AbstractAggregationEngine) bc1.getRouter().getPostProcessor().getAggregationEngine();
		
		/*aggEngine=new DummyAggregationEngine(null, null);*/
		rt =new AggregationRoutingTable(aggEngine);
		
		Map<String, Predicate> predicates = new HashMap<String, Predicate>();
		aggID = new AggregationID(predicates, 10, 100, "A",
				OperatorType.SUM,"0");
		messageSent = new ArrayList<PublicationMessage>();
		
		/*c = new CollectorAtBroker(new DummyNotifier(bc,messageSent), aggID, 0, aggEngine);
		((CollectionBasedAggregationEngine)aggEngine).getCollectorSet().add(c);*/
		SubscriptionMessage sub = new SubscriptionMessage(new Subscription(),
				"au");
		
		
		
		sub.setLastHopID(broker4Dest);
		sub.setNextHopID(broker3Dest);
		sub.getSubscription().setAggregationID(aggID);
		sub.getSubscription().setAggregation(true);
		
		bc1.getInputQueue().addMessage(sub);
		
		//rt.registerSubscription(sub);
		
		sub = new SubscriptionMessage(new Subscription(), "aeou");
		sub.setLastHopID(broker4Dest);
		sub.setNextHopID(broker1Dest);
		sub.getSubscription().setAggregationID(aggID);
		sub.getSubscription().setAggregation(true);
		rt.registerSubscription(sub);
		sub = new SubscriptionMessage(new Subscription(), "aou");
		sub.setLastHopID(broker4Dest);
		sub.setNextHopID(broker2Dest);
		sub.getSubscription().setAggregationID(aggID);
		sub.getSubscription().setAggregation(true);
		time = System.currentTimeMillis();
		Thread.sleep(1000);
		rt.registerSubscription(sub);
		super.setUp();
	}

	/*public void testNormalMsgObserved() {

		c.normalMsgObserved("A", "Window1");
		c.normalMsgObserved("B", "Window1");
		c.normalMsgObserved("C", "Window1");

	}*/

	public void testAddMsg() throws ParseException {
		AggregatedPublication p;
		PublicationMessage pm;
		
		p = new AggregatedPublication(MessageFactory
				.createPublicationFromString("[class,'Notification']"),aggID);
		p.setWindowID(""+time);
		p.setAggregation(true,  OperatorType.SUM, "", "5");
		pm = new PublicationMessage(p);
		pm.setLastHopID(broker3Dest);
		pm.setNextHopID(broker4Dest);
		c.addMsg(pm);
		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assertEquals(1, c.size());
		// assertEquals(1, c.getExpiredMessage().size());

		p = new AggregatedPublication(MessageFactory
				.createPublicationFromString("[class,'Notification']"),aggID);
			
		p.setWindowID(""+time);
		p.setAggregation(true, OperatorType.SUM, "", "7");

		pm = new PublicationMessage(p);
		pm.setLastHopID(broker3Dest);
		pm.setNextHopID(broker4Dest);

		PublicationMessage pm2 = new PublicationMessage(p);
		pm2.setLastHopID(broker1Dest);
		pm2.setNextHopID(broker4Dest);
		c.addMsg(pm2);
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assertEquals(1, c.size());
		assertEquals("12", c.collectResults(new OutgoingWindow(""+time, broker4Dest)));
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assertEquals(1, c.getExpiredMessage().size());

		PublicationMessage pm3 = new PublicationMessage(p);

		p.setWindowID(""+time);

		p.setAggregation(true, OperatorType.SUM, "", "8");
		pm3 = new PublicationMessage(p);
		pm3.setLastHopID(broker3Dest);
		pm3.setNextHopID(broker4Dest);

		p.setWindowID(""+time);

		p.setAggregation(true,  OperatorType.SUM, "", "1");
		pm2 = new PublicationMessage(p);
		pm2.setLastHopID(broker1Dest);
		pm2.setNextHopID(broker4Dest);

		p.setWindowID(""+time);

		p.setAggregation(true,  OperatorType.SUM, "", "2");
		pm = new PublicationMessage(p);
		pm.setLastHopID(broker2Dest);
		pm.setNextHopID(broker4Dest);
		c.addMsg(pm3);
		assertEquals(0, messageSent.size());
		c.addMsg(pm2);
		assertEquals(0, messageSent.size());
		c.addMsg(pm);

		assertEquals(1, messageSent.size());

	}

	public void testSubscribe() {
		
		SubscriptionMessage sub = new SubscriptionMessage(new Subscription(),
				"au");
		
		sub.setLastHopID(new MessageDestination("BrokerDest"));
		sub.setNextHopID(new MessageDestination("BrokerIn0"));
		rt.registerSubscription(sub);
		sub = new SubscriptionMessage(new Subscription(), "aeou");
		sub.setLastHopID(new MessageDestination("BrokerDest"));
		sub.setNextHopID(new MessageDestination("BrokerIn1"));
		rt.registerSubscription(sub);
		sub = new SubscriptionMessage(new Subscription(), "aou");
		sub.setLastHopID(new MessageDestination("BrokerDest"));
		sub.setNextHopID(new MessageDestination("BrokerIn2"));

		assertTrue(rt.getDestinations(aggID,new MessageDestination("BrokerIn0"))
				.contains(new MessageDestination("BrokerDest")));

		sub = new SubscriptionMessage(new Subscription(), "au");
		sub.setLastHopID(new MessageDestination("BrokerIn0"));
		sub.setNextHopID(new MessageDestination("BrokerDest"));
		rt.registerSubscription(sub);
		sub = new SubscriptionMessage(new Subscription(), "aeou");
		sub.setLastHopID(new MessageDestination("BrokerIn0"));
		sub.setNextHopID(new MessageDestination("BrokerIn1"));
		rt.registerSubscription(sub);
		sub = new SubscriptionMessage(new Subscription(), "aou");
		sub.setLastHopID(new MessageDestination("BrokerIn0"));
		sub.setNextHopID(new MessageDestination("BrokerIN2"));
		rt.registerSubscription(sub);

		assertTrue(rt.getDestinations(aggID,new MessageDestination("BrokerIn0"))
				.contains(new MessageDestination("BrokerDest")));
		assertTrue(rt.getDestinations(aggID,new MessageDestination("BrokerIn1"))
				.contains(new MessageDestination("BrokerDest")));
	}

	public void testStarcase() {

		// Broker3 subscribes to everyone
		SubscriptionMessage sub = new SubscriptionMessage(new Subscription(),
				"au");
		sub.setLastHopID(broker3Dest);
		sub.setNextHopID(broker4Dest);
		rt.registerSubscription(sub);
		sub = new SubscriptionMessage(new Subscription(), "aeou");
		sub.setLastHopID(broker3Dest);
		sub.setNextHopID(broker1Dest);
		rt.registerSubscription(sub);
		sub = new SubscriptionMessage(new Subscription(), "aou");
		sub.setLastHopID(broker3Dest);
		sub.setNextHopID(broker2Dest);
		rt.registerSubscription(sub);
		
		// Broker1 subscribes to everyone
		sub = new SubscriptionMessage(new Subscription(), "au");
		sub.setLastHopID(broker1Dest);
		sub.setNextHopID(broker4Dest);
		rt.registerSubscription(sub);
		sub = new SubscriptionMessage(new Subscription(), "aeou");
		sub.setLastHopID(broker1Dest);
		sub.setNextHopID(broker3Dest);
		rt.registerSubscription(sub);
		sub = new SubscriptionMessage(new Subscription(), "aou");
		sub.setLastHopID(broker1Dest);
		sub.setNextHopID(broker2Dest);
		rt.registerSubscription(sub);

		// Broker2 subscribes to everyone
		sub = new SubscriptionMessage(new Subscription(), "au");
		sub.setLastHopID(broker2Dest);
		sub.setNextHopID(broker4Dest);
		rt.registerSubscription(sub);
		sub = new SubscriptionMessage(new Subscription(), "aeou");
		sub.setLastHopID(broker2Dest);
		sub.setNextHopID(broker1Dest);
		rt.registerSubscription(sub);
		sub = new SubscriptionMessage(new Subscription(), "aou");
		sub.setLastHopID(broker2Dest);
		sub.setNextHopID(broker3Dest);
		rt.registerSubscription(sub);
		
		
		
		AggregatedPublication p = null;
		PublicationMessage pm;

		try {
			p = new AggregatedPublication(MessageFactory
				.createPublicationFromString("[class,'Notification']"),aggID);
	
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		assertEquals(0, messageSent.size());
		
		// Broker3 sends value 2
		p.setWindowID(""+time);
		p.setAggregation(true,  OperatorType.SUM, "", "2");
		pm = new PublicationMessage(p);
		// to  ALL //Broker4
		pm.setLastHopID(broker3Dest);
		pm.setNextHopID(broker4Dest);
		c.addMsg(pm);
		/*// to Broker2
		pm = new PublicationMessage(p);
		pm.setLastHopID(broker3Dest);
		pm.setNextHopID(broker2Dest);
		c.addMsg(pm);
		// to Broker1
		pm = new PublicationMessage(p);
		pm.setLastHopID(broker3Dest);
		pm.setNextHopID(broker1Dest);
		c.addMsg(pm);*/
		
		assertEquals(0, messageSent.size());
		
		// Broker4 sends value 3
		p.setWindowID(""+time);
		p.setAggregation(true,  OperatorType.SUM, "", "3");
		pm = new PublicationMessage(p);
		// to Broker3
		pm.setLastHopID(broker4Dest);
		pm.setNextHopID(broker3Dest);
		c.addMsg(pm);
		/*// to Broker2
		pm.setLastHopID(broker4Dest);
		pm.setNextHopID(broker2Dest);
		c.addMsg(pm);
		// to Broker1
		pm.setLastHopID(broker4Dest);
		pm.setNextHopID(broker1Dest);
		c.addMsg(pm);*/
		assertEquals(0, messageSent.size());
		
		
		// Broker2 sends value 5
		p.setWindowID(""+time);
		p.setAggregation(true,  OperatorType.SUM, "", "5");
		pm = new PublicationMessage(p);
		// to Broker3
		pm.setLastHopID(broker2Dest);
		pm.setNextHopID(broker3Dest);
		c.addMsg(pm);
		/*// to Broker4
		pm.setLastHopID(broker2Dest);
		pm.setNextHopID(broker4Dest);
		c.addMsg(pm);
		// to Broker1
		pm.setLastHopID(broker2Dest);
		pm.setNextHopID(broker1Dest);
		c.addMsg(pm);*/
		assertEquals(1, messageSent.size());
		
		
		// Broker1 sends value 7
		p.setWindowID(""+time);
		p.setAggregation(true, OperatorType.SUM, "", "7");
		pm = new PublicationMessage(p);
		// to Broker3
		pm.setLastHopID(broker1Dest);
		pm.setNextHopID(broker3Dest);
		c.addMsg(pm);
		/*// to Broker2
		pm.setLastHopID(broker1Dest);
		pm.setNextHopID(broker2Dest);
		c.addMsg(pm);
		// to Broker4
		pm.setLastHopID(broker1Dest);
		pm.setNextHopID(broker4Dest);
		c.addMsg(pm);*/

		assertEquals(4, messageSent.size());
		assertEquals("Broker1", messageSent.get(0).getNextHopID().getBrokerId());
		assertEquals("Broker2", messageSent.get(1).getNextHopID().getBrokerId());
		assertEquals("Broker3", messageSent.get(2).getNextHopID().getBrokerId());
		assertEquals("Broker4", messageSent.get(3).getNextHopID().getBrokerId());
		
		assertEquals("10", ((AggregatedPublication)messageSent.get(0).getPublication()).getAggResult());
		assertEquals("12", ((AggregatedPublication)messageSent.get(1).getPublication()).getAggResult());
		assertEquals("15", ((AggregatedPublication)messageSent.get(2).getPublication()).getAggResult());
		assertEquals("14", ((AggregatedPublication)messageSent.get(3).getPublication()).getAggResult());
	}
	
	public void testExpired() throws ClientException, BrokerCoreException, ParseException, InterruptedException{
		
		String broker1Address = "rmi://localhost:1521/Broker1";
		String broker2Address = "rmi://localhost:1502/Broker2";
		
		BrokerCore broker1 = new BrokerCore("-uri "+broker1Address);
		broker1.initialize();
		
		BrokerCore broker2 = new BrokerCore("-uri "+broker2Address+ " -n "+broker1Address);
		broker2.initialize();
		
		TestClient publisher1=new TestClient("Publisher 1");
		publisher1.connect(broker1Address);
		TestClient publisher2=new TestClient("Publisher 2");
		publisher2.connect(broker2Address);
		TestClient subscriber=new TestClient("Subscriber");
		subscriber.connect(broker2Address);
		
		publisher1.advertise(MessageFactory.createAdvertisementFromString(
				"[class,eq,STOCK],[value,>,50]"), broker1Address);
		publisher2.advertise(MessageFactory.createAdvertisementFromString(
				"[class,eq,STOCK],[value,>,50]"), broker2Address);
		
		Thread.sleep(1000);
		
		
		subscriber.subscribe(
				"[class,eq,STOCK],[value,>,15],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'5'],[NTF,eq,'5']", broker2Address);
		
		
		Thread.sleep(1000);
		
		for(int i=0;i<1;i++){
			publisher1.publish(MessageFactory.createPublicationFromString(
					"[class,STOCK],[value,80]"), broker1Address);
		}
		assertTrue(subscriber.getAllReceivedMessage().isEmpty());
		
		Thread.sleep(13000);
	
		assertFalse(subscriber.getAllReceivedMessage().isEmpty());
		
	}
	
	public void testAdaptation() throws ClientException, BrokerCoreException, ParseException, InterruptedException{
		
		String broker1Address = "rmi://localhost:1521/Broker1";
		String broker2Address = "rmi://localhost:1501/Broker2";
		
		BrokerCore broker1 = new BrokerCore("-uri "+broker1Address);
		broker1.initialize();
		
		BrokerCore broker2 = new BrokerCore("-uri "+broker2Address+ " -n "+broker1Address);
		broker2.initialize();
		if( broker2.getRouter().getPostProcessor().getAggregationEngine() instanceof AdaptiveAggregationEngine)
			((AdaptiveAggregationEngine) broker2.getRouter().getPostProcessor().getAggregationEngine()).getAdaptationEngine().setForcedMode(AggregationMode.AGG_MODE);
		
		TestClient publisher1=new TestClient("Publisher 1");
		publisher1.connect(broker1Address);
		TestClient publisher2=new TestClient("Publisher 2");
		publisher2.connect(broker2Address);
		TestClient subscriber=new TestClient("Subscriber");
		subscriber.connect(broker2Address);
		
		publisher1.advertise(MessageFactory.createAdvertisementFromString(
				"[class,eq,STOCK],[value,>,50]"), broker1Address);
		publisher2.advertise(MessageFactory.createAdvertisementFromString(
				"[class,eq,STOCK],[value,>,50]"), broker2Address);
		
		Thread.sleep(1000);
		
		
		subscriber.subscribe(
				"[class,eq,STOCK],[value,>,15],[AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']", broker2Address);
		
		
		Thread.sleep(1000);
		
		for(int i=0;i<1;i++){
			publisher1.publish(MessageFactory.createPublicationFromString(
					"[class,STOCK],[value,80]"), broker1Address);
		}
		assertTrue(subscriber.getAllReceivedMessage().isEmpty());
		
		publisher2.publish(MessageFactory.createPublicationFromString(
					"[class,STOCK],[value,80]"), broker2Address);
	
		
		Thread.sleep(5000);
		assertFalse(subscriber.getAllReceivedMessage().isEmpty());
		
		
		
		
	}
	
	

}
