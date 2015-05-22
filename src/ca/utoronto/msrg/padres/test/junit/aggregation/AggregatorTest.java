package ca.utoronto.msrg.padres.test.junit.aggregation;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;
import ca.utoronto.msrg.padres.broker.aggregation.AdaptiveAggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationMode;
import ca.utoronto.msrg.padres.broker.aggregation.message.AggregatedPublication;
import ca.utoronto.msrg.padres.broker.aggregation.operator.OperatorType;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.common.message.Advertisement;
import ca.utoronto.msrg.padres.common.message.AdvertisementMessage;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.Predicate;
import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.Subscription;
import ca.utoronto.msrg.padres.common.message.SubscriptionMessage;
import ca.utoronto.msrg.padres.common.message.parser.MessageFactory;
import ca.utoronto.msrg.padres.common.message.parser.ParseException;
import ca.utoronto.msrg.padres.test.junit.aggregation.utility.DummyNotifier;

public class AggregatorTest extends TestCase {

	ArrayList<PublicationMessage> sentMessages = new ArrayList<PublicationMessage>();
	BrokerCore bc;
	AggregationID aggID;

	@Override
	protected void setUp() throws Exception {
		//System.setProperty("padres.aggregation.implementation","EARLY_AGGREGATION");
		System.setProperty("padres.aggregation.implementation","OPTIMAL_AGGREGATION");
		System.setProperty("aggregation.client", "OFF");
		if(System.getProperty("padres.aggregation.implementation")==null)assertTrue(false);
		
		bc = new BrokerCore("-uri rmi://localhost:1099/broker");
		bc.initialize();
		bc.getRouter().getPostProcessor().getAggregationEngine().setNotifier(new DummyNotifier(bc, sentMessages));
		Map<String, Predicate> predicates = new HashMap<String, Predicate>();

		aggID = new AggregationID(predicates, 1, 1, "value", OperatorType.SUM, "0");
		Advertisement adver=null;
		Subscription sub=null;
		try {
			adver = MessageFactory.createAdvertisementFromString("[class,eq,Notification],[value,>,1]");
			sub = MessageFactory.createSubscriptionFromString("[class,eq,Notification],[value,>,1][AGR,eq,'sum'],[PAR,eq,value],[PRD,eq,'1'],[NTF,eq,'1']");
		} catch (ParseException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

		sub.setAggregation(true);
		sub.setAggregationID(aggID);
		AdvertisementMessage am = new AdvertisementMessage(adver, bc.getNewMessageID());
		am.setLastHopID(new MessageDestination(bc.getBrokerDestination() + "-sub"));

		bc.getInputQueue().addMessage(am);
		Thread.sleep(1000);
		SubscriptionMessage sm = new SubscriptionMessage(sub, bc.getNewMessageID());
		sm.setLastHopID(new MessageDestination(bc.getBrokerDestination() + "-sub"));
		sm.setNextHopID(bc.getBrokerDestination());
		bc.getInputQueue().addMessage(sm);

		System.out.println("SETUP DONE");
		
		// aggregator.registerSubscription(new
		// SubscriptionMessage(sub,"messageID"));
		// bc.getRouter().getPostProcessor().getAggregationEngine().processMessage(new
		// SubscriptionMessage(sub,"sub"), new HashSet<Message>());

		super.setUp();
	}

	public void testSimpleAggregation() {
		Publication publication = null;
		PublicationMessage pm;

		try {
			Thread.sleep(1000 - (System.currentTimeMillis() % (1000)));
		} catch (InterruptedException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

		for (int i = 0; i < 100; i++) {

			try {
				publication = MessageFactory.createPublicationFromString("[class,'Notification'],[value," + (i) + "]");
			} catch (ParseException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			publication.setTimeStamp(new Date());
			// publication.setAggregation(true, true, OperatorType.SUM, "",
			// ""+i);

			pm = new PublicationMessage(publication);
			pm.setAggregationRequired(true);
			pm.setLastHopID(new MessageDestination(bc.getBrokerDestination() + "-sub"));
			pm.setNextHopID(new MessageDestination("Notification"));
			System.out.println("MSG CREATED" + pm.getPublication().getTimeStamp().getTime());// getMessageTime().getTime());
			bc.getInputQueue().addMessage(pm);
			// aggregator.addPublication(pm);

			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		assertTrue(sentMessages.size() != 0);
		String[] res = { "44", "145", "245", "345", "445", "545", "645", "745", "845", "945" };

		for (int i = 0; i < sentMessages.size(); i++) {
			PublicationMessage pubmsg = sentMessages.get(i);
			assertTrue(pubmsg.getPublication() instanceof AggregatedPublication);
			assertEquals(res[i], ((AggregatedPublication) pubmsg.getPublication()).getAggResult());
			System.out.println("Agg value " + ((AggregatedPublication) pubmsg.getPublication()).getAggResult());
		}

	}

}
