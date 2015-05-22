package ca.utoronto.msrg.padres.test.junit.aggregation;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;
import ca.utoronto.msrg.padres.broker.aggregation.AggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.CollectionBasedAggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.PerOutgoingLinkAggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.adaptor.dummyAdaptor;
import ca.utoronto.msrg.padres.broker.aggregation.aggregator.AbstractAggregator;
import ca.utoronto.msrg.padres.broker.aggregation.aggregator.PerOutgoingLinkAbstractAggregator;
import ca.utoronto.msrg.padres.broker.aggregation.collector.AbstractCollector;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCoreException;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.Predicate;
import ca.utoronto.msrg.padres.common.message.SubscriptionMessage;
import ca.utoronto.msrg.padres.common.message.parser.MessageFactory;
import ca.utoronto.msrg.padres.common.message.parser.ParseException;
import ca.utoronto.msrg.padres.test.junit.aggregation.utility.TestClient;

public class StartTimeOfTheCurrentWindowTest extends TestCase {

	BrokerCore brokerCore1;
	BrokerCore brokerCore2;
	BrokerCore brokerCore3;

	TestClient clientPub1;
	// TestClient clientPub2;
	TestClient clientSub1;

	// TestClient clientSub2;

	protected void setUp() throws Exception {

		// ////////////////////////////////////////////
		// System.setProperty("padres.aggregation.implementation",
		// "OPTIMAL_AGGREGATION");
		// //////////////////////////////////////////////
		// System.setProperty("padres.aggregation.implementation",
		// "EARLY_AGGREGATION");

		System.setProperty("aggregation.client", "OFF");

		if (System.getProperty("padres.aggregation.implementation") == null)
			assertTrue(false);

		String broker1url = "-uri rmi://localhost:1101/Broker1";
		String broker2url = "-uri rmi://localhost:1102/Broker2 -n rmi://localhost:1101/Broker1";
		String broker3url = "-uri rmi://localhost:1103/Broker3 -n rmi://localhost:1102/Broker2";

		brokerCore1 = new BrokerCore(broker1url);
		brokerCore2 = new BrokerCore(broker2url);
		brokerCore3 = new BrokerCore(broker3url);

		brokerCore1.initialize();
		brokerCore2.initialize();
		brokerCore3.initialize();

		super.setUp();
	}

	@Override
	protected void tearDown() throws Exception {

		brokerCore1.shutdown();
		brokerCore2.shutdown();
		brokerCore3.shutdown();
		Thread.sleep(2000);
		super.tearDown();
	}

	public void testAggregatedMessageRecievedThreeBroker() throws ClientException, ParseException, BrokerCoreException, InterruptedException {

		clientPub1 = new TestClient("ClientPub1");
		clientPub1.connect("rmi://localhost:1101/Broker1");

		clientSub1 = new TestClient("clientSub1");
		clientSub1.connect("rmi://localhost:1103/Broker3");

		clientPub1.advertise(MessageFactory.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"), "rmi://localhost:1101/Broker1");
		Thread.sleep(1000L);
		clientSub1.subscribe("[class,eq,STOCK],[value,>,60],[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'5'],[NTF,eq,'1']",
				"rmi://localhost:1103/Broker3");

		Thread.sleep(1000L);
		long messageTimeBrokerCore1 = 0;

		Map<String, SubscriptionMessage> subMsgSet1 = brokerCore1.getSubscriptions();
		for (SubscriptionMessage subMesgIt : subMsgSet1.values()) {
			Map<String, Predicate> PredicateMap = subMesgIt.getSubscription().getPredicateMap();
			for (Predicate pred : PredicateMap.values()) {
				if (pred.getValue().equals("STOCK")) {
					messageTimeBrokerCore1 = subMesgIt.getStartOfTheAggregationWindow();
				}
			}
		}

		long messageTimeBrokerCore2 = -1;
		Map<String, SubscriptionMessage> subMsgSet2 = brokerCore2.getSubscriptions();
		for (SubscriptionMessage subMesgIt : subMsgSet2.values()) {
			Map<String, Predicate> PredicateMap = subMesgIt.getSubscription().getPredicateMap();
			for (Predicate pred : PredicateMap.values()) {
				if (pred.getValue().equals("STOCK")) {
					messageTimeBrokerCore2 = subMesgIt.getStartOfTheAggregationWindow();
				}
			}
		}

		assertEquals(messageTimeBrokerCore1, messageTimeBrokerCore2);

		Set<AbstractAggregator> aggregatorSetFromBroker1 = null;
		Set<AbstractAggregator> aggregatorSetFromBroker2 = null;

		AggregationEngine eng1, eng2;
		eng1 = brokerCore1.getRouter().getPostProcessor().getAggregationEngine();
		eng2 = brokerCore2.getRouter().getPostProcessor().getAggregationEngine();

		if ((eng1 instanceof CollectionBasedAggregationEngine)) {
			aggregatorSetFromBroker1 = ((CollectionBasedAggregationEngine) eng1).getAggregatorSet();
			aggregatorSetFromBroker2 = ((CollectionBasedAggregationEngine) eng2).getAggregatorSet();

		} else if (eng1 instanceof PerOutgoingLinkAggregationEngine) {

			PerOutgoingLinkAggregationEngine<PerOutgoingLinkAbstractAggregator, AbstractCollector, dummyAdaptor> aggEngine1 = ((PerOutgoingLinkAggregationEngine<PerOutgoingLinkAbstractAggregator, AbstractCollector, dummyAdaptor>) eng1);

			PerOutgoingLinkAggregationEngine<PerOutgoingLinkAbstractAggregator, AbstractCollector, dummyAdaptor> aggEngine2 = ((PerOutgoingLinkAggregationEngine<PerOutgoingLinkAbstractAggregator, AbstractCollector, dummyAdaptor>) eng2);

			Set<MessageDestination> outgoingLinks1 = (Set<MessageDestination>) aggEngine1.getOutgoingLinks();
			Set<MessageDestination> outgoingLinks2 = (Set<MessageDestination>) aggEngine2.getOutgoingLinks();

			aggregatorSetFromBroker1 = new HashSet<AbstractAggregator>();
			aggregatorSetFromBroker2 = new HashSet<AbstractAggregator>();

			for (MessageDestination md : outgoingLinks1) {
				aggregatorSetFromBroker1.addAll(aggEngine1.getAggregatorMapPerOutgoingLink().get(md));
			}
			for (MessageDestination md : outgoingLinks2) {
				aggregatorSetFromBroker2.addAll(aggEngine2.getAggregatorMapPerOutgoingLink().get(md));
			}
		}

		assertEquals(aggregatorSetFromBroker1.size(), 1);
		assertEquals(aggregatorSetFromBroker2.size(), 1);

		assertEquals(((AbstractAggregator) aggregatorSetFromBroker1.toArray()[0]).getStartTimeOfTheCurrentWindow(),
				((AbstractAggregator) aggregatorSetFromBroker2.toArray()[0]).getStartTimeOfTheCurrentWindow());

		// assertTrue( clientSub1.messageRecieved);
	}
}
