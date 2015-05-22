package ca.utoronto.msrg.padres.test.junit;

import junit.framework.TestCase;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerConfig;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCoreException;
import ca.utoronto.msrg.padres.broker.brokercore.InputQueueHandler;
import ca.utoronto.msrg.padres.client.Client;
import ca.utoronto.msrg.padres.client.ClientConfig;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.message.Advertisement;
import ca.utoronto.msrg.padres.common.message.AdvertisementMessage;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.MessageType;
import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.Subscription;
import ca.utoronto.msrg.padres.common.message.SubscriptionMessage;
import ca.utoronto.msrg.padres.common.message.parser.MessageFactory;
import ca.utoronto.msrg.padres.common.message.parser.ParseException;
import ca.utoronto.msrg.padres.common.util.LogSetup;
import ca.utoronto.msrg.padres.test.junit.tester.GenericBrokerTester;
import ca.utoronto.msrg.padres.test.junit.tester.TesterBrokerCore;
import ca.utoronto.msrg.padres.test.junit.tester.TesterClient;
import ca.utoronto.msrg.padres.test.junit.tester.TesterMessagePredicates;

public class TestAggregation extends TestCase {
	protected GenericBrokerTester _brokerTester;
	
	protected BrokerCore brokerCore1;

	protected BrokerCore brokerCore2;

	protected Client clientA;

	protected Client clientB;

	protected MessageWatchAppender messageWatcher;

	protected PatternFilter msgFilter;

	@Override
	protected void setUp() throws Exception {
		_brokerTester = new GenericBrokerTester();
		
		// setup the standard overlay B1-B2
		AllTests.setupStarNetwork01();
		// start the broker
		// AllTests.brokerConfig01.setHeartBeat(false);
		brokerCore1 = createNewBrokerCore(AllTests.brokerConfig01);
		brokerCore1.initialize();
		// start broker 2
		// AllTests.brokerConfig02.setHeartBeat(false);
		brokerCore2 = createNewBrokerCore(AllTests.brokerConfig02);
		brokerCore2.initialize();
		// setup filter
		messageWatcher = new MessageWatchAppender();
		msgFilter = new PatternFilter(InputQueueHandler.class.getName());
		msgFilter.setPattern(".*" + brokerCore1.getBrokerURI()
				+ ".+got message.+Publication.+OVERLAY-CONNECT_ACK.+");
		messageWatcher.addFilter(msgFilter);
		LogSetup.addAppender("MessagePath", messageWatcher);
		// start swingClientA for Broker1
		clientA = createNewClient(AllTests.clientConfigA);
		clientA.connect(brokerCore1.getBrokerURI());
		// start swingClientB for Broker2
		clientB = createNewClient(AllTests.clientConfigB);
		clientB.connect(brokerCore2.getBrokerURI());
	}
	
	protected Client createNewClient(ClientConfig newConfig) throws ClientException {
		return new TesterClient(_brokerTester, newConfig);
	}

	protected BrokerCore createNewBrokerCore(BrokerConfig brokerConfig) throws BrokerCoreException {
		return new TesterBrokerCore(_brokerTester, brokerConfig);
	}
	 /*
	protected void setUp() throws Exception {
		_brokerTester = new GenericBrokerTester();
		
		// start the broker
		brokerCore1 = createNewBrokerCore(AllTests.brokerConfig01);
		brokerCore1.initialize();
		brokerCore2 = createNewBrokerCore(AllTests.brokerConfig02);
		brokerCore2.initialize();
		// start the clientA
		clientA = createNewClient(AllTests.clientConfigA);
		clientA.connect(brokerCore1.getBrokerURI());
		clientB = createNewClient(AllTests.clientConfigB);
		clientB.connect(brokerCore2.getBrokerURI());
		messageWatcher = new MessageWatchAppender();
		msgFilter = new PatternFilter(Client.class.getName());
		msgFilter.setPattern(".*Client " + clientA.getClientID() + ".+Publication.+stock.+");
		// msgFilter.setPattern(".*Publication.+stock.+");
		messageWatcher.addFilter(msgFilter);
		LogSetup.addAppender("MessagePath", messageWatcher);
	}
	*/

	
	/*public void testPubSubMatchOnTwoClients() throws ParseException {
		// clientB is publisher, clientA is subscriber
		MessageDestination mdA = clientA.getClientDest();
		MessageDestination mdB = clientB.getClientDest();
		Advertisement adv = MessageFactory.createAdvertisementFromString("[class,eq,'STOCK'],[symbol,eq,'IBM'],[open,isPresent,1],[high,isPresent,1],[low,isPresent,1],[close,isPresent,1],[volume,isPresent,1],[date,isPresent,'A']");
		AdvertisementMessage advMsg = new AdvertisementMessage(adv, brokerCore1.getNewMessageID(),
				mdB);
		brokerCore1.routeMessage(advMsg, MessageDestination.INPUTQUEUE);

		Subscription sub = MessageFactory.createSubscriptionFromString("[class,eq,'STOCK'],[symbol,eq,'IBM'],[volume,>,20],[AGR,eq,'max'],[PAR,eq,volume],[PRD,eq,'1'],[NTF,eq,'1']");
		SubscriptionMessage subMsg = new SubscriptionMessage(sub, brokerCore2.getNewMessageID(), mdA);
		brokerCore2.routeMessage(subMsg, MessageDestination.INPUTQUEUE);

		Publication pub = MessageFactory.createPublicationFromString("[class,'STOCK'],[symbol,'IBM'],[open,37],[high,37],[low,37],[close,37],[volume,11291000],[date,'28-Dec-04'] ");
		PublicationMessage pubMsg = new PublicationMessage(pub, brokerCore1.getNewMessageID(), mdB);
		brokerCore1.routeMessage(pubMsg, MessageDestination.INPUTQUEUE);
		try {
			Thread.sleep(1000L);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// waiting for the message to be received
		//messageWatcher.getMessage(8);
		Publication expectedPub = clientA.getCurrentPub();
		//Publication expectedPub = MessageFactory.createPublicationFromString("[class,STOCK],[AGR_ON,'price'],[RANGE,'0'],[symbol,'IBM']");
		assertTrue(
				"The publication:[class,'stock'],[price,100] should be matched at clientB, but not" + pub +"####"+expectedPub,
				expectedPub.equalVals(pub));
	}*/
	
	
	
	public void testConnectionAndPubSubMatchingBetweenTwoExistingBrokers() throws ParseException, InterruptedException {
	
		_brokerTester.clearAll().
		expectReceipt(
			brokerCore1.getBrokerURI(),
			MessageType.SUBSCRIPTION,
			new TesterMessagePredicates().
				addPredicate("class", "eq", "STOCK").
				addPredicate("symbol", "eq", "IBM").
				addPredicate("volume", ">", 20L),
				
			"INPUTQUEUE");
		//clientA.handleCommand("a [class,eq,'stock'],[price,>,80]");
		clientA.handleCommand(" a [class,eq,'STOCK'],[symbol,eq,'IBM'],[volume,isPresent,1],[open,isPresent,1],[high,isPresent,1],[low,isPresent,1],[close,isPresent,1],[date,isPresent,'A']");
	//	clientB.handleCommand("s [class,eq,'stock'],[price,=,100]");
		clientB.handleCommand("s [class,eq,'STOCK'],[symbol,eq,'IBM'],[volume,>,20],[AGR,eq,'max'],[PAR,eq,volume],[PRD,eq,'1'],[NTF,eq,'1']");
		assertTrue("The subscription [class,eq,'STOCK'],[symbol,eq,'IBM'],[volume,>,20] should be sent to Broker1",
				_brokerTester.waitUntilExpectedEventsHappen());
		//clientA.handleCommand("a [class,eq,'stock'],[price,=,100]");
	//	clientB.handleCommand("s [class,eq,'stock'],[price,=,100]");
	//	assertTrue("There should be no msg routed out on Broker2",
		//	_brokerTester.waitUntilExpectedEventsHappen());
	
		_brokerTester.clearAll().
			expectSend(
				brokerCore1.getBrokerURI(),
				MessageType.PUBLICATION,
				new TesterMessagePredicates().
					addPredicate("class", "eq", "STOCK").
					addPredicate("symbol", "eq", "IBM").
					addPredicate("open", "=", 37L).
					addPredicate("high", "=", 37L).
					addPredicate("low", "=", 37L).
					addPredicate("close", "=", 37L).
					addPredicate("volume", "=", 11291000L).
					addPredicate("date", "=", "28-Dec-04"),
					 
					
					 brokerCore2.getBrokerURI());/*.
			expectReceipt(
				brokerCore2.getBrokerURI(),
				MessageType.PUBLICATION,
				new TesterMessagePredicates().
					addPredicate("class", "eq", "stock").
					addPredicate("price", "=", 100L),
				"INPUTQUEUE").
			expectSend(
				brokerCore2.getBrokerURI(),
				MessageType.PUBLICATION,
				new TesterMessagePredicates().
					addPredicate("class", "eq", "stock").
					addPredicate("price", "=", 100L),
				brokerCore2.getBrokerURI() + "-" + clientB.getClientID()).
			expectClientReceivePublication(
				clientB.getClientID(),
				new TesterMessagePredicates().
					addPredicate("class", "eq", "stock").
					addPredicate("price", "=", 100L));*/
		clientA.handleCommand("p [class,'STOCK'],[symbol,'IBM'],[open,37],[high,37],[low,37],[close,37],[volume,11291000],[date,'28-Dec-04']");
		assertTrue(_brokerTester.waitUntilExpectedEventsHappen());
	}
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		clientA.shutdown();
		clientB.shutdown();
		brokerCore1.shutdown();
		brokerCore2.shutdown();
		brokerCore1 = null;
		brokerCore2 = null;
		clientA = null;
		clientB = null;
		messageWatcher = null;
		_brokerTester = null;
		
		LogSetup.removeAppender("MessagePath", messageWatcher);
	}
	
	/*
	protected void tearDown() throws Exception {
		super.tearDown();
		clientA.shutdown();
		clientB.shutdown();
		brokerCore1.shutdown();
		brokerCore2.shutdown();
		LogSetup.removeAppender("MessagePath", messageWatcher);
	}*/
	// add test case for broker etc. etc. 
}
