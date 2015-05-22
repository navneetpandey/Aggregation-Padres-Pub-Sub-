package ca.utoronto.msrg.padres.test.junit.aggregation;

import java.io.IOException;

import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCoreException;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.parser.MessageFactory;
import ca.utoronto.msrg.padres.common.message.parser.ParseException;
import ca.utoronto.msrg.padres.test.junit.aggregation.utility.TestClient;
import junit.framework.TestCase;

public class NormalSubscriptionTest extends TestCase {
	BrokerCore brokerCore1;
	BrokerCore brokerCore2;
	BrokerCore brokerCore3;
	BrokerCore brokerCore4;
	BrokerCore brokerCore5;
	BrokerCore brokerCore6;
	BrokerCore brokerCore7;
	BrokerCore brokerCore8;
	BrokerCore brokerCore9;
	BrokerCore brokerCore10;
	BrokerCore brokerCore11;
	BrokerCore brokerCore12;
	BrokerCore brokerCore13;
	BrokerCore brokerCore14;
	BrokerCore brokerCore15;
	BrokerCore brokerCore16;

	TestClient clientPub1;

	TestClient clientPub5;
	TestClient clientPub6;
	TestClient clientPub7;
	TestClient clientPub8;
	TestClient clientPub9;
	TestClient clientPub10;
	TestClient clientPub11;
	TestClient clientPub12;
	TestClient clientPub13;

	TestClient clientSub1;
	TestClient clientSub2;

	TestClient clientSub8;
	TestClient clientSub9;
	TestClient clientSub10;
	TestClient clientSub11;
	TestClient clientSub12;
	TestClient clientSub13;
	TestClient clientSub14;
	TestClient clientSub15;
	TestClient clientSub16;

	public void testOptimal() throws BrokerCoreException, ClientException,
			ParseException, InterruptedException, IOException {
		System.setProperty("padres.aggregation.implementation",
				"OPTIMAL_AGGREGATION");
		System.setProperty("aggregation.client", "OFF");

		brokerCore1.initialize();
		brokerCore2.initialize();
		brokerCore3.initialize();
		brokerCore4.initialize();
		brokerCore5.initialize();
		brokerCore6.initialize();
		brokerCore7.initialize();
		brokerCore8.initialize();
		brokerCore9.initialize();
		brokerCore10.initialize();
		brokerCore11.initialize();
		brokerCore12.initialize();
		brokerCore13.initialize();
		brokerCore14.initialize();
		brokerCore15.initialize();
		brokerCore16.initialize();

		aggregatedMessageRecievedOneBroker();
		aggregatedMessageRecievedTwoBroker();
		aggregatedMessageRecievedThreeBroker();
		aggregatedMessageRecievedFourBroker();
		aggregatedMessageRecievedExperimentTopology();
		aggregatedMessageRecievedExperimentTopologyForNonOverlap();

	}

	public void testAdaptive() throws BrokerCoreException, ClientException,
			ParseException, InterruptedException, IOException {
		System.setProperty("padres.aggregation.implementation",
				"ADAPTIVE_AGGREGATION");
		System.setProperty("aggregation.client", "OFF");

		brokerCore1.initialize();
		brokerCore2.initialize();
		brokerCore3.initialize();
		brokerCore4.initialize();
		brokerCore5.initialize();
		brokerCore6.initialize();
		brokerCore7.initialize();
		brokerCore8.initialize();
		brokerCore9.initialize();
		brokerCore10.initialize();
		brokerCore11.initialize();
		brokerCore12.initialize();
		brokerCore13.initialize();
		brokerCore14.initialize();
		brokerCore15.initialize();
		brokerCore16.initialize();
		// aggregatedMessageRecievedOneBroker();
		// aggregatedMessageRecievedTwoBroker();
		// aggregatedMessageRecievedThreeBroker();
		// aggregatedMessageRecievedFourBroker();
		aggregatedMessageRecievedExperimentTopology();
		// aggregatedMessageRecievedExperimentTopologyForNonOverlap();

	}

	public void testEarly() throws BrokerCoreException, ClientException,
			ParseException, InterruptedException, IOException {
		System.setProperty("padres.aggregation.implementation",
				"EARLY_AGGREGATION");
		System.setProperty("aggregation.client", "OFF");

		brokerCore1.initialize();
		brokerCore2.initialize();
		brokerCore3.initialize();
		brokerCore4.initialize();
		brokerCore5.initialize();
		brokerCore6.initialize();
		brokerCore7.initialize();
		brokerCore8.initialize();
		brokerCore9.initialize();
		brokerCore10.initialize();
		brokerCore11.initialize();
		brokerCore12.initialize();
		brokerCore13.initialize();
		brokerCore14.initialize();
		brokerCore15.initialize();
		brokerCore16.initialize();
		// aggregatedMessageRecievedOneBroker();
		// aggregatedMessageRecievedTwoBroker();
		// aggregatedMessageRecievedThreeBroker();
		// aggregatedMessageRecievedFourBroker();
		aggregatedMessageRecievedExperimentTopology();
		aggregatedMessageRecievedExperimentTopologyForNonOverlap();

	}

	public void testLate() throws BrokerCoreException, ClientException,
			ParseException, InterruptedException, IOException {
		System.setProperty("padres.aggregation.implementation",
				"LATE_AGGREGATION");
		System.setProperty("aggregation.client", "OFF");

		brokerCore1.initialize();
		brokerCore2.initialize();
		brokerCore3.initialize();
		brokerCore4.initialize();
		brokerCore5.initialize();
		brokerCore6.initialize();
		brokerCore7.initialize();
		brokerCore8.initialize();
		brokerCore9.initialize();
		brokerCore10.initialize();
		brokerCore11.initialize();
		brokerCore12.initialize();
		brokerCore13.initialize();
		brokerCore14.initialize();
		brokerCore15.initialize();
		brokerCore16.initialize();
		aggregatedMessageRecievedOneBroker();
		aggregatedMessageRecievedTwoBroker();
		aggregatedMessageRecievedThreeBroker();
		aggregatedMessageRecievedFourBroker();
		aggregatedMessageRecievedExperimentTopology();
		aggregatedMessageRecievedExperimentTopologyForNonOverlap();

	}

	public void testFG() throws BrokerCoreException, ClientException,
			ParseException, InterruptedException, IOException {
		System.setProperty("padres.aggregation.implementation",
				"FG_AGGREGATION");
		System.setProperty("aggregation.client", "OFF");

		brokerCore1.initialize();
		brokerCore2.initialize();
		brokerCore3.initialize();
		brokerCore4.initialize();
		brokerCore5.initialize();
		brokerCore6.initialize();
		// brokerCore7.initialize();
		// brokerCore8.initialize();
		// brokerCore9.initialize();
		// brokerCore10.initialize();
		// brokerCore11.initialize();
		// brokerCore12.initialize();
		// brokerCore13.initialize();
		// brokerCore14.initialize();
		// brokerCore15.initialize();
		brokerCore16.initialize();
		// aggregatedMessageRecievedOneBroker();
		// aggregatedMessageRecievedTwoBroker();

		// aggregatedMessageRecievedOneBroker();
		// aggregatedMessageRecievedTwoBroker();
		aggregatedMessageRecievedThreeBroker();
		// aggregatedMessageRecievedFourBroker();
		// aggregatedMessageRecievedExperimentTopology();
		// aggregatedMessageRecievedExperimentTopologyForNonOverlap();

	}

	public void testFGOnlyFra() throws BrokerCoreException, ClientException,
			ParseException, InterruptedException, IOException {
		System.setProperty("padres.aggregation.implementation",
				"FG_AGGREGATION");
		System.setProperty("aggregation.client", "OFF");

		brokerCore1.initialize();
		brokerCore2.initialize();
		brokerCore3.initialize();
		brokerCore4.initialize();
		brokerCore5.initialize();
		brokerCore6.initialize();
		brokerCore7.initialize();
		brokerCore8.initialize();
		brokerCore9.initialize();
		brokerCore10.initialize();
		brokerCore11.initialize();
		brokerCore12.initialize();
		brokerCore13.initialize();
		brokerCore14.initialize();
		brokerCore15.initialize();
		brokerCore16.initialize();
		aggregatedMessageRecievedExperimentTopology();

	}

	public void testFGOnlyFra1() throws BrokerCoreException, ClientException,
			ParseException, InterruptedException, IOException {
//		System.setProperty("padres.aggregation.implementation",
//				"FG_AGGREGATION");
//		System.setProperty("aggregation.client", "OFF");

		brokerCore1.initialize();
		brokerCore2.initialize();
		brokerCore3.initialize();
		brokerCore4.initialize();
		brokerCore5.initialize();
		brokerCore6.initialize();
		brokerCore7.initialize();
		brokerCore8.initialize();
		brokerCore9.initialize();
		brokerCore10.initialize();
		brokerCore11.initialize();
		brokerCore12.initialize();
		brokerCore13.initialize();
		brokerCore14.initialize();
		brokerCore15.initialize();
		brokerCore16.initialize();
//		testAggregatedMessageRecievedThreeBrokerWithMixPublisherAndAggregateSubscriber();
//		testAggregatedMessageRecievedThreeBrokerWithMixPublisherAndRegularSubscriber();
		//experimentalSetup100Publication();
		experimentalSetup100PublicationWithoutOverlap();
	}

	@Override
	protected void setUp() throws Exception {
		System.setProperty("aggregation.client", "OFF");
		System.setProperty("padres.aggregation.implementation",
				"FG_AGGREGATION");
		// System.setProperty("padres.aggregation.implementation","ADAPTIVE_AGGREGATION");

		/*
		 * if (System.getProperty("padres.aggregation.implementation") == null)
		 * assertTrue(false);
		 */

		String broker1url = "-uri rmi://localhost:1101/Broker1";
		String broker2url = "-uri rmi://localhost:1102/Broker2 -n rmi://localhost:1101/Broker1";
		String broker3url = "-uri rmi://localhost:1103/Broker3 -n rmi://localhost:1102/Broker2";
		String broker4url = "-uri rmi://localhost:1104/Broker4 -n rmi://localhost:1103/Broker3";
		String broker5url = "-uri rmi://localhost:1105/Broker5 -n rmi://localhost:1101/Broker1";
		String broker6url = "-uri rmi://localhost:1106/Broker6 -n rmi://localhost:1101/Broker1";
		String broker7url = "-uri rmi://localhost:1107/Broker7 -n rmi://localhost:1101/Broker1";
		String broker8url = "-uri rmi://localhost:1108/Broker8 -n rmi://localhost:1102/Broker2";
		String broker9url = "-uri rmi://localhost:1109/Broker9 -n rmi://localhost:1102/Broker2";
		String broker10url = "-uri rmi://localhost:1110/Broker10 -n rmi://localhost:1102/Broker2";
		String broker11url = "-uri rmi://localhost:1111/Broker11 -n rmi://localhost:1103/Broker3";
		String broker12url = "-uri rmi://localhost:1112/Broker12 -n rmi://localhost:1103/Broker3";
		String broker13url = "-uri rmi://localhost:1113/Broker13 -n rmi://localhost:1103/Broker3";
		String broker14url = "-uri rmi://localhost:1114/Broker14 -n rmi://localhost:1104/Broker4";
		String broker15url = "-uri rmi://localhost:1115/Broker15 -n rmi://localhost:1104/Broker4";
		String broker16url = "-uri rmi://localhost:1116/Broker16 -n rmi://localhost:1104/Broker4";

		brokerCore1 = new BrokerCore(broker1url);
		brokerCore2 = new BrokerCore(broker2url);
		brokerCore3 = new BrokerCore(broker3url);
		brokerCore4 = new BrokerCore(broker4url);
		brokerCore5 = new BrokerCore(broker5url);
		brokerCore6 = new BrokerCore(broker6url);
		brokerCore7 = new BrokerCore(broker7url);
		brokerCore8 = new BrokerCore(broker8url);
		brokerCore9 = new BrokerCore(broker9url);
		brokerCore10 = new BrokerCore(broker10url);
		brokerCore11 = new BrokerCore(broker11url);
		brokerCore12 = new BrokerCore(broker12url);
		brokerCore13 = new BrokerCore(broker13url);
		brokerCore14 = new BrokerCore(broker14url);
		brokerCore15 = new BrokerCore(broker15url);
		brokerCore16 = new BrokerCore(broker16url);

		super.setUp();
	}

	public void aggregatedMessageRecievedOneBroker() throws ClientException,
			ParseException, BrokerCoreException, InterruptedException {

		clientPub1 = new TestClient("ClientPub1");
		clientPub1.connect("rmi://localhost:1101/Broker1");

		clientSub1 = new TestClient("clientSub1");
		clientSub1.connect("rmi://localhost:1101/Broker1");
		clientSub2 = new TestClient("clientSub2");
		clientSub2.connect("rmi://localhost:1101/Broker1");

		clientPub1
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1101/Broker1");
		Thread.sleep(100L);
		clientSub1.subscribe("[class,eq,STOCK],[value,>,60]",
				"rmi://localhost:1101/Broker1");
		clientSub2
				.subscribe(
						"[class,eq,STOCK],[value,>,50],[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'5'],[NTF,eq,'5']",
						"rmi://localhost:1101/Broker1");

		Thread.sleep(100L);
		clientPub1.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,80]"),
				"rmi://localhost:1101/Broker1");
		clientPub1.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,55]"),
				"rmi://localhost:1101/Broker1");

		Thread.sleep(10000L);

		assertTrue(clientSub1.isMessageReceived());
		assertEquals(clientSub1.getAllReceivedMessage().size(), 1);
		Long i = (Long) ((PublicationMessage) clientSub1
				.getAllReceivedMessage().get(0)).getPublication().getPairMap()
				.get("value");
		assertTrue(i == 80);

		assertTrue(clientSub2.isMessageReceived());
		assertEquals(clientSub2.getAllReceivedMessage().size(), 1);
		assertEquals(clientSub2.getReceivedAggregatedMessageList().size(), 1);

	}

	public void aggregatedMessageRecievedTwoBroker() throws ClientException,
			ParseException, BrokerCoreException, InterruptedException {

		clientPub1 = new TestClient("ClientPub1");
		clientPub1.connect("rmi://localhost:1101/Broker1");

		clientSub1 = new TestClient("clientSub1");
		clientSub1.connect("rmi://localhost:1102/Broker2");
		clientSub2 = new TestClient("clientSub2");
		clientSub2.connect("rmi://localhost:1102/Broker2");

		clientPub1
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1101/Broker1");
		Thread.sleep(100L);
		clientSub1.subscribe("[class,eq,STOCK],[value,>,60] ",
				"rmi://localhost:1102/Broker2");

		clientSub2
				.subscribe(
						"[class,eq,STOCK],[value,>,50],[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'5'],[NTF,eq,'5']",
						"rmi://localhost:1102/Broker2");
		Thread.sleep(100L);
		clientPub1.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,80]"),
				"rmi://localhost:1101/Broker1");

		clientPub1.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,55]"),
				"rmi://localhost:1101/Broker1");

		Thread.sleep(6000L);

		assertTrue(clientSub1.isMessageReceived());
		assertEquals(clientSub1.getAllReceivedMessage().size(), 1);
		Long i = (Long) ((PublicationMessage) clientSub1
				.getAllReceivedMessage().get(0)).getPublication().getPairMap()
				.get("value");
		assertTrue(i == 80);

		assertTrue(clientSub2.isMessageReceived());
		assertEquals(clientSub2.getAllReceivedMessage().size(), 1);
		assertEquals(clientSub2.getReceivedAggregatedMessageList().size(), 1);
	}

	public void aggregatedMessageRecievedThreeBroker() throws ClientException,
			ParseException, BrokerCoreException, InterruptedException {

		clientPub1 = new TestClient("ClientPub1");
		clientPub1.connect("rmi://localhost:1101/Broker1");

		clientSub1 = new TestClient("clientSub1");
		clientSub1.connect("rmi://localhost:1103/Broker3");

		clientSub2 = new TestClient("clientSub2");
		clientSub2.connect("rmi://localhost:1103/Broker3");

		clientPub1
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1101/Broker1");
		Thread.sleep(100L);
		clientSub1.subscribe("[class,eq,STOCK],[value,>,60]",
				"rmi://localhost:1103/Broker3");

		clientSub2
				.subscribe(
						"[class,eq,STOCK],[value,>,50],[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'5'],[NTF,eq,'5']",
						"rmi://localhost:1103/Broker3");
		Thread.sleep(100L);
		clientPub1.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,80]"),
				"rmi://localhost:1101/Broker1");

		clientPub1.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,55]"),
				"rmi://localhost:1101/Broker1");

		Thread.sleep(7000L);

		assertTrue(clientSub1.isMessageReceived());
		assertEquals(clientSub1.getAllReceivedMessage().size(), 1);
		Long i = (Long) ((PublicationMessage) clientSub1
				.getAllReceivedMessage().get(0)).getPublication().getPairMap()
				.get("value");
		assertTrue(i == 80);

		assertTrue(clientSub2.isMessageReceived());
		assertEquals(clientSub2.getAllReceivedMessage().size(), 1);
		assertEquals(clientSub2.getReceivedAggregatedMessageList().size(), 1);
	}

	public void aggregatedMessageRecievedFourBroker() throws ClientException,
			ParseException, BrokerCoreException, InterruptedException {

		clientPub1 = new TestClient("ClientPub1");
		clientPub1.connect("rmi://localhost:1101/Broker1");

		clientSub1 = new TestClient("clientSub1");
		clientSub1.connect("rmi://localhost:1104/Broker4");

		clientSub2 = new TestClient("clientSub2");
		clientSub2.connect("rmi://localhost:1104/Broker4");

		clientPub1
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1101/Broker1");
		Thread.sleep(100L);
		clientSub1.subscribe("[class,eq,STOCK],[value,>,60] ",
				"rmi://localhost:1104/Broker4");

		clientSub2
				.subscribe(
						"[class,eq,STOCK],[value,>,60],[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'5'],[NTF,eq,'1']",
						"rmi://localhost:1104/Broker4");
		Thread.sleep(100L);
		clientPub1.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,80]"),
				"rmi://localhost:1101/Broker1");

		Thread.sleep(6000L);
		assertTrue(clientSub1.isMessageReceived());
		assertEquals(clientSub1.getAllReceivedMessage().size(), 1);
		Long i = (Long) ((PublicationMessage) clientSub1
				.getAllReceivedMessage().get(0)).getPublication().getPairMap()
				.get("value");
		assertTrue(i == 80);

		assertTrue(clientSub2.isMessageReceived());
		assertEquals(clientSub2.getAllReceivedMessage().size(), 5);
		assertEquals(clientSub2.getReceivedAggregatedMessageList().size(), 5);
	}

	public void testAggregatedMessageRecievedThreeBrokerWithMixPublisherAndRegularSubscriber()
			throws ClientException, ParseException, BrokerCoreException,
			InterruptedException {

		brokerCore1.initialize();
		brokerCore2.initialize();
		brokerCore3.initialize();
		clientPub1 = new TestClient("ClientPub1");
		clientPub1.connect("rmi://localhost:1101/Broker1");

		clientSub1 = new TestClient("clientSub1");
		clientSub1.connect("rmi://localhost:1101/Broker1");

		clientSub2 = new TestClient("clientSub2");
		clientSub2.connect("rmi://localhost:1103/Broker3");

		clientPub1
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1101/Broker1");
		Thread.sleep(100L);
		clientSub1.subscribe("[class,eq,STOCK],[value,>,60]",
				"rmi://localhost:1101/Broker1");

		clientSub2
				.subscribe(
						"[class,eq,STOCK],[value,>,60],[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'5'],[NTF,eq,'1']",
						"rmi://localhost:1103/Broker3");
		Thread.sleep(100L);
		clientPub1.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,80]"),
				"rmi://localhost:1101/Broker1");

		Thread.sleep(6000L);

		assertTrue(clientSub1.isMessageReceived());
		assertEquals(clientSub1.getAllReceivedMessage().size(), 1);
		Long i = (Long) ((PublicationMessage) clientSub1
				.getAllReceivedMessage().get(0)).getPublication().getPairMap()
				.get("value");
		assertTrue(i == 80);
	}

	public void testAggregatedMessageRecievedThreeBrokerWithMixPublisherAndAggregateSubscriber()
			throws ClientException, ParseException, BrokerCoreException,
			InterruptedException {

		brokerCore1.initialize();
		brokerCore2.initialize();
		brokerCore3.initialize();

		clientPub1 = new TestClient("ClientPub1");
		clientPub1.connect("rmi://localhost:1101/Broker1");

		clientSub1 = new TestClient("clientSub1");
		clientSub1.connect("rmi://localhost:1101/Broker1");

		clientSub2 = new TestClient("clientSub2");
		clientSub2.connect("rmi://localhost:1101/Broker1");

		TestClient clientSub3;
		clientSub3 = new TestClient("clientSub3");
		clientSub3.connect("rmi://localhost:1103/Broker3");

		clientPub1
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1101/Broker1");
		Thread.sleep(100L);

		clientSub1
				.subscribe(
						"[class,eq,STOCK],[value,>,50],[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'5'],[NTF,eq,'5']",
						"rmi://localhost:1101/Broker1");

		clientSub2.subscribe("[class,eq,STOCK],[value,>,60]",
				"rmi://localhost:1101/Broker1");
		clientSub3.subscribe("[class,eq,STOCK],[value,>,60]",
				"rmi://localhost:1103/Broker3");

		Thread.sleep(100L);
		clientPub1.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,80]"),
				"rmi://localhost:1101/Broker1");
		clientPub1.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,55]"),
				"rmi://localhost:1101/Broker1");

		Thread.sleep(6000L);

		assertTrue(clientSub1.isMessageReceived());
		assertEquals(clientSub1.getAllReceivedMessage().size(), 1);
		assertEquals(clientSub1.getReceivedAggregatedMessageList().size(), 1);

		assertTrue(clientSub2.isMessageReceived());
		assertEquals(clientSub2.getAllReceivedMessage().size(), 1);
		Long i1 = (Long) ((PublicationMessage) clientSub2
				.getAllReceivedMessage().get(0)).getPublication().getPairMap()
				.get("value");
		assertTrue(i1 == 80);

		assertTrue(clientSub3.isMessageReceived());
		assertEquals(clientSub3.getAllReceivedMessage().size(), 1);
		Long i2 = (Long) ((PublicationMessage) clientSub3
				.getAllReceivedMessage().get(0)).getPublication().getPairMap()
				.get("value");
		assertTrue(i2 == 80);
	}

	public void aggregatedMessageRecievedExperimentTopology()
			throws ClientException, ParseException, BrokerCoreException,
			InterruptedException {

		clientPub5 = new TestClient("ClientPub5");
		clientPub5.connect("rmi://localhost:1105/Broker5");
		clientPub6 = new TestClient("ClientPub6");
		clientPub6.connect("rmi://localhost:1106/Broker6");
		clientPub7 = new TestClient("ClientPub7");
		clientPub7.connect("rmi://localhost:1107/Broker7");
		clientPub8 = new TestClient("ClientPub8");
		clientPub8.connect("rmi://localhost:1108/Broker8");
		clientPub9 = new TestClient("ClientPub9");
		clientPub9.connect("rmi://localhost:1109/Broker9");
		clientPub10 = new TestClient("ClientPub10");
		clientPub10.connect("rmi://localhost:1110/Broker10");
		clientPub11 = new TestClient("ClientPub11");
		clientPub11.connect("rmi://localhost:1111/Broker11");
		clientPub12 = new TestClient("ClientPub12");
		clientPub12.connect("rmi://localhost:1112/Broker12");
		clientPub13 = new TestClient("ClientPub13");
		clientPub13.connect("rmi://localhost:1113/Broker13");

		clientSub8 = new TestClient("clientSub8");
		clientSub8.connect("rmi://localhost:1108/Broker8");
		clientSub9 = new TestClient("clientSub9");
		clientSub9.connect("rmi://localhost:1109/Broker9");
		clientSub10 = new TestClient("clientSub10");
		clientSub10.connect("rmi://localhost:1110/Broker10");
		clientSub11 = new TestClient("clientSub11");
		clientSub11.connect("rmi://localhost:1111/Broker11");
		clientSub12 = new TestClient("clientSub12");
		clientSub12.connect("rmi://localhost:1112/Broker12");
		clientSub13 = new TestClient("clientSub13");
		clientSub13.connect("rmi://localhost:1113/Broker13");
		clientSub14 = new TestClient("clientSub14");
		clientSub14.connect("rmi://localhost:1114/Broker14");
		clientSub15 = new TestClient("clientSub15");
		clientSub15.connect("rmi://localhost:1115/Broker15");
		clientSub16 = new TestClient("clientSub16");
		clientSub16.connect("rmi://localhost:1116/Broker16");

		Long l1 = 10L;
		clientPub5
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1105/Broker5");
		Thread.sleep(l1);
		clientPub6
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1106/Broker6");
		Thread.sleep(l1);
		clientPub7
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1107/Broker7");
		Thread.sleep(l1);
		clientPub8
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1108/Broker8");
		Thread.sleep(l1);
		clientPub9
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1109/Broker9");
		Thread.sleep(l1);
		clientPub10
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1110/Broker10");
		Thread.sleep(l1);
		clientPub11
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1111/Broker11");
		Thread.sleep(l1);
		clientPub12
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1112/Broker12");
		Thread.sleep(l1);
		clientPub13
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1113/Broker13");

		Thread.sleep(100L);
		Long l2 = 10L;
		clientSub8.subscribe("[class,eq,STOCK],[value,>,60] ",
				"rmi://localhost:1108/Broker8");
		Thread.sleep(l2);
		clientSub9.subscribe("[class,eq,STOCK],[value,>,70] ",
				"rmi://localhost:1109/Broker9");
		Thread.sleep(l2);
		clientSub10.subscribe("[class,eq,STOCK],[value,>,80] ",
				"rmi://localhost:1110/Broker10");
		Thread.sleep(l2);
		clientSub11.subscribe("[class,eq,STOCK],[value,>,90] ",
				"rmi://localhost:1111/Broker11");
		Thread.sleep(l2);
		clientSub12.subscribe("[class,eq,STOCK],[value,>,100] ",
				"rmi://localhost:1112/Broker12");
		Thread.sleep(l2);
		clientSub13.subscribe("[class,eq,STOCK],[value,>,110] ",
				"rmi://localhost:1113/Broker13");
		Thread.sleep(l2);
		clientSub14.subscribe("[class,eq,STOCK],[value,>,120] ",
				"rmi://localhost:1114/Broker14");
		Thread.sleep(l2);
		clientSub15.subscribe("[class,eq,STOCK],[value,>,130] ",
				"rmi://localhost:1115/Broker15");
		Thread.sleep(l2);
		clientSub16.subscribe("[class,eq,STOCK],[value,>,140] ",
				"rmi://localhost:1116/Broker16");

		Thread.sleep(100L);
		Long l3 = 10L;
		clientSub8
				.subscribe(
						"[class,eq,STOCK],[value,>,60],[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2'] ",
						"rmi://localhost:1108/Broker8");
		Thread.sleep(l3);
		clientSub9
				.subscribe(
						"[class,eq,STOCK],[value,>,70] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1109/Broker9");
		Thread.sleep(l3);
		clientSub10
				.subscribe(
						"[class,eq,STOCK],[value,>,80] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1110/Broker10");
		Thread.sleep(l3);
		clientSub11
				.subscribe(
						"[class,eq,STOCK],[value,>,90] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1111/Broker11");
		Thread.sleep(l3);
		clientSub12
				.subscribe(
						"[class,eq,STOCK],[value,>,100] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1112/Broker12");
		Thread.sleep(l3);
		clientSub13
				.subscribe(
						"[class,eq,STOCK],[value,>,110],[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2'] ",
						"rmi://localhost:1113/Broker13");
		Thread.sleep(l3);
		clientSub14
				.subscribe(
						"[class,eq,STOCK],[value,>,120] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1114/Broker14");
		Thread.sleep(l3);
		clientSub15
				.subscribe(
						"[class,eq,STOCK],[value,>,130] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1115/Broker15");
		Thread.sleep(l3);
		clientSub16
				.subscribe(
						"[class,eq,STOCK],[value,>,140] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1116/Broker16");

		Thread.sleep(100L);
		Long l4 = 10L;
		clientPub5.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,80]"),
				"rmi://localhost:1105/Broker5");
		Thread.sleep(l4);
		clientPub6.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,90]"),
				"rmi://localhost:1106/Broker6");
		Thread.sleep(l4);
		clientPub7.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,100]"),
				"rmi://localhost:1107/Broker7");
		Thread.sleep(l4);
		clientPub8.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,110]"),
				"rmi://localhost:1108/Broker8");
		Thread.sleep(l4);
		clientPub9.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,120]"),
				"rmi://localhost:1109/Broker9");
		Thread.sleep(l4);
		clientPub10.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,130]"),
				"rmi://localhost:1110/Broker10");
		Thread.sleep(l4);
		clientPub11.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,140]"),
				"rmi://localhost:1111/Broker11");
		Thread.sleep(l4);
		clientPub12.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,150]"),
				"rmi://localhost:1112/Broker12");
		Thread.sleep(l4);
		clientPub13.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,1600]"),
				"rmi://localhost:1113/Broker13");
		Thread.sleep(l4);

		Thread.sleep(600L);
		assertTrue(clientSub8.isMessageReceived());
		assertEquals(clientSub8.getAllReceivedMessage().size(), 9);
		assertEquals(clientSub8.getReceivedRegularMessageList().size(), 9);

		assertTrue(clientSub9.isMessageReceived());
		assertEquals(clientSub9.getAllReceivedMessage().size(), 9);
		assertEquals(clientSub9.getReceivedRegularMessageList().size(), 9);

		assertTrue(clientSub10.isMessageReceived());
		assertEquals(clientSub10.getAllReceivedMessage().size(), 8);
		assertEquals(clientSub10.getReceivedRegularMessageList().size(), 8);

		assertTrue(clientSub11.isMessageReceived());
		assertEquals(clientSub11.getAllReceivedMessage().size(), 7);
		assertEquals(clientSub11.getReceivedRegularMessageList().size(), 7);

		assertTrue(clientSub12.isMessageReceived());
		assertEquals(clientSub12.getAllReceivedMessage().size(), 6);
		assertEquals(clientSub12.getReceivedRegularMessageList().size(), 6);

		assertTrue(clientSub13.isMessageReceived());
		assertEquals(clientSub13.getAllReceivedMessage().size(), 5);
		assertEquals(clientSub13.getReceivedRegularMessageList().size(), 5);

		assertTrue(clientSub14.isMessageReceived());
		assertEquals(clientSub14.getAllReceivedMessage().size(), 4);
		assertEquals(clientSub14.getReceivedRegularMessageList().size(), 4);

		assertTrue(clientSub15.isMessageReceived());
		assertEquals(clientSub15.getAllReceivedMessage().size(), 3);
		assertEquals(clientSub15.getReceivedRegularMessageList().size(), 3);

		assertTrue(clientSub16.isMessageReceived());
		assertEquals(clientSub16.getAllReceivedMessage().size(), 2);
		assertEquals(clientSub16.getReceivedRegularMessageList().size(), 2);

		Thread.sleep(3000L);
		assertEquals(9, clientSub8.getReceivedRegularMessageList().size());
		assertTrue(clientSub8.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub8.getReceivedAggregatedMessageList().size() > 0);

		assertTrue(clientSub9.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub9.getReceivedAggregatedMessageList().size() > 0);

		assertTrue(clientSub10.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub10.getReceivedAggregatedMessageList().size() > 0);

		assertTrue(clientSub11.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub11.getReceivedAggregatedMessageList().size() > 0);

		assertTrue(clientSub12.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub12.getReceivedAggregatedMessageList().size() > 0);

		assertTrue(clientSub13.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub13.getReceivedAggregatedMessageList().size() > 0);

		Thread.sleep(10000L);

		assertTrue(clientSub14.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub14.getReceivedAggregatedMessageList().size() > 0);

		assertTrue(clientSub15.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub15.getReceivedAggregatedMessageList().size() > 0);

		assertTrue(clientSub16.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub16.getReceivedAggregatedMessageList().size() > 0);

		// assertTrue(clientSub9.isMessageReceived());
		// assertEquals(clientSub9.getAllReceivedMessage().size(), 10);
		// assertEquals(clientSub9.getReceivedRegularMessageList().size(), 9);
		// assertEquals(clientSub9.getReceivedAggregatedMessageList().size(),
		// 1);
		//
		// assertTrue(clientSub10.isMessageReceived());
		// //assertEquals(clientSub10.getAllReceivedMessage().size(), 9);
		// assertEquals(clientSub10.getReceivedRegularMessageList().size(), 8);
		// assertEquals(clientSub10.getReceivedAggregatedMessageList().size(),
		// 1);
		//
		// assertTrue(clientSub11.isMessageReceived());
		// assertEquals(clientSub11.getAllReceivedMessage().size(), 8);
		// assertEquals(clientSub11.getReceivedRegularMessageList().size(), 7);
		// assertEquals(clientSub11.getReceivedAggregatedMessageList().size(),
		// 1);
		//
		//
		// assertTrue(clientSub12.isMessageReceived());
		// assertEquals(clientSub12.getAllReceivedMessage().size(), 7);
		// assertEquals(clientSub12.getReceivedRegularMessageList().size(), 6);
		// assertEquals(clientSub12.getReceivedAggregatedMessageList().size(),
		// 1);
		//
		// assertTrue(clientSub13.isMessageReceived());
		// assertEquals(clientSub13.getAllReceivedMessage().size(), 6);
		// assertEquals(clientSub13.getReceivedRegularMessageList().size(), 5);
		// assertEquals(clientSub13.getReceivedAggregatedMessageList().size(),
		// 1);
		//
		//
		// assertTrue(clientSub14.isMessageReceived());
		// assertEquals(clientSub14.getAllReceivedMessage().size(), 5);
		// assertEquals(clientSub14.getReceivedRegularMessageList().size(), 4);
		// assertEquals(clientSub14.getReceivedAggregatedMessageList().size(),
		// 1);
		//
		//
		// assertTrue(clientSub15.isMessageReceived());
		// assertEquals(clientSub15.getAllReceivedMessage().size(), 4);
		// assertEquals(clientSub15.getReceivedRegularMessageList().size(), 3);
		// assertEquals(clientSub15.getReceivedAggregatedMessageList().size(),
		// 1);
		//
		// assertTrue(clientSub16.isMessageReceived());
		// assertEquals(clientSub16.getAllReceivedMessage().size(),3);
		// assertEquals(clientSub16.getReceivedRegularMessageList().size(), 2);
		// assertEquals(clientSub16.getReceivedAggregatedMessageList().size(),
		// 1);

	}

	public void aggregatedMessageRecievedExperimentTopologyForNonOverlap()
			throws ClientException, ParseException, BrokerCoreException,
			InterruptedException {

		clientPub5 = new TestClient("ClientPub5");
		clientPub5.connect("rmi://localhost:1105/Broker5");
		clientPub6 = new TestClient("ClientPub6");
		clientPub6.connect("rmi://localhost:1106/Broker6");
		clientPub7 = new TestClient("ClientPub7");
		clientPub7.connect("rmi://localhost:1107/Broker7");
		clientPub8 = new TestClient("ClientPub8");
		clientPub8.connect("rmi://localhost:1108/Broker8");
		clientPub9 = new TestClient("ClientPub9");
		clientPub9.connect("rmi://localhost:1109/Broker9");
		clientPub10 = new TestClient("ClientPub10");
		clientPub10.connect("rmi://localhost:1110/Broker10");
		clientPub11 = new TestClient("ClientPub11");
		clientPub11.connect("rmi://localhost:1111/Broker11");
		clientPub12 = new TestClient("ClientPub12");
		clientPub12.connect("rmi://localhost:1112/Broker12");
		clientPub13 = new TestClient("ClientPub13");
		clientPub13.connect("rmi://localhost:1113/Broker13");

		clientSub8 = new TestClient("clientSub8");
		clientSub8.connect("rmi://localhost:1108/Broker8");
		clientSub9 = new TestClient("clientSub9");
		clientSub9.connect("rmi://localhost:1109/Broker9");
		clientSub10 = new TestClient("clientSub10");
		clientSub10.connect("rmi://localhost:1110/Broker10");
		clientSub11 = new TestClient("clientSub11");
		clientSub11.connect("rmi://localhost:1111/Broker11");
		clientSub12 = new TestClient("clientSub12");
		clientSub12.connect("rmi://localhost:1112/Broker12");
		clientSub13 = new TestClient("clientSub13");
		clientSub13.connect("rmi://localhost:1113/Broker13");
		clientSub14 = new TestClient("clientSub14");
		clientSub14.connect("rmi://localhost:1114/Broker14");
		clientSub15 = new TestClient("clientSub15");
		clientSub15.connect("rmi://localhost:1115/Broker15");
		clientSub16 = new TestClient("clientSub16");
		clientSub16.connect("rmi://localhost:1116/Broker16");

		Long l1 = 10L;
		clientPub5
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1105/Broker5");
		Thread.sleep(l1);
		clientPub6
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1106/Broker6");
		Thread.sleep(l1);
		clientPub7
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1107/Broker7");
		Thread.sleep(l1);
		clientPub8
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1108/Broker8");
		Thread.sleep(l1);
		clientPub9
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1109/Broker9");
		Thread.sleep(l1);
		clientPub10
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1110/Broker10");
		Thread.sleep(l1);
		clientPub11
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1111/Broker11");
		Thread.sleep(l1);
		clientPub12
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1112/Broker12");
		Thread.sleep(l1);
		clientPub13
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1113/Broker13");

		clientPub5
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK1],[value,>,50]"),
						"rmi://localhost:1105/Broker5");
		Thread.sleep(l1);
		clientPub6
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK1],[value,>,50]"),
						"rmi://localhost:1106/Broker6");
		Thread.sleep(l1);
		clientPub7
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK1],[value,>,50]"),
						"rmi://localhost:1107/Broker7");
		Thread.sleep(l1);
		clientPub8
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK1],[value,>,50]"),
						"rmi://localhost:1108/Broker8");
		Thread.sleep(l1);
		clientPub9
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK1],[value,>,50]"),
						"rmi://localhost:1109/Broker9");
		Thread.sleep(l1);
		clientPub10
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK1],[value,>,50]"),
						"rmi://localhost:1110/Broker10");
		Thread.sleep(l1);
		clientPub11
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK1],[value,>,50]"),
						"rmi://localhost:1111/Broker11");
		Thread.sleep(l1);
		clientPub12
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK1],[value,>,50]"),
						"rmi://localhost:1112/Broker12");
		Thread.sleep(l1);
		clientPub13
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK1],[value,>,50]"),
						"rmi://localhost:1113/Broker13");

		Thread.sleep(100L);
		Long l2 = 10L;
		clientSub8.subscribe("[class,eq,STOCK1],[value,>,60] ",
				"rmi://localhost:1108/Broker8");
		Thread.sleep(l2);
		clientSub9.subscribe("[class,eq,STOCK1],[value,>,70] ",
				"rmi://localhost:1109/Broker9");
		Thread.sleep(l2);
		clientSub10.subscribe("[class,eq,STOCK1],[value,>,80] ",
				"rmi://localhost:1110/Broker10");
		Thread.sleep(l2);
		clientSub11.subscribe("[class,eq,STOCK1],[value,>,90] ",
				"rmi://localhost:1111/Broker11");
		Thread.sleep(l2);
		clientSub12.subscribe("[class,eq,STOCK1],[value,>,100] ",
				"rmi://localhost:1112/Broker12");
		Thread.sleep(l2);
		clientSub13.subscribe("[class,eq,STOCK1],[value,>,110] ",
				"rmi://localhost:1113/Broker13");
		Thread.sleep(l2);
		clientSub14.subscribe("[class,eq,STOCK1],[value,>,120] ",
				"rmi://localhost:1114/Broker14");
		Thread.sleep(l2);
		clientSub15.subscribe("[class,eq,STOCK1],[value,>,130] ",
				"rmi://localhost:1115/Broker15");
		Thread.sleep(l2);
		clientSub16.subscribe("[class,eq,STOCK1],[value,>,140] ",
				"rmi://localhost:1116/Broker16");

		Thread.sleep(100L);
		Long l3 = 10L;
		clientSub8
				.subscribe(
						"[class,eq,STOCK],[value,>,60],[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2'] ",
						"rmi://localhost:1108/Broker8");
		Thread.sleep(l3);
		clientSub9
				.subscribe(
						"[class,eq,STOCK],[value,>,70] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1109/Broker9");
		Thread.sleep(l3);
		clientSub10
				.subscribe(
						"[class,eq,STOCK],[value,>,80] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1110/Broker10");
		Thread.sleep(l3);
		clientSub11
				.subscribe(
						"[class,eq,STOCK],[value,>,90] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1111/Broker11");
		Thread.sleep(l3);
		clientSub12
				.subscribe(
						"[class,eq,STOCK],[value,>,100] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1112/Broker12");
		Thread.sleep(l3);
		clientSub13
				.subscribe(
						"[class,eq,STOCK],[value,>,110],[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2'] ",
						"rmi://localhost:1113/Broker13");
		Thread.sleep(l3);
		clientSub14
				.subscribe(
						"[class,eq,STOCK],[value,>,120] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1114/Broker14");
		Thread.sleep(l3);
		clientSub15
				.subscribe(
						"[class,eq,STOCK],[value,>,130] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1115/Broker15");
		Thread.sleep(l3);
		clientSub16
				.subscribe(
						"[class,eq,STOCK],[value,>,140] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1116/Broker16");

		Thread.sleep(100L);
		Long l4 = 10L;
		clientPub5.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,80]"),
				"rmi://localhost:1105/Broker5");
		Thread.sleep(l4);
		clientPub6.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,90]"),
				"rmi://localhost:1106/Broker6");
		Thread.sleep(l4);
		clientPub7.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,100]"),
				"rmi://localhost:1107/Broker7");
		Thread.sleep(l4);
		clientPub8.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,110]"),
				"rmi://localhost:1108/Broker8");
		Thread.sleep(l4);
		clientPub9.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,120]"),
				"rmi://localhost:1109/Broker9");
		Thread.sleep(l4);
		clientPub10.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,130]"),
				"rmi://localhost:1110/Broker10");
		Thread.sleep(l4);
		clientPub11.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,140]"),
				"rmi://localhost:1111/Broker11");
		Thread.sleep(l4);
		clientPub12.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,150]"),
				"rmi://localhost:1112/Broker12");
		Thread.sleep(l4);
		clientPub13.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,1600]"),
				"rmi://localhost:1113/Broker13");
		Thread.sleep(l4);

		Thread.sleep(100L);
		clientPub5.publish(MessageFactory
				.createPublicationFromString("[class,STOCK1],[value,80]"),
				"rmi://localhost:1105/Broker5");
		Thread.sleep(l4);
		clientPub6.publish(MessageFactory
				.createPublicationFromString("[class,STOCK1],[value,90]"),
				"rmi://localhost:1106/Broker6");
		Thread.sleep(l4);
		clientPub7.publish(MessageFactory
				.createPublicationFromString("[class,STOCK1],[value,100]"),
				"rmi://localhost:1107/Broker7");
		Thread.sleep(l4);
		clientPub8.publish(MessageFactory
				.createPublicationFromString("[class,STOCK1],[value,110]"),
				"rmi://localhost:1108/Broker8");
		Thread.sleep(l4);
		clientPub9.publish(MessageFactory
				.createPublicationFromString("[class,STOCK1],[value,120]"),
				"rmi://localhost:1109/Broker9");
		Thread.sleep(l4);
		clientPub10.publish(MessageFactory
				.createPublicationFromString("[class,STOCK1],[value,130]"),
				"rmi://localhost:1110/Broker10");
		Thread.sleep(l4);
		clientPub11.publish(MessageFactory
				.createPublicationFromString("[class,STOCK1],[value,140]"),
				"rmi://localhost:1111/Broker11");
		Thread.sleep(l4);
		clientPub12.publish(MessageFactory
				.createPublicationFromString("[class,STOCK1],[value,150]"),
				"rmi://localhost:1112/Broker12");
		Thread.sleep(l4);
		clientPub13.publish(MessageFactory
				.createPublicationFromString("[class,STOCK1],[value,1600]"),
				"rmi://localhost:1113/Broker13");
		Thread.sleep(l4);
		Thread.sleep(600L);
		assertTrue(clientSub8.isMessageReceived());
		assertEquals(clientSub8.getAllReceivedMessage().size(), 9);
		assertEquals(clientSub8.getReceivedRegularMessageList().size(), 9);

		assertTrue(clientSub9.isMessageReceived());
		assertEquals(clientSub9.getAllReceivedMessage().size(), 9);
		assertEquals(clientSub9.getReceivedRegularMessageList().size(), 9);

		assertTrue(clientSub10.isMessageReceived());
		assertEquals(clientSub10.getAllReceivedMessage().size(), 8);
		assertEquals(clientSub10.getReceivedRegularMessageList().size(), 8);

		assertTrue(clientSub11.isMessageReceived());
		assertEquals(clientSub11.getAllReceivedMessage().size(), 7);
		assertEquals(clientSub11.getReceivedRegularMessageList().size(), 7);

		assertTrue(clientSub12.isMessageReceived());
		assertEquals(clientSub12.getAllReceivedMessage().size(), 6);
		assertEquals(clientSub12.getReceivedRegularMessageList().size(), 6);

		assertTrue(clientSub13.isMessageReceived());
		assertEquals(clientSub13.getAllReceivedMessage().size(), 5);
		assertEquals(clientSub13.getReceivedRegularMessageList().size(), 5);

		assertTrue(clientSub14.isMessageReceived());
		assertEquals(clientSub14.getAllReceivedMessage().size(), 4);
		assertEquals(clientSub14.getReceivedRegularMessageList().size(), 4);

		assertTrue(clientSub15.isMessageReceived());
		assertEquals(clientSub15.getAllReceivedMessage().size(), 3);
		assertEquals(clientSub15.getReceivedRegularMessageList().size(), 3);

		assertTrue(clientSub16.isMessageReceived());
		assertEquals(clientSub16.getAllReceivedMessage().size(), 2);
		assertEquals(clientSub16.getReceivedRegularMessageList().size(), 2);

		Thread.sleep(3000L);
		assertEquals(9, clientSub8.getReceivedRegularMessageList().size());
		assertTrue(clientSub8.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub8.getReceivedAggregatedMessageList().size() > 0);

		assertTrue(clientSub9.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub9.getReceivedAggregatedMessageList().size() > 0);

		assertTrue(clientSub10.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub10.getReceivedAggregatedMessageList().size() > 0);

		assertTrue(clientSub11.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub11.getReceivedAggregatedMessageList().size() > 0);

		assertTrue(clientSub12.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub12.getReceivedAggregatedMessageList().size() > 0);

		assertTrue(clientSub13.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub13.getReceivedAggregatedMessageList().size() > 0);

		Thread.sleep(10000L);

		assertTrue(clientSub14.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub14.getReceivedAggregatedMessageList().size() > 0);

		assertTrue(clientSub15.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub15.getReceivedAggregatedMessageList().size() > 0);

		assertTrue(clientSub16.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub16.getReceivedAggregatedMessageList().size() > 0);

	}

	public void experimentalSetup100PublicationWithoutOverlap() throws ClientException, ParseException,
	BrokerCoreException, InterruptedException{


		clientPub5 = new TestClient("ClientPub5");
		clientPub5.connect("rmi://localhost:1105/Broker5");
		clientPub6 = new TestClient("ClientPub6");
		clientPub6.connect("rmi://localhost:1106/Broker6");
		clientPub7 = new TestClient("ClientPub7");
		clientPub7.connect("rmi://localhost:1107/Broker7");
		clientPub8 = new TestClient("ClientPub8");
		clientPub8.connect("rmi://localhost:1108/Broker8");
		clientPub9 = new TestClient("ClientPub9");
		clientPub9.connect("rmi://localhost:1109/Broker9");
		clientPub10 = new TestClient("ClientPub10");
		clientPub10.connect("rmi://localhost:1110/Broker10");
		clientPub11 = new TestClient("ClientPub11");
		clientPub11.connect("rmi://localhost:1111/Broker11");
		clientPub12 = new TestClient("ClientPub12");
		clientPub12.connect("rmi://localhost:1112/Broker12");
		clientPub13 = new TestClient("ClientPub13");
		clientPub13.connect("rmi://localhost:1113/Broker13");

		clientSub8 = new TestClient("clientSub8");
		clientSub8.connect("rmi://localhost:1108/Broker8");
		clientSub9 = new TestClient("clientSub9");
		clientSub9.connect("rmi://localhost:1109/Broker9");
		clientSub10 = new TestClient("clientSub10");
		clientSub10.connect("rmi://localhost:1110/Broker10");
		clientSub11 = new TestClient("clientSub11");
		clientSub11.connect("rmi://localhost:1111/Broker11");
		clientSub12 = new TestClient("clientSub12");
		clientSub12.connect("rmi://localhost:1112/Broker12");
		clientSub13 = new TestClient("clientSub13");
		clientSub13.connect("rmi://localhost:1113/Broker13");
		clientSub14 = new TestClient("clientSub14");
		clientSub14.connect("rmi://localhost:1114/Broker14");
		clientSub15 = new TestClient("clientSub15");
		clientSub15.connect("rmi://localhost:1115/Broker15");
		clientSub16 = new TestClient("clientSub16");
		clientSub16.connect("rmi://localhost:1116/Broker16");

		Long l1 = 10L;
		clientPub5
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1105/Broker5");
		Thread.sleep(l1);
		clientPub6
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1106/Broker6");
		Thread.sleep(l1);
		clientPub7
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1107/Broker7");
		Thread.sleep(l1);
		clientPub8
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1108/Broker8");
		Thread.sleep(l1);
		clientPub9
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1109/Broker9");
		Thread.sleep(l1);
		clientPub10
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1110/Broker10");
		Thread.sleep(l1);
		clientPub11
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1111/Broker11");
		Thread.sleep(l1);
		clientPub12
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1112/Broker12");
		Thread.sleep(l1);
		clientPub13
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1113/Broker13");

		clientPub5
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK1],[value,>,50]"),
						"rmi://localhost:1105/Broker5");
		Thread.sleep(l1);
		clientPub6
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK1],[value,>,50]"),
						"rmi://localhost:1106/Broker6");
		Thread.sleep(l1);
		clientPub7
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK1],[value,>,50]"),
						"rmi://localhost:1107/Broker7");
		Thread.sleep(l1);
		clientPub8
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK1],[value,>,50]"),
						"rmi://localhost:1108/Broker8");
		Thread.sleep(l1);
		clientPub9
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK1],[value,>,50]"),
						"rmi://localhost:1109/Broker9");
		Thread.sleep(l1);
		clientPub10
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK1],[value,>,50]"),
						"rmi://localhost:1110/Broker10");
		Thread.sleep(l1);
		clientPub11
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK1],[value,>,50]"),
						"rmi://localhost:1111/Broker11");
		Thread.sleep(l1);
		clientPub12
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK1],[value,>,50]"),
						"rmi://localhost:1112/Broker12");
		Thread.sleep(l1);
		clientPub13
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK1],[value,>,50]"),
						"rmi://localhost:1113/Broker13");

		Thread.sleep(100L);
		Long l2 = 10L;
		clientSub8.subscribe("[class,eq,STOCK1],[value,>,60] ",
				"rmi://localhost:1108/Broker8");
		Thread.sleep(l2);
		clientSub9.subscribe("[class,eq,STOCK1],[value,>,70] ",
				"rmi://localhost:1109/Broker9");
		Thread.sleep(l2);
		clientSub10.subscribe("[class,eq,STOCK1],[value,>,80] ",
				"rmi://localhost:1110/Broker10");
		Thread.sleep(l2);
		clientSub11.subscribe("[class,eq,STOCK1],[value,>,90] ",
				"rmi://localhost:1111/Broker11");
		Thread.sleep(l2);
		clientSub12.subscribe("[class,eq,STOCK1],[value,>,100] ",
				"rmi://localhost:1112/Broker12");
		Thread.sleep(l2);
		clientSub13.subscribe("[class,eq,STOCK1],[value,>,110] ",
				"rmi://localhost:1113/Broker13");
		Thread.sleep(l2);
		clientSub14.subscribe("[class,eq,STOCK1],[value,>,120] ",
				"rmi://localhost:1114/Broker14");
		Thread.sleep(l2);
		clientSub15.subscribe("[class,eq,STOCK1],[value,>,130] ",
				"rmi://localhost:1115/Broker15");
		Thread.sleep(l2);
		clientSub16.subscribe("[class,eq,STOCK1],[value,>,140] ",
				"rmi://localhost:1116/Broker16");

		Thread.sleep(100L);
		Long l3 = 10L;
		clientSub8
				.subscribe(
						"[class,eq,STOCK],[value,>,60],[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2'] ",
						"rmi://localhost:1108/Broker8");
		Thread.sleep(l3);
		clientSub9
				.subscribe(
						"[class,eq,STOCK],[value,>,70] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1109/Broker9");
		Thread.sleep(l3);
		clientSub10
				.subscribe(
						"[class,eq,STOCK],[value,>,80] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1110/Broker10");
		Thread.sleep(l3);
		clientSub11
				.subscribe(
						"[class,eq,STOCK],[value,>,90] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1111/Broker11");
		Thread.sleep(l3);
		clientSub12
				.subscribe(
						"[class,eq,STOCK],[value,>,100] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1112/Broker12");
		Thread.sleep(l3);
		clientSub13
				.subscribe(
						"[class,eq,STOCK],[value,>,110],[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2'] ",
						"rmi://localhost:1113/Broker13");
		Thread.sleep(l3);
		clientSub14
				.subscribe(
						"[class,eq,STOCK],[value,>,120] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1114/Broker14");
		Thread.sleep(l3);
		clientSub15
				.subscribe(
						"[class,eq,STOCK],[value,>,130] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1115/Broker15");
		Thread.sleep(l3);
		clientSub16
				.subscribe(
						"[class,eq,STOCK],[value,>,140] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1116/Broker16");

		Thread.sleep(100L);
		Long l4 = 10L;
		clientPub5.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,80]"),
				"rmi://localhost:1105/Broker5");
		Thread.sleep(l4);
		clientPub6.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,90]"),
				"rmi://localhost:1106/Broker6");
		Thread.sleep(l4);
		clientPub7.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,100]"),
				"rmi://localhost:1107/Broker7");
		Thread.sleep(l4);
		clientPub8.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,110]"),
				"rmi://localhost:1108/Broker8");
		Thread.sleep(l4);
		clientPub9.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,120]"),
				"rmi://localhost:1109/Broker9");
		Thread.sleep(l4);
		clientPub10.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,130]"),
				"rmi://localhost:1110/Broker10");
		Thread.sleep(l4);
		clientPub11.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,140]"),
				"rmi://localhost:1111/Broker11");
		Thread.sleep(l4);
		clientPub12.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,150]"),
				"rmi://localhost:1112/Broker12");
		Thread.sleep(l4);
		clientPub13.publish(MessageFactory
				.createPublicationFromString("[class,STOCK],[value,1600]"),
				"rmi://localhost:1113/Broker13");
		Thread.sleep(l4);

		Thread.sleep(100L);
		int TOTOAL_ITR=100;
		int itr =0;
		while(itr++ < TOTOAL_ITR) {
		clientPub5.publish(MessageFactory
				.createPublicationFromString("[class,STOCK1],[value,80]"),
				"rmi://localhost:1105/Broker5");
		Thread.sleep(l4);
		clientPub6.publish(MessageFactory
				.createPublicationFromString("[class,STOCK1],[value,90]"),
				"rmi://localhost:1106/Broker6");
		Thread.sleep(l4);
		clientPub7.publish(MessageFactory
				.createPublicationFromString("[class,STOCK1],[value,100]"),
				"rmi://localhost:1107/Broker7");
		Thread.sleep(l4);
		clientPub8.publish(MessageFactory
				.createPublicationFromString("[class,STOCK1],[value,110]"),
				"rmi://localhost:1108/Broker8");
		Thread.sleep(l4);
		clientPub9.publish(MessageFactory
				.createPublicationFromString("[class,STOCK1],[value,120]"),
				"rmi://localhost:1109/Broker9");
		Thread.sleep(l4);
		clientPub10.publish(MessageFactory
				.createPublicationFromString("[class,STOCK1],[value,130]"),
				"rmi://localhost:1110/Broker10");
		Thread.sleep(l4);
		clientPub11.publish(MessageFactory
				.createPublicationFromString("[class,STOCK1],[value,140]"),
				"rmi://localhost:1111/Broker11");
		Thread.sleep(l4);
		clientPub12.publish(MessageFactory
				.createPublicationFromString("[class,STOCK1],[value,150]"),
				"rmi://localhost:1112/Broker12");
		Thread.sleep(l4);
		clientPub13.publish(MessageFactory
				.createPublicationFromString("[class,STOCK1],[value,1600]"),
				"rmi://localhost:1113/Broker13");
		Thread.sleep(l4);
		}
		Thread.sleep(600L*TOTOAL_ITR);
		
		


		assertEquals(clientSub8.getReceivedRegularMessageList().size(), 9*TOTOAL_ITR);
		assertEquals(clientSub9.getReceivedRegularMessageList().size(), 9*TOTOAL_ITR);
		assertEquals(clientSub10.getReceivedRegularMessageList().size(), 8*TOTOAL_ITR);
		assertEquals(clientSub11.getReceivedRegularMessageList().size(), 7*TOTOAL_ITR);
		assertEquals(clientSub12.getReceivedRegularMessageList().size(), 6*TOTOAL_ITR);
		assertEquals(clientSub13.getReceivedRegularMessageList().size(), 5*TOTOAL_ITR);
		assertEquals(clientSub14.getReceivedRegularMessageList().size(), 4*TOTOAL_ITR);
		assertEquals(clientSub15.getReceivedRegularMessageList().size(), 3*TOTOAL_ITR);
		assertEquals(clientSub16.getReceivedRegularMessageList().size(), 2*TOTOAL_ITR);
		
		assertTrue(clientSub8.isMessageReceived());
	
	

//		assertTrue(clientSub9.isMessageReceived());
//		assertEquals(clientSub9.getAllReceivedMessage().size(), 9*TOTOAL_ITR);
//
//		assertTrue(clientSub10.isMessageReceived());
//		assertEquals(clientSub10.getAllReceivedMessage().size(), 8*TOTOAL_ITR);
//
//		assertTrue(clientSub11.isMessageReceived());
//		assertEquals(clientSub11.getAllReceivedMessage().size(), 7*TOTOAL_ITR);
//
//		assertTrue(clientSub12.isMessageReceived());
//		assertEquals(clientSub12.getAllReceivedMessage().size(), 6*TOTOAL_ITR);
//
//		assertTrue(clientSub13.isMessageReceived());
//		assertEquals(clientSub13.getAllReceivedMessage().size(), 5*TOTOAL_ITR);
//
//		assertTrue(clientSub14.isMessageReceived());
//		assertEquals(clientSub14.getAllReceivedMessage().size(), 4*TOTOAL_ITR);
//
//		assertTrue(clientSub15.isMessageReceived());
//		assertEquals(clientSub15.getAllReceivedMessage().size(), 3*TOTOAL_ITR);
//
//		assertTrue(clientSub16.isMessageReceived());
//		assertEquals(clientSub16.getAllReceivedMessage().size(), 2*TOTOAL_ITR);
//		
//		assertEquals(clientSub8.getAllReceivedMessage().size(), 9*TOTOAL_ITR);
		
		Thread.sleep(3000L);
		//assertEquals(9, clientSub8.getReceivedRegularMessageList().size());
		assertTrue(clientSub8.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub8.getReceivedAggregatedMessageList().size() > 0);

		assertTrue(clientSub9.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub9.getReceivedAggregatedMessageList().size() > 0);

		assertTrue(clientSub10.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub10.getReceivedAggregatedMessageList().size() > 0);

		assertTrue(clientSub11.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub11.getReceivedAggregatedMessageList().size() > 0);

		assertTrue(clientSub12.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub12.getReceivedAggregatedMessageList().size() > 0);

		assertTrue(clientSub13.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub13.getReceivedAggregatedMessageList().size() > 0);

		Thread.sleep(10000L);

		assertTrue(clientSub14.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub14.getReceivedAggregatedMessageList().size() > 0);

		assertTrue(clientSub15.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub15.getReceivedAggregatedMessageList().size() > 0);

		assertTrue(clientSub16.getReceivedAggregatedMessageList().size() < 3);
		assertTrue(clientSub16.getReceivedAggregatedMessageList().size() > 0);

	
	}
	public void experimentalSetup100Publication() throws ClientException, ParseException,
			BrokerCoreException, InterruptedException {

		clientPub5 = new TestClient("ClientPub5");
		clientPub5.connect("rmi://localhost:1105/Broker5");
		clientPub6 = new TestClient("ClientPub6");
		clientPub6.connect("rmi://localhost:1106/Broker6");
		clientPub7 = new TestClient("ClientPub7");
		clientPub7.connect("rmi://localhost:1107/Broker7");
		clientPub8 = new TestClient("ClientPub8");
		clientPub8.connect("rmi://localhost:1108/Broker8");
		clientPub9 = new TestClient("ClientPub9");
		clientPub9.connect("rmi://localhost:1109/Broker9");
		clientPub10 = new TestClient("ClientPub10");
		clientPub10.connect("rmi://localhost:1110/Broker10");
		clientPub11 = new TestClient("ClientPub11");
		clientPub11.connect("rmi://localhost:1111/Broker11");
		clientPub12 = new TestClient("ClientPub12");
		clientPub12.connect("rmi://localhost:1112/Broker12");
		clientPub13 = new TestClient("ClientPub13");
		clientPub13.connect("rmi://localhost:1113/Broker13");

		clientSub8 = new TestClient("clientSub8");
		clientSub8.connect("rmi://localhost:1108/Broker8");
		clientSub9 = new TestClient("clientSub9");
		clientSub9.connect("rmi://localhost:1109/Broker9");
		clientSub10 = new TestClient("clientSub10");
		clientSub10.connect("rmi://localhost:1110/Broker10");
		clientSub11 = new TestClient("clientSub11");
		clientSub11.connect("rmi://localhost:1111/Broker11");
		clientSub12 = new TestClient("clientSub12");
		clientSub12.connect("rmi://localhost:1112/Broker12");
		clientSub13 = new TestClient("clientSub13");
		clientSub13.connect("rmi://localhost:1113/Broker13");
		clientSub14 = new TestClient("clientSub14");
		clientSub14.connect("rmi://localhost:1114/Broker14");
		clientSub15 = new TestClient("clientSub15");
		clientSub15.connect("rmi://localhost:1115/Broker15");
		clientSub16 = new TestClient("clientSub16");
		clientSub16.connect("rmi://localhost:1116/Broker16");

		Long l1 = 10L;
		clientPub5
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1105/Broker5");
		Thread.sleep(l1);
		clientPub6
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1106/Broker6");
		Thread.sleep(l1);
		clientPub7
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1107/Broker7");
		Thread.sleep(l1);
		clientPub8
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1108/Broker8");
		Thread.sleep(l1);
		clientPub9
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1109/Broker9");
		Thread.sleep(l1);
		clientPub10
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1110/Broker10");
		Thread.sleep(l1);
		clientPub11
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1111/Broker11");
		Thread.sleep(l1);
		clientPub12
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1112/Broker12");
		Thread.sleep(l1);
		clientPub13
				.advertise(
						MessageFactory
								.createAdvertisementFromString("[class,eq,STOCK],[value,>,50]"),
						"rmi://localhost:1113/Broker13");

		Thread.sleep(100L);
		Long l2 = 10L;
		clientSub8.subscribe("[class,eq,STOCK],[value,>,60] ",
				"rmi://localhost:1108/Broker8");
		Thread.sleep(l2);
		clientSub9.subscribe("[class,eq,STOCK],[value,>,70] ",
				"rmi://localhost:1109/Broker9");
		Thread.sleep(l2);
		clientSub10.subscribe("[class,eq,STOCK],[value,>,80] ",
				"rmi://localhost:1110/Broker10");
		Thread.sleep(l2);
		clientSub11.subscribe("[class,eq,STOCK],[value,>,90] ",
				"rmi://localhost:1111/Broker11");
		Thread.sleep(l2);
		clientSub12.subscribe("[class,eq,STOCK],[value,>,100] ",
				"rmi://localhost:1112/Broker12");
		Thread.sleep(l2);
		clientSub13.subscribe("[class,eq,STOCK],[value,>,110] ",
				"rmi://localhost:1113/Broker13");
		Thread.sleep(l2);
		clientSub14.subscribe("[class,eq,STOCK],[value,>,120] ",
				"rmi://localhost:1114/Broker14");
		Thread.sleep(l2);
		clientSub15.subscribe("[class,eq,STOCK],[value,>,130] ",
				"rmi://localhost:1115/Broker15");
		Thread.sleep(l2);
		clientSub16.subscribe("[class,eq,STOCK],[value,>,140] ",
				"rmi://localhost:1116/Broker16");

		Thread.sleep(100L);
		Long l3 = 10L;
		clientSub8
				.subscribe(
						"[class,eq,STOCK],[value,>,60],[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2'] ",
						"rmi://localhost:1108/Broker8");
		Thread.sleep(l3);
		clientSub9
				.subscribe(
						"[class,eq,STOCK],[value,>,70] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1109/Broker9");
		Thread.sleep(l3);
		clientSub10
				.subscribe(
						"[class,eq,STOCK],[value,>,80] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1110/Broker10");
		Thread.sleep(l3);
		clientSub11
				.subscribe(
						"[class,eq,STOCK],[value,>,90] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1111/Broker11");
		Thread.sleep(l3);
		clientSub12
				.subscribe(
						"[class,eq,STOCK],[value,>,100] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1112/Broker12");
		Thread.sleep(l3);
		clientSub13
				.subscribe(
						"[class,eq,STOCK],[value,>,110],[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2'] ",
						"rmi://localhost:1113/Broker13");
		Thread.sleep(l3);
		clientSub14
				.subscribe(
						"[class,eq,STOCK],[value,>,120] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1114/Broker14");
		Thread.sleep(l3);
		clientSub15
				.subscribe(
						"[class,eq,STOCK],[value,>,130] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1115/Broker15");
		Thread.sleep(l3);
		clientSub16
				.subscribe(
						"[class,eq,STOCK],[value,>,140] ,[AGR,eq,'range'],[PAR,eq,value],[PRD,eq,'2'],[NTF,eq,'2']",
						"rmi://localhost:1116/Broker16");

		Thread.sleep(100L);
		int TOTAL_ITR=100;
		int times = 0;
		while (times++ < TOTAL_ITR) {
			Long l4 = 10L;
			clientPub5.publish(MessageFactory
					.createPublicationFromString("[class,STOCK],[value,80]"),
					"rmi://localhost:1105/Broker5");
			Thread.sleep(l4);
			clientPub6.publish(MessageFactory
					.createPublicationFromString("[class,STOCK],[value,90]"),
					"rmi://localhost:1106/Broker6");
			Thread.sleep(l4);
			clientPub7.publish(MessageFactory
					.createPublicationFromString("[class,STOCK],[value,100]"),
					"rmi://localhost:1107/Broker7");
			Thread.sleep(l4);
			clientPub8.publish(MessageFactory
					.createPublicationFromString("[class,STOCK],[value,110]"),
					"rmi://localhost:1108/Broker8");
			Thread.sleep(l4);
			clientPub9.publish(MessageFactory
					.createPublicationFromString("[class,STOCK],[value,120]"),
					"rmi://localhost:1109/Broker9");
			Thread.sleep(l4);
			clientPub10.publish(MessageFactory
					.createPublicationFromString("[class,STOCK],[value,130]"),
					"rmi://localhost:1110/Broker10");
			Thread.sleep(l4);
			clientPub11.publish(MessageFactory
					.createPublicationFromString("[class,STOCK],[value,140]"),
					"rmi://localhost:1111/Broker11");
			Thread.sleep(l4);
			clientPub12.publish(MessageFactory
					.createPublicationFromString("[class,STOCK],[value,150]"),
					"rmi://localhost:1112/Broker12");
			Thread.sleep(l4);
			clientPub13.publish(MessageFactory
					.createPublicationFromString("[class,STOCK],[value,1600]"),
					"rmi://localhost:1113/Broker13");
			Thread.sleep(l4);
			
		}

		Thread.sleep(600L*TOTAL_ITR);
		
		assertTrue(clientSub8.isMessageReceived());

		assertEquals(clientSub8.getReceivedRegularMessageList().size(), 9*TOTAL_ITR);

		assertTrue(clientSub9.isMessageReceived());

		assertEquals(clientSub9.getReceivedRegularMessageList().size(), 9*TOTAL_ITR);

		assertTrue(clientSub10.isMessageReceived());

		assertEquals(clientSub10.getReceivedRegularMessageList().size(), 8*TOTAL_ITR);

		assertTrue(clientSub11.isMessageReceived());

		assertEquals(clientSub11.getReceivedRegularMessageList().size(), 7*TOTAL_ITR);

		assertTrue(clientSub12.isMessageReceived());

		assertEquals(clientSub12.getReceivedRegularMessageList().size(), 6*TOTAL_ITR);

		assertTrue(clientSub13.isMessageReceived());

		assertEquals(clientSub13.getReceivedRegularMessageList().size(), 5*TOTAL_ITR);

		assertTrue(clientSub14.isMessageReceived());

		assertEquals(clientSub14.getReceivedRegularMessageList().size(), 4*TOTAL_ITR);

		assertTrue(clientSub15.isMessageReceived());

		assertEquals(clientSub15.getReceivedRegularMessageList().size(), 3*TOTAL_ITR);

		assertTrue(clientSub16.isMessageReceived());

		assertEquals(clientSub16.getReceivedRegularMessageList().size(), 2*TOTAL_ITR);

		



	

	

		assertEquals(clientSub8.getAllReceivedMessage().size(), 9*TOTAL_ITR);
		assertEquals(clientSub9.getAllReceivedMessage().size(), 9*TOTAL_ITR);
		assertEquals(clientSub10.getAllReceivedMessage().size(), 8*TOTAL_ITR);
		assertEquals(clientSub11.getAllReceivedMessage().size(), 7*TOTAL_ITR);
		assertEquals(clientSub12.getAllReceivedMessage().size(), 6*TOTAL_ITR);

		assertEquals(clientSub12.getAllReceivedMessage().size(), 6*TOTAL_ITR);

		assertEquals(clientSub13.getAllReceivedMessage().size(), 5*TOTAL_ITR);

		assertEquals(clientSub14.getAllReceivedMessage().size(), 4*TOTAL_ITR);

		assertEquals(clientSub15.getAllReceivedMessage().size(), 3*TOTAL_ITR);

		assertEquals(clientSub16.getAllReceivedMessage().size(), 2*TOTAL_ITR);
		
		Thread.sleep(3000L);
//		assertEquals(9, clientSub8.getReceivedRegularMessageList().size());
//		assertTrue(clientSub8.getReceivedAggregatedMessageList().size() < 3);
//		assertTrue(clientSub8.getReceivedAggregatedMessageList().size() > 0);
//
//		assertTrue(clientSub9.getReceivedAggregatedMessageList().size() < 3);
//		assertTrue(clientSub9.getReceivedAggregatedMessageList().size() > 0);
//
//		assertTrue(clientSub10.getReceivedAggregatedMessageList().size() < 3);
//		assertTrue(clientSub10.getReceivedAggregatedMessageList().size() > 0);
//
//		assertTrue(clientSub11.getReceivedAggregatedMessageList().size() < 3);
//		assertTrue(clientSub11.getReceivedAggregatedMessageList().size() > 0);
//
//		assertTrue(clientSub12.getReceivedAggregatedMessageList().size() < 3);
//		assertTrue(clientSub12.getReceivedAggregatedMessageList().size() > 0);
//
//		assertTrue(clientSub13.getReceivedAggregatedMessageList().size() < 3);
//		assertTrue(clientSub13.getReceivedAggregatedMessageList().size() > 0);
//
//		Thread.sleep(10000L);
//
//		assertTrue(clientSub14.getReceivedAggregatedMessageList().size() < 3);
//		assertTrue(clientSub14.getReceivedAggregatedMessageList().size() > 0);
//
//		assertTrue(clientSub15.getReceivedAggregatedMessageList().size() < 3);
//		assertTrue(clientSub15.getReceivedAggregatedMessageList().size() > 0);
//
//		assertTrue(clientSub16.getReceivedAggregatedMessageList().size() < 3);
//		assertTrue(clientSub16.getReceivedAggregatedMessageList().size() > 0);

		// assertTrue(clientSub9.isMessageReceived());
		// assertEquals(clientSub9.getAllReceivedMessage().size(), 10);
		// assertEquals(clientSub9.getReceivedRegularMessageList().size(), 9);
		// assertEquals(clientSub9.getReceivedAggregatedMessageList().size(),
		// 1);
		//
		// assertTrue(clientSub10.isMessageReceived());
		// //assertEquals(clientSub10.getAllReceivedMessage().size(), 9);
		// assertEquals(clientSub10.getReceivedRegularMessageList().size(), 8);
		// assertEquals(clientSub10.getReceivedAggregatedMessageList().size(),
		// 1);
		//
		// assertTrue(clientSub11.isMessageReceived());
		// assertEquals(clientSub11.getAllReceivedMessage().size(), 8);
		// assertEquals(clientSub11.getReceivedRegularMessageList().size(), 7);
		// assertEquals(clientSub11.getReceivedAggregatedMessageList().size(),
		// 1);
		//
		//
		// assertTrue(clientSub12.isMessageReceived());
		// assertEquals(clientSub12.getAllReceivedMessage().size(), 7);
		// assertEquals(clientSub12.getReceivedRegularMessageList().size(), 6);
		// assertEquals(clientSub12.getReceivedAggregatedMessageList().size(),
		// 1);
		//
		// assertTrue(clientSub13.isMessageReceived());
		// assertEquals(clientSub13.getAllReceivedMessage().size(), 6);
		// assertEquals(clientSub13.getReceivedRegularMessageList().size(), 5);
		// assertEquals(clientSub13.getReceivedAggregatedMessageList().size(),
		// 1);
		//
		//
		// assertTrue(clientSub14.isMessageReceived());
		// assertEquals(clientSub14.getAllReceivedMessage().size(), 5);
		// assertEquals(clientSub14.getReceivedRegularMessageList().size(), 4);
		// assertEquals(clientSub14.getReceivedAggregatedMessageList().size(),
		// 1);
		//
		//
		// assertTrue(clientSub15.isMessageReceived());
		// assertEquals(clientSub15.getAllReceivedMessage().size(), 4);
		// assertEquals(clientSub15.getReceivedRegularMessageList().size(), 3);
		// assertEquals(clientSub15.getReceivedAggregatedMessageList().size(),
		// 1);
		//
		// assertTrue(clientSub16.isMessageReceived());
		// assertEquals(clientSub16.getAllReceivedMessage().size(),3);
		// assertEquals(clientSub16.getReceivedRegularMessageList().size(), 2);
		// assertEquals(clientSub16.getReceivedAggregatedMessageList().size(),
		// 1);

	}
}
