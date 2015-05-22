package ca.utoronto.msrg.padres.broker.aggregation;

import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationType;
import ca.utoronto.msrg.padres.broker.aggregation.aggregator.Aggregator;
import ca.utoronto.msrg.padres.broker.aggregation.aggregator.SimpleAggregatorFactory;
import ca.utoronto.msrg.padres.broker.aggregation.collector.DummyCollector;
import ca.utoronto.msrg.padres.broker.aggregation.message.AggregatedPublication;
import ca.utoronto.msrg.padres.broker.aggregation.utility.AggregationTimerService;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.Subscription;
import ca.utoronto.msrg.padres.common.message.SubscriptionMessage;

public class ClientAggregator {

	private Notifier notifier;
	Aggregator aggregator;
	DummyCollector collector;
	long DEFAULT_WAITING = 2000;

	AbstractAggregationEngine abstractAggregationEngine;

	AggregationTimerService aggregationTimerService;

	public ClientAggregator(SubscriptionMessage subMsg, long startOfTheAggregationWindow, BrokerCore brokerCore,
			AggregationTimerService aggregationTimerService) {

		Subscription sub = subMsg.getSubscription();

		this.aggregationTimerService = aggregationTimerService;

		notifier = new Notifier(brokerCore);

		SimpleAggregatorFactory factory = new SimpleAggregatorFactory();

		aggregator = factory.createAggregator(notifier, subMsg.getSubscription().getAggregationID(), AggregationType.TIMEBASED,
				startOfTheAggregationWindow, subMsg.getNextHopID());

		abstractAggregationEngine = (AbstractAggregationEngine) brokerCore.getRouter().getPostProcessor().getAggregationEngine();

		collector = new DummyCollector(notifier, sub.getAggregationID(), DEFAULT_WAITING, aggregationTimerService, abstractAggregationEngine);
		collector.setDestination(subMsg.getLastHopID());

		aggregator.subscribeCollector(collector, subMsg);

	}

	public Aggregator getAggregator() {
		return aggregator;
	}

	public DummyCollector getCollector() {
		return collector;
	}

	public void recievedPublication(PublicationMessage pubMsg) {
		if (!(pubMsg.getPublication() instanceof AggregatedPublication)) {

			System.out.println("[CLAGGR" + notifier.getBrokerCore().getBrokerID() + "]recievedPublication=>" + "recieved raw publication"
					+ pubMsg.getPublication().toString());
			aggregator.addPublication(pubMsg.getPublication());
		} else
			System.out.println("[CLAGGR" + notifier.getBrokerCore().getBrokerID() + "]recievedPublication=>" + "recieved agg publication"
					+ pubMsg.getPublication().toString());
	}
}
