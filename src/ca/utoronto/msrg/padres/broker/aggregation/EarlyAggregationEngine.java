package ca.utoronto.msrg.padres.broker.aggregation;

import ca.utoronto.msrg.padres.broker.aggregation.aggregator.Aggregator;
import ca.utoronto.msrg.padres.broker.aggregation.aggregator.SimpleAggregatorFactory;
import ca.utoronto.msrg.padres.broker.aggregation.collector.CollectorAtBroker;
import ca.utoronto.msrg.padres.broker.aggregation.collector.SimpleCollectorFactory;
import ca.utoronto.msrg.padres.broker.aggregation.utility.AggregationInfo;
import ca.utoronto.msrg.padres.broker.aggregation.utility.Stats;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;

public class EarlyAggregationEngine extends CollectionBasedAggregationEngine<Aggregator,CollectorAtBroker> {

	public EarlyAggregationEngine(BrokerCore brockerCore, AggregationInfo aggregationInfo) {
		super(brockerCore, aggregationInfo,new SimpleAggregatorFactory(), new SimpleCollectorFactory());
	}

	@Override
	public void aggregatePublicationMessage() {
		long startTime = System.currentTimeMillis();
		super.aggregatePublicationMessage();
		aggregate();
		Stats.getInstance("test").addValue("TIME.RECEIVE", System.currentTimeMillis() - startTime);
		
	}

	@Override
	public void getNotificationFromAggregator(Aggregator aggregator) {
		long startTime = System.currentTimeMillis();

		aggregator.publishResult(false);

		Stats.getInstance().addValue("TIME.NOTIFY", System.currentTimeMillis() - startTime);
	}

}
