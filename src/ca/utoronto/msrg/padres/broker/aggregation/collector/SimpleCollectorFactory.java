package ca.utoronto.msrg.padres.broker.aggregation.collector;

import ca.utoronto.msrg.padres.broker.aggregation.AbstractAggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.Notifier;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;

public class SimpleCollectorFactory implements CollectorFactory<CollectorAtBroker> {

	@Override
	public CollectorAtBroker createCollector(Notifier notifier, AggregationID aggregationID, long waitingTime, 
			AbstractAggregationEngine abstractAggregationEngine) {
		return new CollectorAtBroker(notifier, aggregationID, waitingTime,abstractAggregationEngine);
	}
	
}
