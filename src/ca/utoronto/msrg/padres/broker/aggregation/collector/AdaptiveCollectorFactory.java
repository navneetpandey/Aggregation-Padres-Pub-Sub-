package ca.utoronto.msrg.padres.broker.aggregation.collector;

import ca.utoronto.msrg.padres.broker.aggregation.AbstractAggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.Notifier;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;

public class AdaptiveCollectorFactory implements CollectorFactory<AdaptiveCollectorAtBroker> {


	public AdaptiveCollectorAtBroker createCollector(Notifier notifier, AggregationID aggregationID, long waitingTime, 
			AbstractAggregationEngine abstractAggregationEngine) {
		return new AdaptiveCollectorAtBroker(notifier, aggregationID, waitingTime,abstractAggregationEngine);
	}

}
