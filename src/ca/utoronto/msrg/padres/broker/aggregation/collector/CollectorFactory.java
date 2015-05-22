package ca.utoronto.msrg.padres.broker.aggregation.collector;

import ca.utoronto.msrg.padres.broker.aggregation.AbstractAggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.Notifier;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;

public interface CollectorFactory<T extends AbstractCollector> {

	public T createCollector(Notifier notifier,
			AggregationID aggregationID, long waitingTime, AbstractAggregationEngine abstractAggregationEngine); //String fromBroker, ???
	
	/*{

			return new CollectorAtBroker(notifier, aggregationID, waitingTime,abstractAggregationEngine);
	}*/
}
