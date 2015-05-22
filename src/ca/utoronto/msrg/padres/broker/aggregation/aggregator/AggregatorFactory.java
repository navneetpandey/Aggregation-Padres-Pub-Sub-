package ca.utoronto.msrg.padres.broker.aggregation.aggregator;

import ca.utoronto.msrg.padres.broker.aggregation.Notifier;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationType;
import ca.utoronto.msrg.padres.common.message.MessageDestination;

public interface AggregatorFactory<T extends AbstractAggregator> {

	T createAggregator(Notifier notifier,
			AggregationID aggregationID, AggregationType aggType,
			long startTimeOfTheCurrentWindow, MessageDestination sourceLink);
	
	

}
