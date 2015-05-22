package ca.utoronto.msrg.padres.broker.aggregation.aggregator;

import ca.utoronto.msrg.padres.broker.aggregation.Notifier;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationType;
import ca.utoronto.msrg.padres.common.message.MessageDestination;

public class SimpleAggregatorFactory implements AggregatorFactory<Aggregator> {

	@Override
	public Aggregator createAggregator(Notifier notifier, AggregationID aggregationID, AggregationType aggType, long startTimeOfTheCurrentWindow,
			MessageDestination sourceLink) {

		return new Aggregator(notifier, aggregationID, aggType, startTimeOfTheCurrentWindow, sourceLink);

	}

}
