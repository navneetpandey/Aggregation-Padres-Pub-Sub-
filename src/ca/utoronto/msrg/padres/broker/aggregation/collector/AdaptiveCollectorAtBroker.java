package ca.utoronto.msrg.padres.broker.aggregation.collector;

import ca.utoronto.msrg.padres.broker.aggregation.AbstractAggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.Notifier;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;

public class AdaptiveCollectorAtBroker extends CollectorAtBroker {

	public AdaptiveCollectorAtBroker(Notifier notifier,
			AggregationID aggregationID, long waitingTime,
			AbstractAggregationEngine aggregationEngine) {
		super(notifier, aggregationID, waitingTime, aggregationEngine);
	 
	}
	public boolean isWindowComplete(boolean schedulerContains, int expectingBrokerSize, OutgoingWindow outgoingWindow){
	// return false;
		return super.isWindowComplete(schedulerContains, expectingBrokerSize, outgoingWindow);
	}

}
