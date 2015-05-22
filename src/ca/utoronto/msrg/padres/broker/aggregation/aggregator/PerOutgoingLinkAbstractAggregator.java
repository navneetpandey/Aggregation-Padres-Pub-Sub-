package ca.utoronto.msrg.padres.broker.aggregation.aggregator;

import ca.utoronto.msrg.padres.broker.aggregation.Notifier;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationType;
import ca.utoronto.msrg.padres.common.message.MessageDestination;

public abstract class PerOutgoingLinkAbstractAggregator extends AbstractAggregator {

	MessageDestination destination;
	
	public boolean aggregate;
	

	public MessageDestination getDestination() {
		return destination;
	}

	public void setDestination(MessageDestination destination) {
		this.destination = destination;
	}

	public PerOutgoingLinkAbstractAggregator(Notifier notifier, AggregationID aggregationID, AggregationType aggregationType,
			long startTimeOfTheCurrentWindow, MessageDestination sourceLink) {
		super(notifier, aggregationID, aggregationType, startTimeOfTheCurrentWindow, sourceLink);
	}

}
