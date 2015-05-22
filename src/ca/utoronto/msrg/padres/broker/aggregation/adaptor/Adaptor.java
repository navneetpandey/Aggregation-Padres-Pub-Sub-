package ca.utoronto.msrg.padres.broker.aggregation.adaptor;

import ca.utoronto.msrg.padres.common.message.PublicationMessage;

public interface Adaptor {

	public void addPubToSample(PublicationMessage publicationMessage);
	public boolean shouldAggregate();
	
}
