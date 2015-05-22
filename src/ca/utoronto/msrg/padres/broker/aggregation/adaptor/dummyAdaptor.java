package ca.utoronto.msrg.padres.broker.aggregation.adaptor;

import ca.utoronto.msrg.padres.common.message.PublicationMessage;

public class dummyAdaptor implements Adaptor {

	@Override
	public void addPubToSample(PublicationMessage publicationMessage) {

	}

	@Override
	public boolean shouldAggregate() {
		return true;
	}

}
