package ca.utoronto.msrg.padres.broker.aggregation.adaptor;

public class dummyAdaptorFactory implements AdaptorFactory<dummyAdaptor> {

	@Override
	public dummyAdaptor createAdaptor() {
		return new dummyAdaptor();
	}

}
