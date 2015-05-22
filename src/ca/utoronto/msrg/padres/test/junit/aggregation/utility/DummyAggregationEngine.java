package ca.utoronto.msrg.padres.test.junit.aggregation.utility;

import ca.utoronto.msrg.padres.broker.aggregation.CollectionBasedAggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.aggregator.Aggregator;
import ca.utoronto.msrg.padres.broker.aggregation.aggregator.SimpleAggregatorFactory;
import ca.utoronto.msrg.padres.broker.aggregation.collector.DummyCollector;
import ca.utoronto.msrg.padres.broker.aggregation.collector.DummyCollector.dummyCollectorFactory;
import ca.utoronto.msrg.padres.broker.aggregation.utility.AggregationInfo;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerConfig;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCoreException;
import ca.utoronto.msrg.padres.broker.router.ReteRouter;

public class DummyAggregationEngine extends CollectionBasedAggregationEngine<Aggregator, DummyCollector>{

	public DummyAggregationEngine(BrokerCore brockerCore, AggregationInfo agInfo) throws BrokerCoreException {
		super(new BrokerCore(new BrokerConfig()), agInfo, new SimpleAggregatorFactory(), new dummyCollectorFactory());
		this.router=new ReteRouter(this.brokerCore);
		// TODO Auto-generated constructor stub
	}

	@Override
	public void aggregatePublicationMessage() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void getNotificationFromAggregator(Aggregator aggregator) {
		// TODO Auto-generated method stub
		
	}

}
