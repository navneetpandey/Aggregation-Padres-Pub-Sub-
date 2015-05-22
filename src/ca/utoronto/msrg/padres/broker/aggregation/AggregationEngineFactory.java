package ca.utoronto.msrg.padres.broker.aggregation;

import ca.utoronto.msrg.padres.broker.aggregation.aggregator.SmartAggregator.SmartAggregatorFactory;
import ca.utoronto.msrg.padres.broker.aggregation.utility.AggregationInfo;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;

public class AggregationEngineFactory {

	public static AggregationEngine createAggregationEngine(BrokerCore brockerCore, AggregationInfo aggregationInfo){
		switch(aggregationInfo.getAggregationImplementation()){
		
		case ADAPTIVE_AGGREGATION: 
			return new AdaptiveAggregationEngine(brockerCore, aggregationInfo);
		case EARLY_AGGREGATION:
			return new EarlyAggregationEngine(brockerCore, aggregationInfo);
		case LATE_AGGREGATION:
			return new LateAggregationEngine(brockerCore, aggregationInfo);
		case OPTIMAL_AGGREGATION:
			return new OptimalAggregationEngine(brockerCore, aggregationInfo);
		case FG_AGGREGATION:
			return new FineGrainAdaptiveAggregationEngine(brockerCore, aggregationInfo);
		default: 
			return null;
		}
		
	}
	
	
}
