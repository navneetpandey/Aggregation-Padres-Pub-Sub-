package ca.utoronto.msrg.padres.broker.aggregation.adaptation;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationMode;
import ca.utoronto.msrg.padres.broker.aggregation.aggregator.Aggregator;
import ca.utoronto.msrg.padres.common.message.Subscription;

public class TransitFacilitator {

	Map<Aggregator, Boolean> transitTable;

	public TransitFacilitator() {
		transitTable = new HashMap<Aggregator, Boolean>();
	}

	public void prepareTransit(Set<Aggregator> aggregatorSet) {

		for( Aggregator ag: aggregatorSet ){
			transitTable.put(ag, true);
		}
	}

	public void doAggIDTagging(Subscription sub) {
		//TODO

	}


	public void processMessage(){}//may be not required

	public void isTransitionOver(){
		//TODO check in the table
	}

	public void closeTransition(){
		transitTable.clear();
	}
	
	/*
	 * If old value exist it overwrites the old one with new value 
	 */

	public void updateTransitionTable(Aggregator ag, AggregationMode rawMode) {
		
		if(rawMode == AggregationMode.RAW_MODE)
			transitTable.put(ag, false);
		else
			transitTable.put(ag, true);

	}

}
