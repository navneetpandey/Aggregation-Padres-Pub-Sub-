package ca.utoronto.msrg.padres.broker.aggregation.aggregator;

import ca.utoronto.msrg.padres.broker.aggregation.Notifier;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationMode;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationType;
import ca.utoronto.msrg.padres.broker.aggregation.utility.TimeValueComparablePair;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.Publication;

public class Aggregator extends AbstractAggregator {



	private boolean overlappedWithNormalSubscritpion;

	private AggregationMode aggregatorMode;

	boolean transitionHappening;


	
	public boolean isOverlappedWithNormalSubscritpion() {
		return overlappedWithNormalSubscritpion;
	}

	public void setOverlappedWithNormalSubscritpion(boolean overlappedWithNormalSubscritpion) {
		this.overlappedWithNormalSubscritpion = overlappedWithNormalSubscritpion;
	}

	public Aggregator(Notifier notifier, AggregationID aggregationID, AggregationType aggregationType, long startTimeOfTheCurrentWindow,
			MessageDestination sourceLink) {
		super(notifier, aggregationID, aggregationType, startTimeOfTheCurrentWindow, sourceLink);
		
		
		this.overlappedWithNormalSubscritpion = false;
		aggregatorMode = AggregationMode.AGG_MODE;
		this.transitionHappening = false;
		notifier.getBrokerCore().getRouter().getPostProcessor().getAggregationEngine().getAggregatorTimerService().registerAggregator(this);

		if (VERBOSE_ON)
			System.out.println("StartTime " + startTimeOfTheCurrentWindow + " " + Math.max((windowSize - shiftSize) * MILLISECOND, 0));
		
	}


	public void switchMode(AggregationMode newMOD) {
		if (newMOD != AggregationMode.TRANSIT_MODE) {
			aggregatorMode = newMOD;
		}

	}
	

	public boolean isAggregationMode() {
		if (aggregatorMode == AggregationMode.AGG_MODE)
			return true;
		else
			return false;
	}

	public void setTransitionON() {
		transitionHappening = true;

	}

	public void setTransitionOFF() {
		transitionHappening = false;

	}

	public boolean isInTransitionMode() {
		return transitionHappening;
	}

	public void forwardContainedPublication() {
		// TODO
		// Rather than forwarding , just deleting for a while
		timeAndPubValueQueue.clear();
	}

}
