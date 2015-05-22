package ca.utoronto.msrg.padres.broker.aggregation.adaptation;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import ca.utoronto.msrg.padres.broker.aggregation.AdaptiveAggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationMode;
import ca.utoronto.msrg.padres.broker.aggregation.aggregator.Aggregator;
import ca.utoronto.msrg.padres.broker.aggregation.utility.Stats;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;

public class AdaptationEngine {
	

	final int ADAPTATION_FREQUENCY = 1;
	
	final long SAMPLE_WINDOW_SIZE = 2000L;
	
	AdaptationSwitch adaptationSwitch;
	
	public AdaptationSwitch getAdaptationSwitch() {
		return adaptationSwitch;
	}

	AdaptiveAggregationEngine adaptiveAggregationEngine;

	long transitionStartTime;
	AggregationMode previousAggregationState;
	
	
	TransitFacilitator transitFacilitator;
	
	public AggregationMode getAggregationMode() {
		return aggregationMode;
	}

	private AggregationMode aggregationMode;

	//protected AdaptationController adaptationController;

	AggregationMode forcedMode = null;
	

	private long adaptationStartTime; 
	
	public AdaptationEngine(boolean adaptationON, AdaptiveAggregationEngine adaptiveAggregationEngine){
		
		aggregationMode = AggregationMode.AGG_MODE;
		previousAggregationState = AggregationMode.AGG_MODE;
		adaptationSwitch = new AdaptationSwitch(SAMPLE_WINDOW_SIZE);
		//adaptationController = new AdaptationController(adaptationSwitch,
		//		ADAPTATION_FREQUENCY);
		transitFacilitator = new TransitFacilitator();
		transitionStartTime = 0;
		adaptationStartTime=System.currentTimeMillis();
		
		if(!adaptationON){
			forcedMode=AggregationMode.AGG_MODE;
		}
		this.adaptiveAggregationEngine = adaptiveAggregationEngine;
		
			
		
	}


	public void addPubMessage(PublicationMessage pubMsg) {
		adaptationSwitch.addPubMessage(pubMsg);
		
	}

	public void addNotificationPerAggregator(AggregationID aggregationID){
		adaptationSwitch.addNTFMessages(aggregationID);
	}

	public boolean checkForAggregationTrigger() {
		if(forcedMode!=null)return false;
		if(System.currentTimeMillis()<adaptationStartTime+SAMPLE_WINDOW_SIZE)
			return false;
		return adaptationSwitch.checkForAggregationTrigger();
	}


	public void switchMode(AggregationMode mode) {
		 previousAggregationState = aggregationMode;
		 aggregationMode = mode;
		
	}
	
	public AggregationMode getSwitchStatus() {
		if(forcedMode==null)
			return adaptationSwitch.getAdapStatus();
		return forcedMode;
	}


	public void updateTransitionTable(Aggregator ag, AggregationMode rawMode) {
		transitFacilitator.updateTransitionTable( ag, rawMode) ;
		
	}


	public void setTransitionStartTime(long transitionStartTime) {
		this.transitionStartTime = transitionStartTime;
	}
	
	public long getTransitionStartTime(){
		return this.transitionStartTime;
	}


	public void transitionCompletionCheck() {
		 if(aggregationMode == AggregationMode.TRANSIT_MODE && (isTransitionPeriodOver() || tanstitionCompletedForAllAggregator())) {
			 Stats.getInstance("test").addValue("Transition.duration",System.currentTimeMillis()-transitionStartTime);
			 synchronized(aggregationMode){
				if(previousAggregationState == AggregationMode.AGG_MODE)
					aggregationMode = AggregationMode.RAW_MODE;
				else
					aggregationMode = AggregationMode.AGG_MODE;
						
			 }
		 }
		
	}


	private boolean tanstitionCompletedForAllAggregator() {
		synchronized (adaptiveAggregationEngine.getAggregatorMap()) {
			Set<Aggregator> aggregators =  adaptiveAggregationEngine.getAggregatorSet();
			for( Aggregator aggregator : aggregators) {
				if(aggregator.isInTransitionMode())
					return false;
			}
			return true;
		}
		
	}


	private boolean isTransitionPeriodOver(){

		long currentTime = System.currentTimeMillis();
		if( currentTime > transitionStartTime + adaptiveAggregationEngine.getLargestWindow()) {
			adaptiveAggregationEngine.resetTransitionToAllAggregator();
			return true;
		}
		return false;
	
	}
	public AggregationMode getForcedMode() {
		return forcedMode;
	}


	public void setForcedMode(AggregationMode forcedMode) {
		this.forcedMode = forcedMode;
	}

	public boolean isTransitionFromRawToAGG(){
		if(previousAggregationState == AggregationMode.RAW_MODE)
			return true;
		else
			return false;
	}
	
}
