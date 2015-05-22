package ca.utoronto.msrg.padres.broker.aggregation;

import java.util.ArrayList;
import java.util.Set;

import ca.utoronto.msrg.padres.broker.aggregation.adaptation.AdaptationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationMode;
import ca.utoronto.msrg.padres.broker.aggregation.aggregator.Aggregator;
import ca.utoronto.msrg.padres.broker.aggregation.aggregator.SimpleAggregatorFactory;
import ca.utoronto.msrg.padres.broker.aggregation.collector.AdaptiveCollectorAtBroker;
import ca.utoronto.msrg.padres.broker.aggregation.collector.AdaptiveCollectorFactory;
import ca.utoronto.msrg.padres.broker.aggregation.collector.CollectorAtBroker;
import ca.utoronto.msrg.padres.broker.aggregation.collector.SimpleCollectorFactory;
import ca.utoronto.msrg.padres.broker.aggregation.utility.AggregationInfo;
import ca.utoronto.msrg.padres.broker.aggregation.utility.Stats;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.common.message.Message;
import ca.utoronto.msrg.padres.common.message.Subscription;
import ca.utoronto.msrg.padres.common.message.SubscriptionMessage;

public class AdaptiveAggregationEngine extends CollectionBasedAggregationEngine<Aggregator, AdaptiveCollectorAtBroker> {

	boolean timeBased = true;
	AdaptationEngine adaptationEngine;
	boolean VERBOSE=true;
		
	long largestWindow;

	public AdaptiveAggregationEngine(BrokerCore brockerCore,
			AggregationInfo agInfo) {
		// THere will be entry for every aggregation query
		super(brockerCore, agInfo, new SimpleAggregatorFactory(), new AdaptiveCollectorFactory());
		adaptationEngine = new AdaptationEngine(brockerCore.getBrokerConfig()
				.isAdaptationForAggregation(),this);
		largestWindow = 0;

	}

	

	public void aggregatePublicationMessage() {
 		long startTime=System.currentTimeMillis();
		long startTimeNano=System.nanoTime();
		
		updateAdaptationEngine();
		if(currentPublicationProfile.isAggregatedPublication()){
			super.collectMessage();
			return;
		}
		
		
		if (currentPublicationProfile.getMatchedAggregationSet().isEmpty() || !currentPublicationProfile.getPublicationMessage().aggregationRequired()) {
			Stats.getInstance("");Stats.getInstance("test").addValue("TIME.RECEIVE",
					System.currentTimeMillis() - startTime);	
			return;
		}

		logger.info("[AGE]" + this.brokerCore.getBrokerID()
				+ " receives normal pub" + currentPublicationProfile.getPublication().toString()
				+ " || " + System.currentTimeMillis() + " ||");


		switch (adaptationEngine.getSwitchStatus()) {

		case RAW_MODE: 
			forwardRawMessage();
			for(Aggregator aggregator : getMatchedAggregatorSet()) {
				for ( String windowID : getWindowIds(aggregator)) 
					aggregator.getCollector().normalMsgObserved(currentPublicationProfile.getPublicationMessage().getLastHopID(), windowID);
			}
			break;

		case AGG_MODE:
			aggregate();
			break;

		case TRANSIT_MODE:
			transition();

		}

		if (adaptationEngine.getAggregationMode() == AggregationMode.TRANSIT_MODE) {
			adaptationEngine.transitionCompletionCheck();
		}

		if (adaptationEngine.checkForAggregationTrigger()) {
			adaptationEngine.switchMode(AggregationMode.TRANSIT_MODE);
			Stats.getInstance("test").addValue("Transition.number",1);
			adaptationEngine.setTransitionStartTime(System.currentTimeMillis());
			System.out.println("[ADAP_AGE] + TRANSITION START "+ System.currentTimeMillis());
			setTransitionToAllAggregator();
		}

		Stats.getInstance("test").addValue("TIME.RECEIVE",
				System.currentTimeMillis() - startTime);
	}

	private void setTransitionToAllAggregator() {
		for (Aggregator ag : getAggregatorSet()) {
			ag.setTransitionON();
		}
		
	}
	
	public void resetTransitionToAllAggregator() {
		for (Aggregator ag : getAggregatorSet()) {
			ag.setTransitionOFF();
		}
		
	}



	public long getLargestWindow() {
		return largestWindow;
	}



	private void updateAdaptationEngine() {

		if (currentPublicationProfile.getMatchedAggregationSet().isEmpty() || !currentPublicationProfile.getPublicationMessage().aggregationRequired() ||
				!currentPublicationProfile.getOverlappLink().isEmpty())
			return;

		logger.info("[AGE-" + brokerCore.getBrokerID()
				+ "]sendUpdateToAdaptationEngine=> matcheAggregatorSet size "
				+ currentPublicationProfile.getMatchedAggregationSet().size());
		if (!currentPublicationProfile.getMatchedAggregationSet().isEmpty())
			adaptationEngine.addPubMessage(currentPublicationProfile.getPublicationMessage());

		for (AggregationID aggregationID :  currentPublicationProfile.getMatchedAggregationSet())
			adaptationEngine.addNotificationPerAggregator(aggregationID);

	}

	public void transition() {

		boolean aggregated = false;

		ArrayList<AggregationID> rawModeAggregatorList = new ArrayList<AggregationID>();

		for (Aggregator aggregator :  getMatchedAggregatorSet()) {

			// special case for sliding window
			if ( Aggregator.isSlidingWindow(aggregator.getAggregationID())) {
				
				if(adaptationEngine.isTransitionFromRawToAGG()) {
					aggregator.addPublication(currentPublicationProfile.getPublication());
					
					if (hasNewNWRAfterTransition(aggregator, currentPublicationProfile.getPublication().getTimeStamp().getTime())) 
						currentPublicationProfile.getPublicationMessage().fwdOnlyForTheseAggregatorID(aggregator.getAggregationID());
					
				} else {
					currentPublicationProfile.getPublicationMessage().fwdOnlyForTheseAggregatorID(aggregator.getAggregationID());
					if (hasNewNWRAfterTransition(aggregator, currentPublicationProfile.getPublication().getTimeStamp().getTime())) 
						aggregator.addPublication(currentPublicationProfile.getPublication());
				}
				

			} else {// for tumbling and sampling window
				if (aggregator.isAggregationMode()) {
					aggregator.addPublication(currentPublicationProfile.getPublication());
					aggregated = true;
				} else {
					rawModeAggregatorList.add(aggregator.getAggregationID());
				}
			}
		}

		if (aggregated && !rawModeAggregatorList.isEmpty()) {
			for (AggregationID aggregationID : rawModeAggregatorList) {
				currentPublicationProfile.getPublicationMessage().fwdOnlyForTheseAggregatorID(aggregationID);
			}
		}
		//TODO : Fix required for sliding window ( It should not send for all destination which is happening in forwardRawMessage
		if(!rawModeAggregatorList.isEmpty()) {
			forwardRawMessage();
		}

	}
	

	/*protected Set<Aggregator> findMatchingAggregators() {
		Set<Aggregator> requireAggProcessing = super.findMatchingAggregators();
		
		if(pubMsg.getOnlyForTheseAggregators() != null) {
			 Set<AggregationID> fwdAggregatorList = pubMsg.getOnlyForTheseAggregators() ;
			 Iterator<Aggregator>  itr = requireAggProcessing.iterator();
			while(itr.hasNext()){
				if(!fwdAggregatorList.contains(itr.next().getAggregationID())){
					itr.remove();
				}
			}
			pubMsg.resetForwardAggregatorList();
		}

		return requireAggProcessing;

	}*/


	public AdaptationEngine getAdaptationEngine() {
		return adaptationEngine;
	}

	public ArrayList<AdaptiveCollectorAtBroker> getCollectorSet() {
		return collectorSet;
	}
	
	private boolean hasNewNWRAfterTransition(Aggregator aggregator, long currentTime){
		 return aggregator.getAggregationID().getShiftSize() < currentTime
		- adaptationEngine.getTransitionStartTime();
	}

	
	public void getNotificationFromAggregator(Aggregator aggregator) {
		long startTime=System.currentTimeMillis();
		
		if (aggregator.isAggregationMode()) { // intentionally avoiding RAW-AGG
												// switch handling
			aggregator.publishResult(false);

			if (aggregator.isInTransitionMode() ) {
				if(Aggregator.isSlidingWindow(aggregator.getAggregationID())) {
					if(System.currentTimeMillis() - adaptationEngine.getTransitionStartTime()> aggregator.getAggregationID().getWindowSize()) {
						aggregator.setTransitionOFF();
						aggregator.switchMode(AggregationMode.RAW_MODE);
						//forward publications stored for next window
						aggregator.forwardContainedPublication();
					}
						
				} else {
					aggregator.setTransitionOFF();
					aggregator.switchMode(AggregationMode.RAW_MODE);
					//forward publications stored for next window
					aggregator.forwardContainedPublication();
				}
			}

		} else {
			aggregator.resetWindowTime();
			if (aggregator.isInTransitionMode()) {
				aggregator.setTransitionOFF();
				aggregator.switchMode(AggregationMode.AGG_MODE);
			}
		}
		
		Stats.getInstance("");Stats.getInstance("test").addValue("TIME.NOTIFY",
				System.currentTimeMillis() - startTime);
	}

	public void updateLargestShiftSize(AggregationID aggregationID) {
		super.updateLargestShiftSize(aggregationID);
		if (aggregationID.getWindowSize() > largestWindow)
				largestWindow = aggregationID.getWindowSize();
		
		 
	}
	
	public void processSubscriptionMessage(SubscriptionMessage subMsg,
			Set<Message> outGoingmessageSet) {
		super.processSubscriptionMessage(subMsg, outGoingmessageSet);
		Subscription sub = subMsg.getSubscription();
		/*if (sub.isAggregation()) {
			addClient(subMsg);
			
		}*/
	}


	 
}
