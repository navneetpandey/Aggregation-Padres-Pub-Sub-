package ca.utoronto.msrg.padres.broker.aggregation;

import java.util.HashMap;
import java.util.Set;

import ca.utoronto.msrg.padres.broker.aggregation.adaptor.dummyAdaptor;
import ca.utoronto.msrg.padres.broker.aggregation.adaptor.dummyAdaptorFactory;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.aggregator.SmartAggregator;
import ca.utoronto.msrg.padres.broker.aggregation.aggregator.SmartAggregator.SmartAggregatorFactory;
import ca.utoronto.msrg.padres.broker.aggregation.collector.DummyCollector;
import ca.utoronto.msrg.padres.broker.aggregation.collector.DummyCollector.dummyCollectorFactory;
import ca.utoronto.msrg.padres.broker.aggregation.utility.AggregationInfo;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.common.message.Message;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.common.message.SubscriptionMessage;

public class FineGrainAdaptiveAggregationEngine extends PerOutgoingLinkAggregationEngine<SmartAggregator, DummyCollector, dummyAdaptor> {

	public long WAITINGTIME = -1;

	public FineGrainAdaptiveAggregationEngine(BrokerCore brockerCore, AggregationInfo agInfo) {
		super(brockerCore, agInfo, new SmartAggregatorFactory(), new dummyCollectorFactory(), new dummyAdaptorFactory());
		pubToFWD = new HashMap<MessageDestination, Set<Publication>>();

	}

	@Override
	public void processSubscriptionMessage(SubscriptionMessage subMsg, Set<Message> outGoingmessageSet) {

		AggregationID aggID = subMsg.getSubscription().getAggregationID();

		// TODO Need to be fixed to support for different shift size for
		// different subscriptionsupdateMatchedAggregatorSet();
		if (WAITINGTIME == -1) {
			WAITINGTIME = aggID.getShiftSize() * 1000;
			getAggregatorTimerService().addFGTask(this);
		}

		super.processSubscriptionMessage(subMsg, outGoingmessageSet);
		currentSubAggregator.setDestination(currentSubDestination);
		currentSubAggregator.setPubToFWD(pubToFWD.get(currentSubDestination));
		currentSubCollector.setDestination(currentSubDestination);
	}

	@Override
	public void getNotificationFromAggregator(SmartAggregator aggregator) {
		//System.out.println(notifier.getBrokerCore().getBrokerID() + "\nGoing to check aggregator for dest " + aggregator.getDestination() + "\n");
		aggregator.publishResult(false);
	}

	@Override
	public void aggregate(MessageDestination destination) {

		if (currentPublicationProfile.getOverlappLink().contains(destination)){
			//System.out.println("[NKDEBUG]3 overlapped link"); 
			return;
		}

		for (SmartAggregator smartAggregator : matchedAggregatorMap.get(destination)) {
			if (currentPublicationProfile.getPublicationMessage().getOnlyForTheseAggregators() == null
					|| currentPublicationProfile.getPublicationMessage().getOnlyForTheseAggregators().contains(smartAggregator.getAggregationID())) {
				smartAggregator.addPublication(currentPublicationProfile.getPublication(), matchedAggregatorMap.get(destination).size());
				//System.out.println("[NKDEBUG]1" +"Size"+ matchedAggregatorMap.get(destination).size());
			}else{
				//System.out.println("[NKDEBUG]2" + currentPublicationProfile.getPublicationMessage().getOnlyForTheseAggregators());
			}
			// System.err.println("[FG-"+notifier.getBrokerCore().getBrokerID()+"] add pub in agg for dest "+smartAggregator.getDestination()+
			// " with score "+score);
		}

	}

}
