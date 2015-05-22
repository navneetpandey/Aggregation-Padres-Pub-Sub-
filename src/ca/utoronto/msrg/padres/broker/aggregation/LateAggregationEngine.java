package ca.utoronto.msrg.padres.broker.aggregation;

import java.util.Set;

import ca.utoronto.msrg.padres.broker.aggregation.aggregator.Aggregator;
import ca.utoronto.msrg.padres.broker.aggregation.aggregator.SimpleAggregatorFactory;
import ca.utoronto.msrg.padres.broker.aggregation.collector.CollectorAtBroker;
import ca.utoronto.msrg.padres.broker.aggregation.collector.SimpleCollectorFactory;
import ca.utoronto.msrg.padres.broker.aggregation.utility.AggregationInfo;
import ca.utoronto.msrg.padres.broker.aggregation.utility.Stats;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.common.message.Message;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.common.message.SubscriptionMessage;

public class LateAggregationEngine extends CollectionBasedAggregationEngine<Aggregator,CollectorAtBroker> {

	public LateAggregationEngine(BrokerCore brockerCore, AggregationInfo agInfo) {
		super(brockerCore, agInfo, new SimpleAggregatorFactory(), new SimpleCollectorFactory());
	}

	
	@Override
	public void aggregatePublicationMessage() {
		long startTime = System.currentTimeMillis();
		//Forward for all desitnation except for client
		forwardRawMessage();
		//Aggregate only for client ( if you find aggregator)
		aggregate();
		Stats.getInstance("test").addValue("TIME.RECEIVE", System.currentTimeMillis() - startTime);
	}

	
	@Override
	public void processSubscriptionMessage(SubscriptionMessage subMsg,
			Set<Message> outGoingmessageSet) {
		if (!subMsg.getLastHopID().isBroker()) {
			super.processSubscriptionMessage(subMsg, outGoingmessageSet);
			//subMsg.getSubscription().setAggregation(false);
//			for(Message msg: outGoingmessageSet)
//				((SubscriptionMessage)msg).getSubscription().setAggregation(false);
		}
		else
			registerSubMessageForRouting(subMsg, outGoingmessageSet);
	}

	@Override
	public void getNotificationFromAggregator(Aggregator aggregator) {
	
		long startTime = System.currentTimeMillis();

		aggregator.publishResult(true);
		aggregator.resetWindowTime();

		Stats.getInstance().addValue("TIME.NOTIFY", System.currentTimeMillis() - startTime);

	}
	
	
	protected void forwardRawMessage() {
		setAggregationRequiredOnlyForTheseLinks(currentPublicationProfile.getOverlappLink());
		for (MessageDestination dest : currentPublicationProfile.getPureAggregateSubDestination()) {
			if(!dest.isBroker()) 
				continue;
			Publication p = currentPublicationProfile.getPublication().duplicate();
			logger.info("[AGE]" + this.brokerCore.getBrokerID() + " forwared raw message to destination " + dest.toString());
			notifier.SendRawPublication(p, dest);
		}

	}

}
