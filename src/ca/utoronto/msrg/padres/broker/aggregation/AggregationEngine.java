package ca.utoronto.msrg.padres.broker.aggregation;

import java.util.Set;

import ca.utoronto.msrg.padres.broker.aggregation.utility.AggregationTimerService;
import ca.utoronto.msrg.padres.common.message.Message;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.SubscriptionMessage;

public interface AggregationEngine {
	public void aggregatePublicationMessage();

	public void processSubscriptionMessage(SubscriptionMessage subMsg, Set<Message> outGoingmessageSet);

	// public long getLargestShiftSize();

	public Notifier getNotifier();

	public void setNotifier(Notifier notifier);

	/*
	 * public Set<Aggregator> getAggregatorSet();
	 * 
	 * 
	 * public ArrayList<CollectorAtBroker> getCollectorSet();
	 */

	//public void notifyFromAggregatorTimer(int timecount);

	// private void processPublicationForClient(PublicationMessage pubMsg);

	public AggregationTimerService getAggregatorTimerService();

	public void processPublicationMessage(PublicationMessage msg, Set<Message> messageSet);

}
