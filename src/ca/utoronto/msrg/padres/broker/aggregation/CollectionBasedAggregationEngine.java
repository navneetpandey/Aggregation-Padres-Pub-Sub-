package ca.utoronto.msrg.padres.broker.aggregation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationType;
import ca.utoronto.msrg.padres.broker.aggregation.aggregator.AbstractAggregator;
import ca.utoronto.msrg.padres.broker.aggregation.aggregator.Aggregator;
import ca.utoronto.msrg.padres.broker.aggregation.aggregator.AggregatorFactory;
import ca.utoronto.msrg.padres.broker.aggregation.collector.AbstractCollector;
import ca.utoronto.msrg.padres.broker.aggregation.collector.CollectorAtBroker;
import ca.utoronto.msrg.padres.broker.aggregation.collector.CollectorFactory;
import ca.utoronto.msrg.padres.broker.aggregation.message.AggregatedPublication;
import ca.utoronto.msrg.padres.broker.aggregation.utility.AggregationInfo;
import ca.utoronto.msrg.padres.broker.aggregation.utility.AggregationTimerService;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.broker.router.matching.MatcherException;
import ca.utoronto.msrg.padres.common.message.AdvertisementMessage;
import ca.utoronto.msrg.padres.common.message.Message;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.Subscription;
import ca.utoronto.msrg.padres.common.message.SubscriptionMessage;

public abstract class CollectionBasedAggregationEngine<A extends AbstractAggregator, C extends AbstractCollector> extends AbstractAggregationEngine {

	protected ArrayList<C> collectorSet = new ArrayList<C>();
	protected HashMap<AggregationID, HashSet<A>> aggregatorMap = new HashMap<AggregationID, HashSet<A>>();

	AggregationTimerService collectorTimerService;

	AggregatorFactory<A> aggregatorFactory;
	
	CollectorFactory<C> collectorFactory;
	
	Set<A> matchedAggregtors;
	public CollectionBasedAggregationEngine(BrokerCore brockerCore, AggregationInfo agInfo, AggregatorFactory<A> aggregatorFactory, CollectorFactory<C> collectorFactory) {
		super(brockerCore, agInfo);
		collectorTimerService = new AggregationTimerService(brockerCore.getBrokerConfig().getExperimentDuration());
		this.aggregatorFactory=aggregatorFactory;
		this.collectorFactory=collectorFactory;
		
	}

	@Override
	public void aggregatePublicationMessage() {

		if (currentPublicationProfile.isAggregatedPublication()) {
			logger.info("[AGE-" + brokerCore.getBrokerID() + "]processPerMessage=>" + " got AggregatedPublication "
					+ currentPublicationProfile.getPublication().getPairMap() + " || " + System.currentTimeMillis() + " ||");
			collectMessage();

		}

	}

	@Override
	public void processPublicationMessage(PublicationMessage msg, Set<Message> messageSet) {
		// TODO Auto-generated method stub
		super.processPublicationMessage(msg, messageSet);
	}

	@Override
	public void processSubscriptionMessage(SubscriptionMessage subMsg, Set<Message> outGoingmessageSet) {
		super.processSubscriptionMessage(subMsg, outGoingmessageSet);

		Subscription sub = subMsg.getSubscription();
		AggregationID subID = sub.getAggregationID();

		// TODO: set predicate to print, only if edge broker then store the
		// value
		if (sub.isAggregation()) {
			logger.info("[AGE" + brokerCore.getBrokerID() + "]processSubscriptionMessage=>" + subMsg.getSubscription().getPredicateMap().toString());

			HashSet<A> matchedAggregatorSetForLink = new HashSet<A>();
			HashSet<A> matchedAggregatorSetForAggID = aggregatorMap.get(subID);
			if (matchedAggregatorSetForAggID == null) {
				matchedAggregatorSetForAggID = new HashSet<A>();
				aggregatorMap.put(subID, matchedAggregatorSetForAggID);
			}

			long startOfTheAggregationWindow = getStartOfTheWindow(subMsg);

			for (Message outMessage : outGoingmessageSet) {
				boolean found = false;
				for (A ag : matchedAggregatorSetForAggID) {
					if (outMessage.getNextHopID().equals(ag.getSourceLink())) {
						matchedAggregatorSetForLink.add(ag);
						found = true;
						startOfTheAggregationWindow = ag.getStartTimeOfTheCurrentWindow();
						break;
					}

				}
				if (!found) {
					matchedAggregatorSetForLink.add(aggregatorFactory.createAggregator(notifier, sub.getAggregationID(), AggregationType.TIMEBASED,
							startOfTheAggregationWindow, outMessage.getNextHopID()));

				}
			}

			if (!matchedAggregatorSetForLink.isEmpty()) {
				synchronized (matchedAggregatorSetForAggID) {
					matchedAggregatorSetForAggID.addAll(matchedAggregatorSetForLink);
				}
			}

			for (A aggregator : matchedAggregatorSetForLink)
				aggregator.registerSubscription(subMsg);

			/*brokerCore.getAggregatorTimer().updateWindowTimeGCD(subMsg.getSubscription().getAggregationID().getWindowSize(),
					subMsg.getSubscription().getAggregationID().getShiftSize());
*/
			String lastBrokerID = subMsg.getLastHopID().toString();
			C collector = null;

			for (C c : collectorSet) {
				if (c.getAggregationID().equals(subID)) {
					collector = c;
					break;
				}
			}

			if (collector == null) {
				collector = (C) collectorFactory.createCollector(notifier, sub.getAggregationID(), DEFAULT_WAITING, //lastBrokerID,
						this);
				synchronized (collectorSet) {
					collectorSet.add(collector);
				}
			}

			checkAndAddSelfPublisher(subMsg, collector);

			updateLargestShiftSize(sub.getAggregationID());

			for (Message m : outGoingmessageSet) {
				for (A aggregator : matchedAggregatorSetForLink) {
					aggregator.subscribeCollector(collector, (SubscriptionMessage) m);
				}
				((SubscriptionMessage) m).setStartOfTheAggregationWindow(startOfTheAggregationWindow);
			}
			
			matchedAggregtors = null;// updateMatchedAggregatorSet();
		}

	}

	public Set<A> getAggregatorSet() {
		HashSet<A> aggregatorSet = new HashSet<A>();

		synchronized (aggregatorMap) {
			Collection<HashSet<A>> c = aggregatorMap.values();
			synchronized (c) {
				for (int i = 0; i < c.size(); i++) {
					aggregatorSet.addAll((Collection<? extends A>) c.toArray()[i]);
				}
				for (Set<A> set : c)
					aggregatorSet.addAll(set);
			}
		}
		return aggregatorSet;
	}

	protected void collectMessage() {

		for (C collector : collectorSet) {
			if (collector.getAggregationID().equals(((AggregatedPublication) currentPublicationProfile.getPublication()).getAggregationID()))
				collector.addMsg(currentPublicationProfile.getPublicationMessage());
		}

	}

	public void checkAndAddSelfPublisher(SubscriptionMessage subMsg, C collector) {
		try {
			Set<String> publishers = router.getMatcher().getMatchingAdvFromSubs(subMsg);
			boolean publisherFound = false;

			for (String publisher : publishers) {
				AdvertisementMessage adm = router.getAdvertisement(publisher);
				if (adm.getLastHopID().getBrokerId().equals(brokerCore.getBrokerDestination().getBrokerId())) {
					publisherFound = true;
					break;
				}
			}
			if (publisherFound) {
				HashSet<A> aggSet = aggregatorMap.get(subMsg.getSubscription().getAggregationID());
				if (aggSet == null) {
					aggSet = new HashSet<A>();
					aggregatorMap.put(subMsg.getSubscription().getAggregationID(), aggSet);
				}

				A aggregator = null;

				for (A ag : aggSet) {
					if (brokerCore.getBrokerDestination().equals(ag.getSourceLink())) {

						aggregator = ag;
						break;
					}

				}
				if (aggregator == null) {
			
					aggregator = aggregatorFactory.createAggregator(notifier, subMsg.getSubscription().getAggregationID(), AggregationType.TIMEBASED,
							getStartOfTheWindow(subMsg.getSubscription().getAggregationID()), new MessageDestination(brokerCore.getBrokerID())); //AggregatorFactory.createAggregator
					aggSet.add(aggregator);

				}

				aggregator.subscribeCollector(collector, subMsg);

				subMsg.setNextHopID(brokerCore.getBrokerDestination());

			}

		} catch (MatcherException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public long getLargestShiftSize() {
		return largestShiftSize;
	}

	public ArrayList<C> getCollectorSet() {
		return collectorSet;
	}

	protected void aggregate() {
		for (A aggregator : getMatchedAggregatorSet())
			aggregator.addPublication(currentPublicationProfile.getPublication());

	}

	protected Set<A> getMatchedAggregatorSet() {
		//if(matchedAggregtors == null)
			matchedAggregtors = updateMatchedAggregatorSet();
		return matchedAggregtors;
	}
	
	
	protected HashSet<A> updateMatchedAggregatorSet() {
		HashSet<A> matchedAggregtorsSet = new HashSet<A>();
		Set<A> aggregatorSet = new HashSet<A>();
		for (AggregationID aggregationID : currentPublicationProfile.getMatchedAggregationSet()) {
			HashSet<A> ag = aggregatorMap.get(aggregationID);
			if(ag != null)
				aggregatorSet.addAll(ag);
		}

		if (!aggregatorSet.isEmpty()) {
			for (A aggregator : aggregatorSet) {
				if ( // aggregator.getAggregationID().equals(
						// router.getSubscriptionMessage(subID).getSubscription().getAggregationID())
						// &&
				aggregator.getSourceLink().getBrokerId().equals(currentPublicationProfile.getPublicationMessage().getLastHopID().getBrokerId()))
					matchedAggregtorsSet.add(aggregator);
			}
		}

		return matchedAggregtorsSet;
	}

	public void notifyFromAggregatorTimer(int timecount) {
		if (aggregatorMap == null)
			return;

		for (int i = 0; i < getAggregatorSet().size(); i++) {
			A aggregator = (A) getAggregatorSet().toArray()[i];
			logger.info("[AGG-" + brokerCore.getBrokerID() + "] Timecount " + timecount + " " + aggregator.getAggregationID().getShiftSize() + " "
					+ (timecount) % aggregator.getAggregationID().getShiftSize() + " || " + System.currentTimeMillis() + " ||");
			if (((timecount) % aggregator.getAggregationID().getShiftSize()) == 0) {
				getNotificationFromAggregator(aggregator);
				// System.out.println("[NTF-"+ this.brokerCore.getBrokerID() +
				// "-]notifyFromAggregator=>" + " timecount matched for aggID "
				// + aggregator.getAggregationID());
			}
		}

	}

	protected long getStartOfTheWindow(AggregationID aggregationID) {
		HashSet<A> aggregatorSet;
		long startOftheWindow;
		if ((aggregatorSet = aggregatorMap.get(aggregationID)) != null && !aggregatorSet.isEmpty()) {
			startOftheWindow = ((Aggregator) aggregatorSet.toArray()[0]).getStartTimeOfTheCurrentWindow();
		} else
			startOftheWindow = genStartOfTheWindow(aggregationID);

		return startOftheWindow;
	}


	public abstract void getNotificationFromAggregator(A aggregator);
	
	
	public HashMap<AggregationID, HashSet<A>> getAggregatorMap() {
		return aggregatorMap;
	}

	public List<String> getWindowIds(Aggregator agg) {
		ArrayList<String> windowIds = new ArrayList<String>();
		long pubTime = currentPublicationProfile.getPublicationTime();
		long win = agg.getAggregationID().getWindowSize();
		long shift = agg.getAggregationID().getShiftSize();
		Set<MessageDestination> dest = new HashSet<MessageDestination>();

		long start = getWindowStartTime(pubTime - win * 1000, agg.getAggregationID());

		dest.addAll(routingTable.getDestinations(agg.getAggregationID(), new MessageDestination(currentPublicationProfile.getPublicationMessage()
				.getLastHopID().getBrokerId())));

		if (currentPublicationProfile.isAggregatedPublication()) {

			windowIds.add(((AggregatedPublication) currentPublicationProfile.getPublication()).getWindowID());

			return windowIds;
		}

		while (start <= pubTime) {
			if (pubTime >= getWindowStartTime(start, agg.getAggregationID()) && pubTime < start + win * MILLISECOND) {
				windowIds.add("" + start + win * MILLISECOND);
			}
			start += shift * MILLISECOND;
		}

		return windowIds;
	}

	public long getWindowStartTime(long time, AggregationID agg) {
		return time - (time % (agg.getShiftSize() * 1000));
	}

	public AggregationTimerService getCollectorTimerService() {
		return collectorTimerService;
	}

	/*
	 * public void notifyFromAggregator(int timecount) {
	 * brokerCore.getRouter().getPostProcessor().getAggregationEngine()
	 * .notifyFromAggregatorTimer(timecount); }
	 */
	/*
	 * public void notifyFromCollector() { ArrayList<CollectorAtBroker>
	 * collectorSet = getCollectorSet(); if (collectorSet == null) return;
	 * 
	 * CollectorAtBroker collector; for (int i = 0; i < collectorSet.size();
	 * i++) { collector = collectorSet.get(i); collector.sendExpiredMessage(); }
	 * 
	 * }
	 */
	//only for test
	public void setMatchedAggregatorSet(Set<A> matchedAggregator) {
		this.matchedAggregtors = matchedAggregator;
		
	}

}
