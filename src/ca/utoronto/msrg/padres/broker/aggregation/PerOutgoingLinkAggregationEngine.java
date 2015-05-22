package ca.utoronto.msrg.padres.broker.aggregation;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import ca.utoronto.msrg.padres.broker.aggregation.adaptor.Adaptor;
import ca.utoronto.msrg.padres.broker.aggregation.adaptor.AdaptorFactory;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationType;
import ca.utoronto.msrg.padres.broker.aggregation.aggregator.AggregatorFactory;
import ca.utoronto.msrg.padres.broker.aggregation.aggregator.PerOutgoingLinkAbstractAggregator;
import ca.utoronto.msrg.padres.broker.aggregation.collector.AbstractCollector;
import ca.utoronto.msrg.padres.broker.aggregation.collector.CollectorFactory;
import ca.utoronto.msrg.padres.broker.aggregation.message.AggregatedPublication;
import ca.utoronto.msrg.padres.broker.aggregation.utility.AggregationInfo;
import ca.utoronto.msrg.padres.broker.aggregation.utility.Stats;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.common.message.Message;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.SubscriptionMessage;

public abstract class PerOutgoingLinkAggregationEngine<Agg extends PerOutgoingLinkAbstractAggregator, Coll extends AbstractCollector, Adap extends Adaptor>
		extends AbstractAggregationEngine {

	protected Map<MessageDestination, Adap> adaptorMap;
	protected Map<MessageDestination, Set<Agg>> matchedAggregatorMap;
	protected Map<MessageDestination, Set<Publication>> pubToFWD;
	protected Map<MessageDestination, Set<Agg>> aggregatorMapPerOutgoingLink;

	protected Map<AggregationID, Set<Agg>> aggregatorMapPerAggID;

	protected HashSet<Coll> collectors;

	AggregatorFactory<Agg> aggregatorFactory;
	CollectorFactory<Coll> collectorFactory;
	AdaptorFactory<Adap> adaptorFactory;

	// Variables containing information about processSubscriptionMessage
	protected MessageDestination currentSubDestination;
	protected MessageDestination currentSubSource;
	protected Agg currentSubAggregator;
	protected Coll currentSubCollector;

	public PerOutgoingLinkAggregationEngine(BrokerCore brockerCore, AggregationInfo agInfo, AggregatorFactory<Agg> aggregatorFactory,
			CollectorFactory<Coll> collectorFactory, AdaptorFactory<Adap> adaptorFactory) {
		super(brockerCore, agInfo);
		// TODO Auto-generated constructor stub

		this.aggregatorFactory = aggregatorFactory;
		this.collectorFactory = collectorFactory;
		this.adaptorFactory = adaptorFactory;

		aggregatorMapPerOutgoingLink = new HashMap<MessageDestination, Set<Agg>>();
		aggregatorMapPerAggID = new HashMap<AggregationID, Set<Agg>>();
		collectors = new HashSet<Coll>();
		matchedAggregatorMap = new HashMap<MessageDestination, Set<Agg>>();
		adaptorMap = new HashMap<MessageDestination, Adap>();
		//Below is useless as publicaiton profile contains copy of messageset
		currentPublicationProfile.setSelectivelyDisableAggregationRequired();

	}

	private void addNewOutgoingLink(MessageDestination md) {
		//pubToFWD.put(md, Collections.synchronizedSet(new HashSet<Publication>()));
		pubToFWD.put(md, new HashSet<Publication>());
		aggregatorMapPerOutgoingLink.put(md, new HashSet<Agg>());
		matchedAggregatorMap.put(md, new HashSet<Agg>());
		adaptorMap.put(md, adaptorFactory.createAdaptor());
	}

	public void processSubscriptionMessage(SubscriptionMessage subscriptionMsg, Set<Message> outGoingmessageSet) {

		
		if (!subscriptionMsg.getSubscription().isAggregation()) return;
		
		SubscriptionMessage subMsg=subscriptionMsg.duplicateAGG();
		
		AggregationID aggID = subMsg.getSubscription().getAggregationID();

		

		if (outGoingmessageSet.isEmpty()) {
			currentSubDestination = subMsg.getLastHopID();
			subMsg.setNextHopID(brokerCore.getBrokerDestination());
			currentSubSource = subMsg.getNextHopID();
			System.out.println("DEST " + currentSubDestination + " SRC " + currentSubSource);
		} else {
			SubscriptionMessage m = ((SubscriptionMessage) outGoingmessageSet.toArray()[0]).duplicateAGG();
			currentSubDestination = m.getLastHopID();
			currentSubSource = m.getNextHopID();
		}
		this.registerSubMessageForRouting(subMsg, outGoingmessageSet);

		if (!pubToFWD.containsKey(currentSubDestination)) {
			addNewOutgoingLink(currentSubDestination);
		}

		Set<Agg> aggSet = aggregatorMapPerOutgoingLink.get(currentSubDestination);
		currentSubAggregator = aggregatorFactory.createAggregator(notifier, aggID, AggregationType.TIMEBASED, genStartOfTheWindow(aggID),
				currentSubSource);

		currentSubCollector = collectorFactory.createCollector(notifier, aggID, DEFAULT_WAITING, this);

		collectors.add(currentSubCollector);
		currentSubAggregator.registerSubscription(subMsg);
		currentSubAggregator.subscribeCollector(currentSubCollector, subMsg);
		aggSet.add(currentSubAggregator);

		if (!aggregatorMapPerAggID.containsKey(aggID))
			aggregatorMapPerAggID.put(aggID, new HashSet<Agg>());
		aggregatorMapPerAggID.get(aggID).add(currentSubAggregator);
		

		
	}

	private void dirtySend(Publication p, MessageDestination dest, Set<AggregationID> aggs) {
		PublicationMessage msgToRoute = new PublicationMessage(p.duplicate());
		msgToRoute.setAggregationRequired(true);
		if (msgToRoute.getMessageID().equals(""))
			msgToRoute.setMessageID(brokerCore.getNewMessageID());
		msgToRoute.setNextHopID(dest);
		msgToRoute.setLastHopID(brokerCore.getBrokerDestination());
		for (AggregationID aggregationID : aggs) {
			msgToRoute.fwdOnlyForTheseAggregatorID(aggregationID);
		}

		notifier.sendPublicationMessage(msgToRoute, dest);
	}

	public void notifyFromAggregatorTimer(int timecount) {

		Set<AggregationID> rawAgg = new HashSet<AggregationID>();
		for (MessageDestination outgoinglink : aggregatorMapPerOutgoingLink.keySet()) {
			for (Agg aggregator : aggregatorMapPerOutgoingLink.get(outgoinglink)) {
				getNotificationFromAggregator(aggregator);
				if (!aggregator.aggregate) {
					rawAgg.add(aggregator.getAggregationID());
				}
			}
			synchronized (pubToFWD.get(outgoinglink)) {
				for (Publication p : pubToFWD.get(outgoinglink)) {
					System.out.println("Send as RAW to " + outgoinglink);
					dirtySend(p.duplicate(), outgoinglink, rawAgg);
					// notifier.SendRawPublication(p.duplicate(), outgoinglink);
				}
				pubToFWD.get(outgoinglink).clear();
			}
		}

	}

	public abstract void getNotificationFromAggregator(Agg aggregator);

	@Override
	public void aggregatePublicationMessage() {

		long startTime = System.currentTimeMillis();

		// Add the publicationMessage to each concerned adaptor
		for (MessageDestination md : currentPublicationProfile.getPureAggregateSubDestination()) {
			adaptorMap.get(md).addPubToSample(currentPublicationProfile.getPublicationMessage());
			//System.out.println("[NKDEBUG]5");
		}

		// AggegatePublication are added to the collector directly
		if (currentPublicationProfile.isAggregatedPublication()) {
			AggregationID aggID = ((AggregatedPublication) currentPublicationProfile.getPublication()).getAggregationID();
			Set<MessageDestination> pubDestination = routingTable.getDestinations(aggID, currentPublicationProfile.getPublicationMessage()
					.getLastHopID());
			//System.out.println("[NKDEBUG]6");
			for (MessageDestination out : aggregatorMapPerOutgoingLink.keySet()) {
				for (Agg agg : aggregatorMapPerOutgoingLink.get(out)) {
					if (agg.getAggregationID().equals(aggID) && pubDestination.contains(agg.getDestination())) {
						agg.getCollector().addMsg(currentPublicationProfile.getPublicationMessage());
						//System.out.println("[NKDEBUG]7");
					}else{
						//System.out.println("[NKDEBUG]8");
					}
				}
			}
			return;
		}

		
		for (MessageDestination md : currentPublicationProfile.getPureAggregateSubDestination()) {
			if (adaptorMap.get(md).shouldAggregate()) {
				updateMatchedAggregatorSet();
//				System.out.println("[NKDEBUG]9");
				aggregate(md);
			} else {
				forward(md);
			//	System.out.println("[NKDEBUG]10");
			}

		}
		
		setAggregationRequiredOnlyForTheseLinks(currentPublicationProfile.getOverlappLink());
		Stats.getInstance("test").addValue("TIME.RECEIVE", System.currentTimeMillis() - startTime);
	}

	private void forward(MessageDestination md) {
		// This publication was already sent for a Normal sub for this link
		if (currentPublicationProfile.getOverlappLink().contains(md))
			return;

		Publication p = currentPublicationProfile.getPublication().duplicate();
		logger.info("[AGE]" + this.brokerCore.getBrokerID() + " forwared raw message to destination " + md.toString());
		notifier.SendRawPublication(p, md);

	}

	public abstract void aggregate(MessageDestination md);

	public void updateMatchedAggregatorSet() {
		Set<Agg> aggregatorSet = new HashSet<Agg>();
		for (AggregationID aggregationID : currentPublicationProfile.getMatchedAggregationSet()) {
			aggregatorSet.addAll(aggregatorMapPerAggID.get(aggregationID));
		}

		MessageDestination source = currentPublicationProfile.getPublicationMessage().getLastHopID();

		if (!aggregatorSet.isEmpty()) {
			for (Agg aggregator : aggregatorSet) {
				if (!aggregator.getDestination().equals(source)
						&& routingTable.getDestinations(aggregator.getAggregationID(), new MessageDestination(source.getBrokerId())).contains(
								aggregator.getDestination())) {
					matchedAggregatorMap.get(aggregator.getDestination()).add(aggregator);
				}
			}
		}
	}

	public Object getOutgoingLinks() {
		return aggregatorMapPerOutgoingLink.keySet();
	}

	public Map<MessageDestination, Set<Agg>> getAggregatorMapPerOutgoingLink() {
		return aggregatorMapPerOutgoingLink;
	}

	@Override
	public void processPublicationMessage(PublicationMessage msg, Set<Message> messageSet) {
		// TODO Auto-generated method stub
		super.processPublicationMessage(msg, messageSet);
	}
}
