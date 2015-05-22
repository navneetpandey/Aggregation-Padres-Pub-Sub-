package ca.utoronto.msrg.padres.broker.aggregation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.message.AggregatedPublication;
import ca.utoronto.msrg.padres.broker.aggregation.utility.Stats;
import ca.utoronto.msrg.padres.common.message.Message;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.MessageType;
import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.SubscriptionMessage;

public class PublicationProfile {

	private Set<AggregationID> matchedAggregationIDSet;
	private Set<MessageDestination> overlappLink;
	private Set<MessageDestination> normalSubDestination;
	private List<MessageDestination> aggregateSubDestination;
	private Set<String> subscriptionIDs;
	AbstractAggregationEngine aggregationEngine;
	PublicationMessage publicationMessage;
	boolean aggregatedPublication;
	boolean fromBroker;
	long publicationTime;
	boolean selectivelyDisableAggregationRequired;

	public PublicationProfile(AbstractAggregationEngine aggEngine) {
		matchedAggregationIDSet = null;
		overlappLink = null;
		normalSubDestination = new HashSet<MessageDestination>();
		aggregationEngine = aggEngine;
		selectivelyDisableAggregationRequired=false;
	}

	public void setSelectivelyDisableAggregationRequired() {
		this.selectivelyDisableAggregationRequired = true;
	}

	private void clear() {
		overlappLink = null;
		normalSubDestination.clear();
		aggregatedPublication = false;
		fromBroker = false;
		publicationMessage = null;
		matchedAggregationIDSet = null;
		aggregateSubDestination = null;
		subscriptionIDs = null;
		publicationTime = -1;
		//This is based on aggregation technique so does not require clearing for each publication. 
		//selectivelyDisableAggregationRequired=false;
	}

	
	public void generateProfile(PublicationMessage pubMSG, Set<Message> messageSet) {
		this.clear();
		publicationMessage = pubMSG;

		for (Message m : messageSet) {
			((PublicationMessage) m).setAggregationRequired(false);
			normalSubDestination.add(m.getNextHopID());
		}
		System.out.println("[DEBUG-PUB_PROF]");
		if(normalSubDestination.isEmpty())
			System.out.println("[DEBUG-PUB_EMPTY]");
		if (publicationMessage.getLastHopID().isBroker())
			fromBroker = true;

		publicationTime = publicationMessage.getPublication().getTimeStamp().getTime();
		aggregateSubDestination = null;
		updateStats();
		if(selectivelyDisableAggregationRequired)
			setAggregationRequiredToOverlapLinks(messageSet);
	}

	public void setAggregationRequiredToOverlapLinks(Set<Message> messageSet) {
		Set<MessageDestination> overlappLink = getOverlappLink();
		if (overlappLink != null && !isAggregatedPublication()) {
			for (Message m : messageSet) {
				if (overlappLink.contains(((PublicationMessage) m)
						.getNextHopID())) {
					((PublicationMessage) m).setAggregationRequired(true);
				}
			}
		}
	}
	public long getPublicationTime() {
		return publicationTime;
	}

	public Set<AggregationID> getMatchedAggregationSet() {
		if (matchedAggregationIDSet == null) {
			long startTime = System.nanoTime();
			matchedAggregationIDSet = new HashSet<AggregationID>();
			//System.out.println("[NKDEBUG]A7 ");
			/*
			 * if(publicationMessage==null)return new HashSet<AggregationID>();
			 * // could happen during the init?
			 */if (publicationMessage.getPublication() instanceof AggregatedPublication) {

				matchedAggregationIDSet.add(((AggregatedPublication) publicationMessage.getPublication()).getAggregationID());
				aggregatedPublication = true;
				//System.out.println("[NKDEBUG]A8 ");
			} else {
				matchedAggregationIDSet = aggregationIDMatcher();
				//System.out.println("[NKDEBUG]A9 ");
			}
			Stats.getInstance().addValue("TIME.MatchingNanoSecond", System.nanoTime() - startTime);
		}

		// Removing ID for "other direction"
		Iterator<AggregationID> it = matchedAggregationIDSet.iterator();

		while (it.hasNext()) {
			AggregationID aggregationID = it.next();
			if (aggregationEngine.getRoutingTable().getDestinations(aggregationID, publicationMessage.getLastHopID()) == null) {
				System.out.println("Remove AggID " + aggregationID.aggregationID);
				it.remove();
			}
		}

		return matchedAggregationIDSet;
	}

	
	public void removeFromMatchedAggregationIDSet(AggregationID aggregationID) {
		matchedAggregationIDSet.remove(aggregationID);
		
	}

	
	public Set<MessageDestination> getOverlappLink() {
		if (overlappLink == null) {
			overlappLink = new HashSet<MessageDestination>();
			for (AggregationID aggregationID : getMatchedAggregationSet()) {
				Set<MessageDestination> destinations = aggregationEngine.getRoutingTable().getDestinations(aggregationID,
						publicationMessage.getLastHopID());
				for (MessageDestination messageDestination : destinations) {
					if (normalSubDestination.contains(messageDestination)) {
						overlappLink.add(messageDestination);
					}
				}
			}
		}
		return overlappLink;
	}

	public Set<MessageDestination> getNormalSubDestination() {
		return normalSubDestination;
	}

	protected Set<AggregationID> aggregationIDMatcher() {

		Set<AggregationID> matchedAggregatorSet = new HashSet<AggregationID>();
		// if (!aggregationEngine.getAggregatorMap().isEmpty())
		if (publicationMessage.aggregationRequired()){
			//System.out.println("[NKDEBUG]B1 ");
			matchedAggregatorSet.addAll(findMatchingAggregators());
		}else{
			//System.out.println("[NKDEBUG]B2 ");
		}

		return matchedAggregatorSet;

	}

	protected Set<AggregationID> findMatchingAggregators() {

		Set<AggregationID> requireAggProcessing = new HashSet<AggregationID>();

		Set<String> subIDSet = getMatchingSubscription();
		//System.out.println("[NKDEBUG]B3 " + subIDSet.size());
		subIDSet = removeNonAggregatedSubIDs(subIDSet);
		//System.out.println("[NKDEBUG]B4 " + subIDSet.size());
		requireAggProcessing = findAggregatorsForSubIDSet(subIDSet, publicationMessage.getLastHopID());
		
		for(AggregationID aggregationID: requireAggProcessing) {
			System.out.println("$$$" + "PubID "+ publicationMessage.getPublication().getPubID()
					+ "PubTime " + (publicationTime-(publicationTime%2000))+ " AggregationID " + aggregationID);
		}

		return requireAggProcessing;

	}

	private Set<String> getMatchingSubscription() {
		Set<String> subIDSet = new HashSet<String>();

		Map<PublicationMessage, Set<String>> map_PubPerBroker_SubIDsPerBroker = aggregationEngine.router.getMatcher().getMatchingSubs(
				publicationMessage);

		Collection<Set<String>> subIDSetCollection = map_PubPerBroker_SubIDsPerBroker.values();

		for (Set<String> subIDs : subIDSetCollection)
			subIDSet.addAll(subIDs);

		return subIDSet;
	}

	private Set<String> removeNonAggregatedSubIDs(Set<String> subIDSet) {
		Iterator<String> it = subIDSet.iterator();
		while (it.hasNext()) {
			SubscriptionMessage subMsg = aggregationEngine.router.getSubscriptionMessage(it.next());
			if (!subMsg.getSubscription().isAggregation())
				it.remove();
		}
		return subIDSet;
	}

	private Set<AggregationID> findAggregatorsForSubIDSet(Set<String> subIDSet, MessageDestination sourceLink) {
		Set<AggregationID> matchedAggregtors = new HashSet<AggregationID>();
	//	System.out.println("[NKDEBUG]B5 ");
		for (String subID : subIDSet) {
			matchedAggregtors.add(aggregationEngine.router.getSubscriptionMessage(subID).getSubscription().getAggregationID());
		//	System.out.println("[NKDEBUG]B6 ");
			/*
			 * if (aggregatorSet != null) { for (Aggregator aggregator :
			 * aggregatorSet) { if (aggregator.getAggregationID().equals(
			 * aggregationEngine
			 * .router.getSubscriptionMessage(subID).getSubscription
			 * ().getAggregationID()) &&
			 * aggregator.getSourceLink().getBrokerId()
			 * .equals(sourceLink.getBrokerId()))
			 * matchedAggregtors.add(aggregator); } }
			 */
		}

		return matchedAggregtors;
	}

	private void updateStats() {

		if (isUnmatched()) {
			Stats.getInstance("test").addValue("PUBLICATION.UNMATCHED", 1);
			System.out.println("PUBLICATION.UNMATCHED");
			if (fromBroker) {

				Stats.getInstance("test").addValue("PUBLICATION.UNMATCHEDFROMBROKER", 1);
			} else {
				Stats.getInstance("test").addValue("PUBLICATION.UNMATCHEDFROMPUBLISHER", 1);
			}
		}

		if (!normalSubDestination.isEmpty()) {
			Stats.getInstance().addValue("PUBLICATION.NormalSub", normalSubDestination.size());
			System.out.println("PUBLICATION.NormalSub");

			if (fromBroker) {
				Stats.getInstance("test").addValue("PUBLICATION.INNORMAL", 1);
			} else {
				Stats.getInstance("test").addValue("PUBLICATION.INNORMALFROMPUBLISHER", 1);
			}
		}

		if (!getOverlappLink().isEmpty()) {
			System.out.println("PUBLICATION.Overlapped");
			if (fromBroker) {
				Stats.getInstance().addValue("PUBLICATION.OverlappedFromBroker", 1L);
			} else {
				Stats.getInstance().addValue("PUBLICATION.OverlappedFromPublisher", 1L);
			}
		}

		if (isAggregatedPublication()) {
			Stats.getInstance("test").addValue("PUBLICATION.INAGGREGATED", 1);
			System.out.println("PUBLICATION.INAGGREGATED");
		}

		if (!getMatchedAggregationSet().isEmpty()) {
			Stats.getInstance("test").addValue("PUBLICATION.INAAGGREGATION", 1);
			System.out.println("PUBLICATION.INAAGGREGATION");
			if (fromBroker) {
				Stats.getInstance("test").addValue("PUBLICATION.INFORAGGREGATIONFROMBROKER", 1);
			} else {

				Stats.getInstance("test").addValue("PUBLICATION.INFORAGGREGATIONFROMPUBLISHER", 1);
			}

		}

	}

	// gives destination of aggregate publication
	private List<MessageDestination> getAggregatePublicationDestinations() {
		List<MessageDestination> destinations;
		AggregationID aggregationID = ((AggregatedPublication) publicationMessage.getPublication()).getAggregationID();

		destinations = new ArrayList<MessageDestination>();

		Set<MessageDestination> set = aggregationEngine.getRoutingTable().getDestinations(aggregationID, publicationMessage.getLastHopID());
		if (set != null)
			destinations.addAll(set);
		return destinations;
	}

	public Set<String> getSubscriptionID() {
		if (subscriptionIDs == null) {
			getRawMessageDestination();
		}
		return subscriptionIDs;
	}

	// gives destination of publication (not aggregate publication) matching
	// aggregate subscription
	private List<MessageDestination> getRawMessageDestination() {

		ArrayList<MessageDestination> pureAggregate = new ArrayList<MessageDestination>();

		Map<PublicationMessage, Set<String>> pubMsgsToSubIDs = aggregationEngine.router.getMatcher().getMatchingSubs(publicationMessage);

		Set<MessageDestination> overlappedDesitations = new HashSet<MessageDestination>();
		Set<String> subIDtemp = new HashSet<String>();
		
		Set<String> aggSubIDs = new HashSet<String>();
		//per outgoing broker loop
		for (PublicationMessage pubMsg : pubMsgsToSubIDs.keySet()) {
			subIDtemp = pubMsgsToSubIDs.get(pubMsg);

			if (removeNormalSubID(subIDtemp))
				// BrokerContainNormalSubscription.add(pubMsg.getNextHopID());
				//If there is normal subscription as well as aggregate subscription
				for (String subID : subIDtemp)
					overlappedDesitations.add(aggregationEngine.router.getMessageDestination(subID, MessageType.SUBSCRIPTION));
			aggSubIDs.addAll(subIDtemp);

		}
		subscriptionIDs = new HashSet<String>();
		for (String subID : aggSubIDs) {
			MessageDestination dest = aggregationEngine.router.getMessageDestination(subID, MessageType.SUBSCRIPTION);

			if (!overlappedDesitations.contains(dest) && !dest.equals(publicationMessage.getLastHopID())) {
				pureAggregate.add(dest);
				overlappedDesitations.add(dest);
				subscriptionIDs.add(subID);
			}

		}
		return pureAggregate;
	}
	private Set<MessageDestination> getAllAggregateSubDestination() {
 
		Set<MessageDestination> destinations = new HashSet<MessageDestination>();
			for (AggregationID aggregationID : getMatchedAggregationSet()) {
				destinations.addAll( aggregationEngine.getRoutingTable().getDestinations(aggregationID,
						publicationMessage.getLastHopID())); 
			//	System.out.println("[NKDEBUG]A6 ");
			}
 
		return destinations;
	}

	private boolean removeNormalSubID(Set<String> subIDs) {
		boolean removed = false;
		Iterator<String> it = subIDs.iterator();
		while (it.hasNext()) {
			SubscriptionMessage subMsg = aggregationEngine.router.getSubscriptionMessage(it.next());
			if (!subMsg.getSubscription().isAggregation()) {
				it.remove();
				removed = true;
			}
		}
		return removed;
	}

	public boolean isFromBroker() {
		return fromBroker;
	}

	public boolean isAggregatedPublication() {
		if(matchedAggregationIDSet==null)getMatchedAggregationSet();
		return aggregatedPublication;
	}

	public PublicationMessage getPublicationMessage() {
		if (isAggregatedPublication()) {

		}
		return publicationMessage;
	}

	public Publication getPublication() {
		return publicationMessage.getPublication();
	}

	public boolean isUnmatched() {
		return getMatchedAggregationSet().isEmpty() && normalSubDestination.isEmpty();
	}

	public List<MessageDestination> getPureAggregateSubDestination() {
		if (aggregateSubDestination == null) {
			if (isAggregatedPublication()) {
				//System.out.println("[NKDEBUG]A1");
				aggregateSubDestination = getAggregatePublicationDestinations();
				//System.out.println("[NKDEBUG]A4 "+aggregateSubDestination.size());
			} else {
				
				aggregateSubDestination = new ArrayList<MessageDestination>();
				aggregateSubDestination.addAll(getAllAggregateSubDestination());
				//System.out.println("[NKDEBUG]A2 "+aggregateSubDestination.size());
				aggregateSubDestination.removeAll(getOverlappLink());
				//System.out.println("[NKDEBUG]A3 "+aggregateSubDestination.size());
			}
		}else{
			//System.out.println("[NKDEBUG]B9 ");
		}
		return aggregateSubDestination;
	}

	

	/*
	 * // ONLY FOR TEST public void
	 * setMatchedAggregationIDSet(Set<AggregationID> matchedAggregationIDSet) {
	 * this.matchedAggregationIDSet = matchedAggregationIDSet; }
	 */

	// ONLY FOR TEST
	public void setPublicationMessage(PublicationMessage publicationMessage) {
		this.publicationMessage = publicationMessage;
		if (publicationMessage.getPublication() instanceof AggregatedPublication)
			aggregatedPublication = true;
		else
			aggregatedPublication = false;
		this.publicationTime = publicationMessage.getPublication().getTimeStamp().getTime();
	}

}
