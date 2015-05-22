package ca.utoronto.msrg.padres.demo.selectivityComputer;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.broker.router.ReteRouter;
import ca.utoronto.msrg.padres.broker.router.matching.PubMsgNotConformedException;
import ca.utoronto.msrg.padres.common.message.AdvertisementMessage;
import ca.utoronto.msrg.padres.common.message.Message;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;

public class AggregatorRouter extends ReteRouter {

	public AggregatorRouter(BrokerCore broker) {
		super(broker);
	}

	protected Set<Message> handleMessage(PublicationMessage pubMsg)
			throws PubMsgNotConformedException {

		pubMsg = prePartOfPublicationMessageHandler(pubMsg);

		// process it through the matcher
		Map<PublicationMessage, Set<String>> pubMsgToSubIDs = matcher
				.getMatchingSubs(pubMsg);
		routerLogger.debug("got " + pubMsgToSubIDs.size()
				+ " matching subs for " + pubMsg);

		return postPartOfPublicationMessagehandler(pubMsgToSubIDs, pubMsg);

	}

	protected PublicationMessage prePartOfPublicationMessageHandler(
			PublicationMessage pubMsg) throws PubMsgNotConformedException {
		if (brokerCore.getBrokerConfig().isPubConformCheck()) {
			// checking whether the publication is a special one.
			boolean specialPub = false;
			MessageDestination lasthop = pubMsg.getLastHopID();
			// For network construction, publications of BROKER_CTL,
			// BROKER_MONITOR,
			// HEARTBEAT_MANAGER and NETWORK_DISCOVERY should be conformed by
			// advertisement
			String msgClass = pubMsg.getPublication().getClassVal();
			if (msgClass.equals("BROKER_CONTROL")
					|| msgClass.equals("BROKER_MONITOR")
					|| msgClass.equals("TRACEROUTE_MESSAGE")
					|| msgClass.equals("HEARTBEAT_MANAGER")
					|| msgClass.equals("NETWORK_DISCOVERY")
					|| msgClass.equals("BROKER_INFO")) {
				if (lasthop.isBroker() && !lasthop.isInternalQueue()
						&& !lasthop.equals(brokerCore.getBrokerDestination())
						&& !msgClass.equals("BROKER_CONTROL")) {
					specialPub = false;
				} else {
					specialPub = true;
				}
			}
			// verify against the given advertisement if it is not a special
			// case
			if (!lasthop.isBroker()
					|| lasthop.getDestinationID().equals("none") || specialPub) {
				Set<String> matchedAdvs = matcher.getMatchingAdvs(pubMsg);
				// get the matching advertisement came from the same origin as
				// the publication
				AdvertisementMessage matchingAdvMsg = null;
				if (matchedAdvs != null) {
					for (String advID : matchedAdvs) {
						AdvertisementMessage matchedAdvMsg = workingAdvs
								.get(advID);
						if (matchedAdvMsg.getLastHopID().equals(lasthop)) {
							matchingAdvMsg = matchedAdvMsg;
							break;
						}
					}
				}
				// if nothing matched, throw an exception
				if (matchingAdvMsg == null) {
					routerLogger
							.error("You did not advertise correctly before you published : "
									+ pubMsg);
					throw new PubMsgNotConformedException(
							"You did not advertise correctly before you published : "
									+ pubMsg);
				}
				// set the publication's TID to the matching advertisement's.
				Map<String, Serializable> pubPairMap = pubMsg.getPublication()
						.getPairMap();
				if (pubPairMap.containsKey("tid")
						&& pubPairMap.get("tid").toString().startsWith("$S$")) {
					String advTID = (String) matchingAdvMsg.getAdvertisement()
							.getPredicateMap().get("tid").getValue();
					pubPairMap.put("tid", advTID);
				}
			}
		}
		// add publication message to the working memory
		workingPubs.put(pubMsg.getMessageID(), pubMsg);
		return pubMsg;
	}

	protected Set<Message> postPartOfPublicationMessagehandler(
			Map<PublicationMessage, Set<String>> pubMsgToSubIDs,
			PublicationMessage pubMsg) {

		Set<Message> messagesToRoute = new HashSet<Message>();
		messagesToRoute.addAll(forwarder.forwardPubMsgs(pubMsgToSubIDs));

		// remove the publication messages from the working memory, if possible
		if (!matcher.isPartialMatch())
			workingPubs.remove(pubMsg.getMessageID());
		return messagesToRoute;
	}

}
