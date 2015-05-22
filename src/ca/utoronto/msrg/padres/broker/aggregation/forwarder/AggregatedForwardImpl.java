package ca.utoronto.msrg.padres.broker.aggregation.forwarder;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationMode;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.broker.router.ForwarderImpl;
import ca.utoronto.msrg.padres.broker.router.Router;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.SubscriptionMessage;

public class AggregatedForwardImpl extends ForwarderImpl {
	private Router router;

	private BrokerCore brokerCore;


	static Logger routerLogger = Logger.getLogger(Router.class);

	static Logger exceptionLogger = Logger.getLogger("Exception");

	static Logger msgPathLogger = Logger.getLogger("MessagePath");

	public AggregatedForwardImpl(Router router, BrokerCore broker) {
		super(router, broker);
		this.router = router;
		this.brokerCore = broker;
	}

	public Set<PublicationMessage> forwardPubMsgs(
			Map<PublicationMessage, Set<String>> pubMsgsToSubIDs) {
		Set<PublicationMessage> messagesToRoute = new HashSet<PublicationMessage>();
		
		for (PublicationMessage pubMsg : pubMsgsToSubIDs.keySet()) {
			routerLogger.debug("forwarding in " + brokerCore.getBrokerID()
					+ ": " + pubMsg);
			msgPathLogger.info("forwarding in " + brokerCore.getBrokerID()
					+ ": " + pubMsg);
			Set<String> subIDs = pubMsgsToSubIDs.get(pubMsg);
            
		//	if(router.getPostProcessor().getAggregationEngine().getAdaptationEngine().getSwitchStatus() == AggregationMode.AGG_MODE)
				removeAggregatedSubID(subIDs);

			Set<PublicationMessage> messagesToAdd = forwardPubMsg(pubMsg,
					subIDs);
			if(!messagesToAdd.isEmpty())
				messagesToRoute.addAll(messagesToAdd);
		}

		return messagesToRoute;
	}

	private void removeAggregatedSubID(Set<String> subIDs) {
		Iterator<String> it = subIDs.iterator();
		while (it.hasNext()) {
			SubscriptionMessage subMsg = router.getSubscriptionMessage(it
					.next());
			if (subMsg.getSubscription().isAggregation())
				it.remove();
		}

	}

}
