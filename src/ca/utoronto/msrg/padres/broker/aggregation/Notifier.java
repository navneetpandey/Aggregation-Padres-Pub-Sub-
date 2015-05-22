package ca.utoronto.msrg.padres.broker.aggregation;

import org.apache.log4j.Logger;

import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.message.AggregatedPublication;
import ca.utoronto.msrg.padres.broker.aggregation.utility.Stats;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.parser.MessageFactory;
import ca.utoronto.msrg.padres.common.message.parser.ParseException;

public class Notifier {

	BrokerCore brokerCore;
	Logger logger;

	public Notifier(BrokerCore brokerCore) {
		super();
		this.brokerCore = brokerCore;
		logger = Logger.getLogger(Notifier.class);
	}

	public BrokerCore getBrokerCore() {
		return this.brokerCore;
	}

	public Publication createPublication(AggregationID aggregationID,
			String value, String windowID, boolean forClient) {

		AggregatedPublication newPub = null;
		try {
			newPub = new AggregatedPublication(
					MessageFactory
							.createPublicationFromString("[class,'Notification']"),
					aggregationID);
			newPub.setAggregation(true, aggregationID.getOperatorType(),
					aggregationID.getFieldName(), value);
			newPub.setWindowID(windowID);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return newPub;
	}

	public void SendPublication(Publication publication,
			MessageDestination destBroker) {
		PublicationMessage msgToRoute = new PublicationMessage(publication);
		Stats.getInstance("test").addValue("PUBLICATION.AGGREGATE", 1);
		if (!destBroker.isBroker()
				&& publication instanceof AggregatedPublication) {
			Stats.getInstance().addValue("Delay.Number", 1);
			long delay = System.currentTimeMillis()
					- Long.parseLong(((AggregatedPublication) publication)
							.getWindowID());
			System.out.println("Delay.Time " + delay + Long.parseLong(((AggregatedPublication) publication)
							.getWindowID()) + ((AggregatedPublication) publication)
							.getWindowID());
			Stats.getInstance().addValue("Delay.Time", delay);
		}
		sendPublicationMessage(msgToRoute, destBroker);
	}

	void sendPublicationMessage(PublicationMessage msgToRoute,
			MessageDestination destBroker) {
		if (msgToRoute.getMessageID().equals(""))
			msgToRoute.setMessageID(brokerCore.getNewMessageID());
		msgToRoute.setNextHopID(destBroker);
		msgToRoute.setLastHopID(brokerCore.getBrokerDestination());
		System.out.println("[NTF-" + this.brokerCore.getBrokerID()
				+ "-]SendPublication=>"
				+ brokerCore.getBrokerDestination().getBrokerId() + " to "
				+ destBroker.getBrokerId() + " pred: "
				+ msgToRoute.getPublication().getPairMap().toString() + " || "
				+ System.currentTimeMillis() + " ||");
		
		brokerCore.routeMessage(msgToRoute);
	}

	

	public void SendRawPublication(Publication p, MessageDestination dest) {
		PublicationMessage msgToRoute = new PublicationMessage(p);
		msgToRoute.setAggregationRequired(true);
		if (msgToRoute.getMessageID().equals(""))
			msgToRoute.setMessageID(brokerCore.getNewMessageID());
		msgToRoute.setNextHopID(dest);
		msgToRoute.setLastHopID(brokerCore.getBrokerDestination());

		/*if (brokerCore.getBrokerConfig().getAggregationInfo()
				.isClientAggregation()
				&& !dest.isBroker()) {

			brokerCore.getRouter().getPostProcessor().getAggregationEngine()
					.processPublicationForClient(msgToRoute);
			return;

		}*/
		sendPublicationMessage(msgToRoute, dest);
		logger.info("[NTF]" + this.brokerCore.getBrokerID()
				+ " forwared raw message" + msgToRoute.toString()
				+ " to destination " + dest.toString());
		Stats.getInstance("test").addValue("PUBLICATION.REGULAR", 1);

	}

}
