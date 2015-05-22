package ca.utoronto.msrg.padres.broker.aggregation.collector;

import java.util.Set;

import ca.utoronto.msrg.padres.broker.aggregation.AbstractAggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.CollectionBasedAggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.Notifier;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.message.AggregatedPublication;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;

public class CollectorAtBroker  extends AbstractCollector{



	public CollectorAtBroker(Notifier notifier, AggregationID aggregationID,
			long waitingTime, AbstractAggregationEngine aggregationEngine) {
		super(notifier, aggregationID, waitingTime, aggregationEngine);

	}

	
	public void addMsg(PublicationMessage msg) {
		super.addMsg(msg);
		long expireTime;
		AggregatedPublication p = (AggregatedPublication) msg.getPublication();
		
		if (p.getWindowID() != null) {
			Set<MessageDestination> outlinks = aggregationEngine.getRoutingTable()
					.getDestinations(aggregationID,msg.getLastHopID());
	
			if(outlinks.isEmpty())System.err.println("\nNo destination for this aggPub");
			
			for (MessageDestination dest : outlinks) {
				expireTime=scheduler.updateScheduler(msg.getLastHopID(),
						new OutgoingWindow(p.getWindowID(), dest),
						p.getAggResult());
				// -1 means that the window is complete, and the scheduler will send the message itself right now 
				if(expireTime!=-1){
					((CollectionBasedAggregationEngine)aggregationEngine).getCollectorTimerService().addPubInCollector(this, expireTime);
				}
			}
		}
		
	}
 
  

	@Override
	public boolean isWindowComplete(boolean schedulerContains, int expectingBrokerSize, OutgoingWindow outgoingWindow){
		if (!schedulerContains
				|| !aggregationEngine.getRoutingTable().hasDestination(aggregationID, outgoingWindow
						.getOutBroker()))
			return false;
		if (expectingBrokerSize == aggregationEngine.getRoutingTable().getSources(aggregationID, outgoingWindow.getOutBroker()).size())
			return true;
		System.out.println("[CLR"
				+ getNotifier().getBrokerCore().getBrokerID()
				+ "]isWindowComplete=> "
				+ "for window ID "
				+ outgoingWindow.getWindowID()
				+ " "
				+ expectingBrokerSize
				+ "/"
				+ aggregationEngine.getRoutingTable().getSources(aggregationID, outgoingWindow.getOutBroker()));
		return false;
	}

	public synchronized void send(OutgoingWindow collectorSchedulerKey) {
		Publication p = createPublication(collectorSchedulerKey);
		if (p == null) {
			System.out.println("[CLR-" + notifier.getBrokerCore().getBrokerID()
					+ "]send=> NULL Publication ");
			return;
		}
		notifier.SendPublication(p, collectorSchedulerKey.getOutBroker());
		System.out.println("[CLR-" + notifier.getBrokerCore().getBrokerID()
				+ "]send to " + collectorSchedulerKey.getOutBroker() + "=>"
				+ "Pub Map " + p.getPairMap().toString());
		aggregateComputer.reset();
	}

}
