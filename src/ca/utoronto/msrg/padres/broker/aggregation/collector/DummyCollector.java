package ca.utoronto.msrg.padres.broker.aggregation.collector;

import ca.utoronto.msrg.padres.broker.aggregation.AbstractAggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.Notifier;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.message.AggregatedPublication;
import ca.utoronto.msrg.padres.broker.aggregation.utility.AggregationTimerService;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;

public class DummyCollector extends AbstractCollector {

	MessageDestination nextHopID;

	AggregationTimerService aggregationTimerService;
	
	
	public static class dummyCollectorFactory implements CollectorFactory<DummyCollector>{
		@Override
		public DummyCollector createCollector(Notifier notifier, AggregationID aggregationID, long waitingTime, 
				AbstractAggregationEngine abstractAggregationEngine) {
			return new DummyCollector(notifier, aggregationID, waitingTime, abstractAggregationEngine.getAggregatorTimerService(), abstractAggregationEngine);
		}
		
	}
	
	public void setDestination(MessageDestination dest){
		this.nextHopID=dest;
	}
	
	
	
	public DummyCollector(Notifier notifier, AggregationID aggregationID,
			long waitingTime,  AggregationTimerService aggregationTimerService,  AbstractAggregationEngine abstractAggregationEngine) {
		super(notifier, aggregationID, waitingTime, abstractAggregationEngine);
		this.aggregationTimerService = aggregationTimerService;
	}

	public boolean isWindowComplete(boolean schedulerContains,
			int expectingBrokerSize, OutgoingWindow outgoingWindow) {

		if (!schedulerContains)
			return false;

		// TODO make wait for while forcefully
		return false;
		// return true;
	}

	public void addMsg(PublicationMessage msg) {
		super.addMsg(msg);
		AggregatedPublication p = (AggregatedPublication) msg.getPublication();
		
		long expireTime = scheduler.updateScheduler(msg.getLastHopID(),
				new OutgoingWindow(p.getWindowID(), nextHopID),
				p.getAggResult());
		System.out.println("WINID "+p.getWindowID() );
		if(expireTime==0)
			System.out.println("[Client DCLR-"
				+ "]addPublication " + p.toString());
		aggregationTimerService.addPubInCollector(this,expireTime);
	}

	@Override
	public synchronized void send(OutgoingWindow collectorSchedulerKey) {
		Publication p = createPublication(collectorSchedulerKey);
		if (p == null) {
			System.out.println("[Client DCLR-"
					+ "]send from Client=> NULL Publication ");
			return;
		}
		notifier.SendPublication(p, collectorSchedulerKey.getOutBroker());
		System.out.println("[Client DCLR-"
				+ "]send from Client=>" + p.toString());
		aggregateComputer.reset();
	}

	@Override
	protected void updateTimeout(Publication p) {
		// TODO Auto-generated method stub

	}

	
	
}