package ca.utoronto.msrg.padres.broker.aggregation.collector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import ca.utoronto.msrg.padres.broker.aggregation.AbstractAggregationEngine;
import ca.utoronto.msrg.padres.broker.aggregation.Notifier;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregateComputer;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationType;
import ca.utoronto.msrg.padres.broker.aggregation.message.AggregatedPublication;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;

public abstract class AbstractCollector {

	AbstractAggregationEngine aggregationEngine;
	
	AggregateComputer aggregateComputer;
	

	Notifier notifier;

	CollectorScheduler scheduler;

	AggregationID aggregationID;

	long maxDelay;
	long minDelay;
	
	public static final long MAX_DELAY = 100;//1000;

	// Constructor
	public AbstractCollector(Notifier notifier, AggregationID aggregationID,
			long waitingTime, AbstractAggregationEngine aggregationEngine) {
		this.notifier = notifier;
		//this.fromBroker = fromBroker;
		this.aggregationID = aggregationID;
		this.aggregateComputer = new AggregateComputer(
				aggregationID.getOperatorType(), AggregationType.TIMEBASED);
		scheduler = new CollectorScheduler(this, waitingTime);
		maxDelay = 2 * waitingTime;
		minDelay = waitingTime / 2;
		this.aggregationEngine=aggregationEngine;
	}

	public void normalMsgObserved(MessageDestination brokerID, String windowID) {
		scheduler.notifyScheduler(brokerID, windowID);
	}

	protected void updateTimeout(Publication p) {
		long diffTime = System.currentTimeMillis()
				- Long.parseLong(((AggregatedPublication) p).getWindowID());
		double avg = (maxDelay + minDelay) / 2.0;
		long timeout;
		if (diffTime > avg) {
			if (diffTime > maxDelay)
				maxDelay = (maxDelay + diffTime) / 2;
		} else {
			if (diffTime < minDelay)
				minDelay = (minDelay + diffTime) / 2;
		}
		timeout=2 * (maxDelay - minDelay);
		scheduler.setTimeout(Math.min(timeout, MAX_DELAY));
		//scheduler.setTimeout( MAX_DELAY);
		//System.err.println("----- Diff " + diffTime);
	}

	public void addMsg(PublicationMessage msg) {
		AggregatedPublication p = (AggregatedPublication) msg.getPublication();
		updateTimeout(p);
		if (p.getWindowID() != null) {
		
			if (notifier.getBrokerCore() != null)
				System.out.println("[CLR-"
						+ notifier.getBrokerCore().getBrokerID() + "]addMsg=>"
						+ "going to check outgoing link Collector Timeout "
						+ scheduler.timeout + " || "
						+ System.currentTimeMillis() + " ||");
			else{
				System.err.println("FATAL: AbstractCollector.addMsg: notifier.getBrokerCore() is null");
				System.exit(1);
			}
				

		}else{
			System.err.println("FATAL: AbstractCollector.addMsg: windowID is null");
			System.exit(1);
		}
	}

	public String collectResults(OutgoingWindow collectorSchedulerKey) {
		ArrayList<String> value = scheduler.getValues(collectorSchedulerKey);
		if (value != null && value.size() > 0) {
			for (String pub : value) {
				aggregateComputer.aggregateComputation(pub);
			}

			return aggregateComputer.getResultString(false); // don't care for
			// client
		}
		return null;
	}

	public Set<OutgoingWindow> getExpiredMessage() {
		Set<OutgoingWindow> result = new HashSet<OutgoingWindow>();
		Set<OutgoingWindow> expiredWindows = scheduler.getExpiredWindows();
		/*System.out.println("[AbsCLR-"
				+ notifier.getBrokerCore().getBrokerID() + "]getExpiredMessage=>" + expiredWindows.size()) ;*/
		for (OutgoingWindow expiredItem : expiredWindows) {

			result.add(expiredItem);
		}
		return result;
	}

	/*public boolean sameSource(String brokerId) {
		return this.fromBroker.equals(brokerId);
	}*/

	public Publication createPublication(OutgoingWindow collectorSchedulerKey) {
		String results = collectResults(collectorSchedulerKey);
		if (results != null) {
			return notifier.createPublication(aggregationID, results, ""
					+ collectorSchedulerKey.getWindowID(), false);
		}
		return null;
	}

	// get the number of current windows
	public int size() {
		return scheduler.size();
	}

	public AggregateComputer getAggregateComputer() {
		return aggregateComputer;
	}

	public abstract void send(OutgoingWindow collectorSchedulerKey) ;


	public void sendExpiredMessage() {
		Set<OutgoingWindow> expiredItem = getExpiredMessage();
		
		for (OutgoingWindow item : expiredItem) {
			System.out.println(" OutgoingWindow "+item);
			send(item);
		}
		scheduler.purgeExpired();
	}

	public AggregationID getAggregationID() {
		return aggregationID;
	}

	public Notifier getNotifier() {
		return notifier;
	}
	
	public abstract boolean isWindowComplete(boolean schedulerContains, int expectingBrokerSize, OutgoingWindow outgoingWindow);
}
