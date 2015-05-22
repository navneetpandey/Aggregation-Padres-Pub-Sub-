package ca.utoronto.msrg.padres.broker.aggregation.collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import ca.utoronto.msrg.padres.broker.aggregation.utility.Stats;
import ca.utoronto.msrg.padres.common.message.MessageDestination;

public class CollectorScheduler {

	long timeout;
	AbstractCollector collector;
	private HashMap<OutgoingWindow, CollectorSchedulerEntry> schedulerList;
	private long purgeTime = 0;

	public CollectorScheduler(AbstractCollector abstractCollector, long timeout) {
		super();
		this.timeout = timeout;
		this.collector = abstractCollector;
		schedulerList = new HashMap<OutgoingWindow, CollectorSchedulerEntry>();
	}

	// Notify the scheduler that one broker is sending raw message (RAW_MODE)
	public void notifyScheduler(MessageDestination brokerID, String windowID) {
		if (schedulerList.containsKey(windowID)) {
			schedulerList.get(windowID).add(brokerID);
		}
	}

	// update Scheduler information when receiving an Aggregated Messages
	public synchronized long updateScheduler(MessageDestination incomingBrokerID,
			OutgoingWindow outgoingWindow, String value) {
		long expireTime;
		
		if (!schedulerList.containsKey(outgoingWindow)) {
			expireTime = create(incomingBrokerID, outgoingWindow, value);
		} else {
			add(incomingBrokerID, outgoingWindow, value);
			expireTime = -2;
		}
		
		if(collector.getNotifier().getBrokerCore()!= null)
			System.out.println("[CLR_SCH-"
				+ collector.getNotifier().getBrokerCore().getBrokerID()
				+ "]updateScheduler=>" + " incomingBrokerID "
				+ incomingBrokerID + " outgoingWindowID "
				+ outgoingWindow.getWindowID() + " Value " + value + "expire time " + expireTime);
		

		if (isWindowComplete(outgoingWindow)) {
			if(collector.getNotifier().getBrokerCore()!= null)
			System.out.println("[CLR_SCH-"
					+ collector.getNotifier().getBrokerCore().getBrokerID()
					+ "]updateScheduler=> " + " Window Complete for window ID "
					+ outgoingWindow.getWindowID());
			else
				System.out.println("[CLR_SCH-"
						+ "XXX WITH BROKER NULL" 
						+ "]updateScheduler=> " + " Window Complete for window ID "
						+ outgoingWindow.getWindowID());
			collector.send(outgoingWindow);
			remove(outgoingWindow);
			expireTime = -1;
		}
		else System.out.println(" Waiting for "+schedulerList.get(outgoingWindow).getNbBrokerID()+" brokers" );
		return expireTime;

	}

	private boolean isWindowComplete(OutgoingWindow outgoingWindow) {
		boolean schedulerContains = schedulerList.containsKey(outgoingWindow);
		if(!schedulerContains)return false;
		int expectingBrokerSize = schedulerList.get(outgoingWindow).getNbBrokerID();
		return collector.isWindowComplete(schedulerContains, expectingBrokerSize, outgoingWindow);
				
	}

	private synchronized long add(MessageDestination incomingBrokerID,
			OutgoingWindow outgoingWindow, String value) {
		schedulerList.get(outgoingWindow).getOutputValues().add(value);
		schedulerList.get(outgoingWindow).brokerIDsSet.add(incomingBrokerID);
		return schedulerList.get(outgoingWindow).getExpireTime();
	}

	private synchronized long create(MessageDestination incomingBrokerID,
			OutgoingWindow outgoingWindow, String value) {
		long expireTime = System.currentTimeMillis() + timeout;
		CollectorSchedulerEntry entry = new CollectorSchedulerEntry(
				incomingBrokerID, expireTime, value);
		schedulerList.put(outgoingWindow, entry);
		return expireTime;
	}

	private synchronized void remove(OutgoingWindow outgoingWindow) {
		schedulerList.remove(outgoingWindow);
		Stats.getInstance("");Stats.getInstance("test").addValue("Queue.SizeAtCollector", schedulerList.size() );
	}

	public synchronized Set<OutgoingWindow> getExpiredWindows() {
		HashSet<OutgoingWindow> result = new HashSet<OutgoingWindow>();

		Iterator<OutgoingWindow> it = schedulerList.keySet().iterator();
		while (it.hasNext()) {
			purgeTime = System.currentTimeMillis();

			OutgoingWindow outgoingWindow = it.next();
			CollectorSchedulerEntry e = schedulerList.get(outgoingWindow);
			if (e.getExpireTime() < purgeTime) {
				System.out.println("[CLR_SCH-"
						+ collector.getNotifier().getBrokerCore().getBrokerID()
						+ "]getExpierdWindows=> EXPIRED outgoing Window ID " + e.getExpireTime()
						+ " windowID " +outgoingWindow.getWindowID());
				result.add(outgoingWindow);
			} else
				System.out.println("[CLR_SCH-"
						+ collector.getNotifier().getBrokerCore().getBrokerID()
						+ "]getExpierdWindows=> NOT EXPIRED outgoing Window ID " + e.getExpireTime()
						+ " windowID " + outgoingWindow.getWindowID());
			
		}

		return result;
	}

	public synchronized void purgeExpired() {
		synchronized (schedulerList) {
			Iterator<CollectorSchedulerEntry> it = schedulerList.values()
					.iterator();
			while (it.hasNext()) {
				CollectorSchedulerEntry e = it.next();
				if (e.getExpireTime() < purgeTime) {
					it.remove();
				}
			}
		}
	}

	public synchronized ArrayList<String> getValues(OutgoingWindow outgoingWindow) {
		CollectorSchedulerEntry e = schedulerList.get(outgoingWindow);

		if (e != null)
			return e.getOutputValues();
		return null;
	}

	public int size() {
		return schedulerList.size();
	}

	public void setTimeout(long l) {
		timeout = l;
	}

}
