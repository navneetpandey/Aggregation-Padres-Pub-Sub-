package ca.utoronto.msrg.padres.broker.aggregation.collector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import ca.utoronto.msrg.padres.common.message.MessageDestination;

public class CollectorSchedulerEntry {

	Set<MessageDestination> brokerIDsSet;
	long expireTime;
	ArrayList<String> outputValues;

	public CollectorSchedulerEntry(MessageDestination incomingBrokerID, long expireTime,
			String outputValue) {
		super();
		this.brokerIDsSet = new HashSet<MessageDestination>();
		this.outputValues = new ArrayList<String>();
		this.expireTime = expireTime;

		this.brokerIDsSet.add(incomingBrokerID);
		this.outputValues.add(outputValue);
	}

	public int getNbBrokerID() {
		return brokerIDsSet.size();
	}

	public void add(MessageDestination brokerID, String outputValue) {
		brokerIDsSet.add(brokerID);
		outputValues.add(outputValue);
	}

	public long getExpireTime() {
		return expireTime;
	}

	public ArrayList<String> getOutputValues() {
		return outputValues;
	}

	// No value, i.e., used for notification when a raw message is received
	public void add(MessageDestination brokerID) {
		brokerIDsSet.add(brokerID);

	}

}
