package ca.utoronto.msrg.padres.broker.aggregation.utility;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.common.message.MessageDestination;

public class RoutingTable {

	protected HashMap<MessageDestination, HashSet<MessageDestination>> table = new HashMap<MessageDestination, HashSet<MessageDestination>>();

	// protected BrokerCore brokerCore;

	public RoutingTable() {
		super();
		// this.brokerCore = brokerCore;
	}

	public void addRoute(MessageDestination brokerIDSource, MessageDestination brokerIDDestination) {
		HashSet<MessageDestination> dest;
		if ((dest = table.get(brokerIDSource)) == null) {
			dest = new HashSet<MessageDestination>();
			table.put(brokerIDSource, dest);
		}
		dest.add(brokerIDDestination);
		System.out.println(this);
	}

	public Set<MessageDestination> getDestinations(MessageDestination messageDestination) {
		return table.get(messageDestination);
	}

	public boolean contains(MessageDestination outBroker) {
		return table.containsKey(outBroker);
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("[RoutingTable]" + "\n\t From=>TO");
		for (MessageDestination msgDstKey : table.keySet()) {
			sb.append("\n" + msgDstKey.toString() + " =>");
			for (MessageDestination msgDstVal : table.get(msgDstKey)) {
				sb.append("\t" + msgDstVal.toString());
			}

		}
		sb.append("\nEndofTable\n");
		return sb.toString();

	}

}
