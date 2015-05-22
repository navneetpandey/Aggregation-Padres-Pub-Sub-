package ca.utoronto.msrg.padres.broker.aggregation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.utility.RoutingTable;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.SubscriptionMessage;

public class AggregationRoutingTable {

	protected HashMap<AggregationID, RoutingTable> routingTableMap;
	protected HashMap<AggregationID, RoutingTable> reverseRoutingTableMap;

	AbstractAggregationEngine aggregationEngine;

	public AggregationRoutingTable(AbstractAggregationEngine abstractAggregationEngine) {
		this.aggregationEngine = abstractAggregationEngine;
		routingTableMap=new HashMap<AggregationID, RoutingTable>();
		reverseRoutingTableMap=new HashMap<AggregationID, RoutingTable>();
	}

	public void registerSubscription(SubscriptionMessage subMsg) {
		if (!subMsg.getSubscription().isAggregation()) {
			System.err.println("AggregationRoutingTable trying to register a normal subscription!");
			return;
		}

		MessageDestination nextHopID = subMsg.getNextHopID();
		
		if (nextHopID.getBrokerId().equals("INPUTQUEUE")) {
			nextHopID = aggregationEngine.brokerCore.getBrokerDestination();
			
			//throw new NullPointerException();
		}

		AggregationID aggregationID = subMsg.getSubscription().getAggregationID();

		RoutingTable routingTable = routingTableMap.get(aggregationID);
		RoutingTable reversedRoutingTable = reverseRoutingTableMap.get(aggregationID);

		if (routingTable == null) {
			routingTable = new RoutingTable();
			reversedRoutingTable = new RoutingTable();
			routingTableMap.put(subMsg.getSubscription().getAggregationID(), routingTable);
			reverseRoutingTableMap.put(subMsg.getSubscription().getAggregationID(), reversedRoutingTable);
		}

		System.out.println("BROKER "+aggregationEngine.router.getBrokerId());
		routingTable.addRoute(nextHopID, subMsg.getLastHopID());
		reversedRoutingTable.addRoute(subMsg.getLastHopID(), nextHopID);

	}

	public Set<MessageDestination> getDestinations(AggregationID aggregationID, MessageDestination incoming) {
		if(routingTableMap.get(aggregationID)==null)return new HashSet<MessageDestination>();
		return routingTableMap.get(aggregationID).getDestinations(new MessageDestination(incoming.getBrokerId()));
	}

	public Set<MessageDestination> getSources(AggregationID aggregationID, MessageDestination incoming) {
		return reverseRoutingTableMap.get(aggregationID).getDestinations(incoming);
	}

	public boolean hasDestination(AggregationID aggregationID,MessageDestination outBroker) {
		RoutingTable reverseroutingTable=reverseRoutingTableMap.get(aggregationID);
		if(reverseroutingTable==null)return false;
		return reverseroutingTable.contains(outBroker);
	}
}
