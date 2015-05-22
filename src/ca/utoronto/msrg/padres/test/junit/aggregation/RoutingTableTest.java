package ca.utoronto.msrg.padres.test.junit.aggregation;

import junit.framework.TestCase;
import ca.utoronto.msrg.padres.broker.aggregation.utility.RoutingTable;
import ca.utoronto.msrg.padres.common.message.MessageDestination;

public class RoutingTableTest extends TestCase {

	public RoutingTable routingTable;

	@Override
	protected void setUp() throws Exception {
		
		System.setProperty("aggregation.client", "OFF");
		//System.setProperty("padres.aggregation.implementation","EARLY_AGGREGATION");
		
		if(System.getProperty("padres.aggregation.implementation")==null)assertTrue(false);
		routingTable=new RoutingTable();
		super.setUp();
	}
	
	
	public void testRoutingTable(){
		
		routingTable.addRoute(new MessageDestination("A"), new MessageDestination("B"));
		assertTrue(routingTable.getDestinations(new MessageDestination("A")).contains(new MessageDestination("B")));
		
		
		routingTable.addRoute(new MessageDestination("A"), new MessageDestination("C"));
		assertTrue(routingTable.getDestinations(new MessageDestination("A")).contains(new MessageDestination("B")));
		assertTrue(routingTable.getDestinations(new MessageDestination("A")).contains(new MessageDestination("C")));
		
		routingTable.addRoute(new MessageDestination("X"), new MessageDestination("Y"));
		routingTable.addRoute(new MessageDestination("X"), new MessageDestination("Z"));
		
		assertTrue(routingTable.getDestinations(new MessageDestination("A")).contains(new MessageDestination("B")));
		assertTrue(routingTable.getDestinations(new MessageDestination("A")).contains(new MessageDestination("C")));
		assertFalse(routingTable.getDestinations(new MessageDestination("A")).contains(new MessageDestination("X")));
		assertFalse(routingTable.getDestinations(new MessageDestination("A")).contains(new MessageDestination("Y")));
		assertTrue(routingTable.getDestinations(new MessageDestination("X")).contains(new MessageDestination("Y")));
		assertTrue(routingTable.getDestinations(new MessageDestination("X")).contains(new MessageDestination("Z")));
		assertFalse(routingTable.getDestinations(new MessageDestination("X")).contains(new MessageDestination("B")));
		assertFalse(routingTable.getDestinations(new MessageDestination("X")).contains(new MessageDestination("C")));
		
		
	}
	
	
}
