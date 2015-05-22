package ca.utoronto.msrg.padres.test.junit.aggregation.utility;

import java.util.ArrayList;

import ca.utoronto.msrg.padres.broker.aggregation.Notifier;
import ca.utoronto.msrg.padres.broker.aggregation.aggregator.Aggregator;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCoreException;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;

public class DummyNotifier extends Notifier {

	ArrayList<PublicationMessage> output;
	ArrayList<Long> timer=null; 
	
	public DummyNotifier(BrokerCore bc, ArrayList<PublicationMessage> out) throws BrokerCoreException {
		super(bc);
		output=out;
		// TODO Auto-generated constructor stub
	}

	@Override
	public void SendPublication(Publication msgToRoute , MessageDestination outBroker) {
		PublicationMessage pm = new PublicationMessage(msgToRoute);
		pm.setNextHopID(outBroker);
		output.add(pm);
		System.out.println("Msg sent");
	}

	/*@Override
	public void getNotificationFromAggregator(Aggregator aggregator) {
		if(timer != null)
			timer.add(System.currentTimeMillis());
		super.getNotificationFromAggregator(aggregator);
	}
*/
	public void setTimerList(ArrayList<Long> timer){
		this.timer=timer;
	}


}
