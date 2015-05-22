package ca.utoronto.msrg.padres.test.junit.aggregation.utility;

import java.util.ArrayList;

import ca.utoronto.msrg.padres.broker.aggregation.message.AggregatedPublication;
import ca.utoronto.msrg.padres.client.Client;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.message.Message;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;

public class TestAggregatorClient extends Client {

	ArrayList<Message> receivedMessageList = new ArrayList<Message>();
	public boolean messageReceived = false;
	public int aggregateMessageReceived = 0;
	public int getAggregateMessageReceived() {
		return aggregateMessageReceived;
	}
	
	public void resetAggregateMessageReceivedCounter() {
		  aggregateMessageReceived=0;
	}


	public void incAggregateMessageReceived() {
		this.aggregateMessageReceived++;
	}

	public int getNonAggregateMessageReceived() {
		return nonAggregateMessageReceived;
	}

	public void incNonAggregateMessageReceived() {
		this.nonAggregateMessageReceived ++;
	}

	public int nonAggregateMessageReceived = 0;

 

	public TestAggregatorClient(String string) throws ClientException {
		super(string);

	}

	public void processMessage(Message msg) {
		super.processMessage(msg);
		System.out.println("[TEST_CLIENT]"+ "messageRecieved ");
		if(((PublicationMessage)msg).getPublication() instanceof AggregatedPublication)
			System.err.println("[TEST_CLIENT] AGG"+ clientID + " " + ((AggregatedPublication)((PublicationMessage)msg).getPublication()).getAggResult().toString());
		else
		 System.err.println("[TEST_CLIENT] NONAGG"+ clientID + " " + ((PublicationMessage)msg).getPublication().toString());
		receivedMessageList.add(msg);
		messageReceived = true;
		if (!(((PublicationMessage)msg).getPublication() instanceof AggregatedPublication))
			incNonAggregateMessageReceived();
		else
			aggregateMessageReceived++;
	}
	
	public ArrayList<Message> getReceivedMessage(){
		return receivedMessageList;
	}
	
	
}
