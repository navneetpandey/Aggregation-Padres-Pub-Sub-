package ca.utoronto.msrg.padres.test.junit.aggregation.utility;

import java.util.ArrayList;

import ca.utoronto.msrg.padres.broker.aggregation.message.AggregatedPublication;
import ca.utoronto.msrg.padres.client.Client;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.message.Message;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;

public class TestClient extends Client {

	ArrayList<Message> receivedAllMessageList = new ArrayList<Message>();
	ArrayList<Message> receivedRegularMessageList = new ArrayList<Message>();
	ArrayList<Message> receivedAggregatedMessageList = new ArrayList<Message>();
	private boolean messageReceived = false;
	private int totalRegularMessages = 0;
	private int totalAggregatedMessages = 0;

	public TestClient(String string) throws ClientException {
		super(string);

	}

	public void processMessage(Message msg) {
		System.out.println("[TEST_CLIENT]" + "messageRecieved ");
		if (((PublicationMessage) msg).getPublication() instanceof AggregatedPublication) {
			System.err.println("[TEST_CLIENT] AGG"
					+ clientID
					+ " "
					+ ((AggregatedPublication) ((PublicationMessage) msg)
							.getPublication()).getAggResult().toString());
			totalAggregatedMessages++;
			receivedAggregatedMessageList.add(msg);
			
		} else {
			if(((PublicationMessage) msg).aggregationRequired())
			System.err.println("[TEST_CLIENT] NONAGG but AggRequired" + clientID + " "
					+ ((PublicationMessage) msg).getPublication().toString());
			else
			System.err.println("[TEST_CLIENT] NONAGG but AggNOTrequired" + clientID + " "
					+ ((PublicationMessage) msg).getPublication().toString());
			totalRegularMessages++;
			receivedRegularMessageList.add(msg);
		}
		receivedAllMessageList.add(msg);
		setMessageReceived(true);

	}

	public ArrayList<Message> getAllReceivedMessage() {
		return receivedAllMessageList;
	}
	 

	public ArrayList<Message> getReceivedRegularMessageList() {
		return receivedRegularMessageList;
	}

	public ArrayList<Message> getReceivedAggregatedMessageList() {
		return receivedAggregatedMessageList;
	}

	public boolean isMessageReceived() {
		return messageReceived;
	}

	public void setMessageReceived(boolean messageReceived) {
		this.messageReceived = messageReceived;
	}

	public int getTotalRegularMessages() {
		return totalRegularMessages;
	}

	public int getTotalAggregatedMessages() {
		return totalAggregatedMessages;
	}

}
