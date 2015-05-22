package ca.utoronto.msrg.padres.broker.aggregation.collector;

import ca.utoronto.msrg.padres.common.message.MessageDestination;

public class OutgoingWindow {
	private String windowID;
	private MessageDestination outBroker;
	
	
	
	public OutgoingWindow(String windowID, MessageDestination outBroker) {
		super();
		this.windowID = windowID;
		this.outBroker = outBroker;
	}
	
	
	public String getWindowID() {
		return windowID;
	}
	public void setWindowID(String windowID) {
		this.windowID = windowID;
	}
	public MessageDestination getOutBroker() {
		return outBroker;
	}
	public void setOutBroker(MessageDestination outBroker) {
		this.outBroker = outBroker;
	}


	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof OutgoingWindow))return false;
		return  (((OutgoingWindow)obj).getOutBroker().equals(outBroker) && ((OutgoingWindow)obj).windowID.equals(windowID));
	}


	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return windowID.hashCode()+outBroker.hashCode()<<13;
	}
	
	

}
