package ca.utoronto.msrg.padres.broker.aggregation.adaptation;

import java.util.Hashtable;

import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationMode;
import ca.utoronto.msrg.padres.broker.aggregation.utility.SlidingWindow;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.Subscription;

public class AdaptationSwitch {

	private AggregationMode adapStatus = AggregationMode.AGG_MODE;

	private SlidingWindow sampleWindow ;
	
	private final int MILLISECOND=1000;

	//for estimating the number of notifications its map of AggID, notifications in the sampling window
	Hashtable<AggregationID, Integer> notificationPerAggregationVector;
	
	public AdaptationSwitch(long sampleWindowsize) {
		super();
		this.sampleWindow = new SlidingWindow(sampleWindowsize);
		this.notificationPerAggregationVector = new Hashtable<AggregationID, Integer>();

	}

	public void addPubMessage(PublicationMessage pubMsg) {
		sampleWindow.add(pubMsg.getPublication().getTimeStamp().getTime()
				//getMessageTime().getTime()
				, pubMsg.toString());
	}

	/*public void addSubMessage(Subscription sub){
		if(!subNTFVector.contains(sub.toString()))
			subNTFVector.put(sub.toString(), (long) sub.getAggNTF());
	}*/
	
	public void addNTFMessages(AggregationID aggregationID) {
		
		if(notificationPerAggregationVector.contains(aggregationID))
			notificationPerAggregationVector.put(aggregationID, notificationPerAggregationVector.get(aggregationID)+1);
		else
			notificationPerAggregationVector.put(aggregationID,1);
		
	}
	
	private void updateWindowSize(long newWindowSize) {
		this.sampleWindow.updateSize(newWindowSize);
	}
	/*
	 * It returns true if aggregation is more efficient than raw mode
	 * 
	 */
	public boolean checkForAggregationTrigger(){
		float totalNTF = 0 ;
		AggregationMode oldValue = adapStatus;
		synchronized(notificationPerAggregationVector) {
			for(AggregationID aggID : notificationPerAggregationVector.keySet()){
				totalNTF += (float) (sampleWindow.getSize())/(aggID.getShiftSize()*MILLISECOND);
			}
		}

		synchronized(adapStatus){
			System.out.println(sampleWindow.getItemCount()+" > "+totalNTF);
			if(sampleWindow.getItemCount() > totalNTF )
				adapStatus = AggregationMode.AGG_MODE;
			else
				adapStatus = AggregationMode.RAW_MODE;
		}
		if(oldValue!=adapStatus)return true;
		return false;
	}
	
	public long getSampleWindowSize(){
		return sampleWindow.getSize();
	}

	public AggregationMode getAdapStatus() {
		return adapStatus;
	}

}
