package ca.utoronto.msrg.padres.broker.aggregation.utility;

import java.util.PriorityQueue;

public class SlidingWindow {
	long windowSize;
	private PriorityQueue<TimeValueComparablePair> pubVector;
	
	
	public SlidingWindow(long windowSize) {
		super();
		this.windowSize = windowSize;
		this.pubVector = new PriorityQueue<TimeValueComparablePair>();
	}

	public void add(long messageTime, Object val) {

		this.pubVector.add(new TimeValueComparablePair(messageTime, val));
		TimeValueComparablePair leastDateItem = this.pubVector.peek();

		while (messageTime - leastDateItem.getMessageTime() > windowSize) {
			this.pubVector.poll();
			leastDateItem = this.pubVector.peek();
			if (leastDateItem.equals(null))
				break;
		}

	}
	
	public int getItemCount(){
		return pubVector.size();	
	}
	
	public long getSize(){
		return this.windowSize;
	}
	
	public void updateSize( long newSize){
		
		//there is no unsubscription so there is no need to decrease
		synchronized(this) {
			if(newSize > windowSize)  windowSize=newSize;
		}
	}
}
