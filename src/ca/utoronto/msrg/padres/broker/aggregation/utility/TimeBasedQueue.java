package ca.utoronto.msrg.padres.broker.aggregation.utility;

import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.PriorityBlockingQueue;

public class TimeBasedQueue {
	long windowSize;
	private PriorityBlockingQueue<TimeValueComparablePair> pubVector;
	
	
	public TimeBasedQueue(long windowSize,
			PriorityBlockingQueue<TimeValueComparablePair> pubVector) {
		super();
		this.windowSize = windowSize;
		this.pubVector = pubVector;
	}

	public void add(long messageTime, Object val) {
		this.pubVector.add(new TimeValueComparablePair(messageTime, val));

	}
	
	public int getItemCount(){
		return pubVector.size();	
	}
	
	public long getSize(){
		return this.windowSize;
	}
	
	public ArrayList<Object> fetchItem(long startTime) {
		long endTime = windowSize + startTime;
		ArrayList<Object> items = new ArrayList<Object>();
		TimeValueComparablePair leastItem;

		
		while ((leastItem = this.pubVector.peek())!= null 
				&& leastItem.getMessageTime() <= endTime) {
			if(leastItem.getMessageTime() > startTime)
				items.add(leastItem.getVal());
			this.pubVector.poll();
		}
		return items;
	}
}
