package ca.utoronto.msrg.padres.broker.aggregation.utility;
 

public class TimeValueComparablePair<T> implements Comparable<TimeValueComparablePair<T>> {
	private long messageTime;
	private T val;

	public TimeValueComparablePair(long messageTime, T val) {
		super();
		this.messageTime = messageTime;
		this.val = val;
	}

	public int compareTo(TimeValueComparablePair<T> arg0) {
		return (int) (messageTime -arg0.messageTime);
	}

	public long getMessageTime() {
		return messageTime;
	}

	public T getVal() {
		return val;
	}



}