package ca.utoronto.msrg.padres.broker.aggregation.utility;

public class KeyValueComparablePair implements Comparable<KeyValueComparablePair> {
	private int key;
	private Object val;

	public KeyValueComparablePair(int key, Object val) {
		super();
		this.key = key;
		this.val = val;
	}

	public int compareTo(KeyValueComparablePair arg0) {
		return key - arg0.key;
	}

	public int getKey() {
		return key;
	}

	public Object getVal() {
		return val;
	}


}
