package ca.utoronto.msrg.padres.broker.aggregation.countsemantic;



//////////////////////DS for maintaining Decomposable ////////////////////////
class DecompoAggregationValueCB implements CountBasedAggregator
{
	int partialResult;

	int count;// should be replace with < string condition, value > pair such as <max, 10>
	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public void incrementCount()
	{
		this.count++;
	}
	
	public void reset()
	{
		partialResult =0;
		count = 0;
		triggerCond = 2;
	}

	int triggerCond;

	public boolean checkTrigger ()
	{
		return this.count > this.triggerCond;
	}
	public DecompoAggregationValueCB()
	{
		partialResult = 0 ;
		count = 0;
		triggerCond = 2;
	}

	public DecompoAggregationValueCB(int partialResult, int count, int triggerCond) {
		super();
		this.partialResult = partialResult;
		this.count = count;
		this.triggerCond = triggerCond;
	}
	
	public void updateResult(int val){
		if(val> partialResult) 
			partialResult=val;
	}

 
	public String getResultString() {
		String s = Integer.toString(partialResult);
		return s;
	}
}