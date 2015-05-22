package ca.utoronto.msrg.padres.broker.aggregation;

import java.util.Set;

import no.uio.ifi.graph.aggregation.TimeNode;
import ca.utoronto.msrg.padres.broker.aggregation.message.AggregatedPublication;
import ca.utoronto.msrg.padres.common.message.Publication;

public class OptimalAggregationGraphNode extends TimeNode<Publication> {

	
	public OptimalAggregationGraphNode(Publication publication) {
		super(publication);
	}
	
	
	public  OptimalAggregationGraphNode(Publication publication, long timestamp) {
		super(publication, timestamp);
	}
	@Override
	public void compute(Set<TimeNode<Publication>> set) {
		if ( !(element instanceof NWR)) {
			System.err.println("Wrong publication type");
			return;
		}
	
		//System.out.println("OptPub computing result: previous value "+((NWR)element).getAggregatedPublication().getAggResult()+" Set "+set.size());
		for(TimeNode<Publication> t: set){
		//	System.out.println("d "+t);
			((NWR)element).addPublication(t.getElement());
		}
		//System.out.println("OptPub computing result: new value "+((NWR)element).getAggregatedPublication().getAggResult());
		
	}

	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof OptimalAggregationGraphNode))return false;
		if(!(element instanceof NWR))return super.equals(obj); //false
		return ((OptimalAggregationGraphNode)obj).element.hashCode()==element.hashCode();//element.equals(((OptimalAggregationGraphNode) obj).element) && getTimestamp()==((OptimalAggregationGraphNode) obj).getTimestamp();
	}

	@Override
	public int hashCode() {
		return (element.hashCode()+""+/*element.*/getTimestamp()).hashCode();
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return element.toString();
	}

	public Publication getPublication(){
		return element;
	}


	@Override
	protected Object clone() throws CloneNotSupportedException {
		return new OptimalAggregationGraphNode(element.duplicate(),this.getTimestamp());
	}


	
	
	@Override
	public int compareTo(TimeNode<Publication> o) {
		if(this.equals(o))return 0;
		if(getTimestamp()<o.getTimestamp())return -1;
		return 1;
	}


	
	
	
	
}
