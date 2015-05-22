package ca.utoronto.msrg.padres.broker.aggregation;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregateComputer;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationType;
import ca.utoronto.msrg.padres.broker.aggregation.message.AggregatedPublication;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;

public class NWR extends AggregatedPublication implements Comparable<NWR> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5912917402234086233L;

	AggregateComputer computer;

	// AggregationID aggregationID;

	MessageDestination messageDestination;

	Set<MessageDestination> messageSources;

	AggregatedPublication aggPub;

	public NWR(PublicationMessage pm) {
		super(pm.getPublication(), ((AggregatedPublication) pm.getPublication()).getAggregationID());
		

		AggregatedPublication agg = (AggregatedPublication) pm.getPublication();

		this.aggPub = new AggregatedPublication(agg, aggregationID);
		this.setTimeStamp(agg.getTimeStamp()); // Possibly redundant ... should
												// be removed if so

		// this.aggPub.getPairMap().clear();

		this.aggregationID = agg.getAggregationID();
		computer = new AggregateComputer(aggregationID.getOperatorType(), AggregationType.TIMEBASED);
		addPublication(pm.getPublication());
		messageSources = new HashSet<MessageDestination>();
		this.messageDestination = null; 
		
		this.messageSources.add(pm.getLastHopID());
	}

	public Set<MessageDestination> getMessageSources() {
		return messageSources;
	}

	public void setMessageSources(Set<MessageDestination> messageSources) {
		this.messageSources = messageSources;
	}

	long delay = 500;

	public NWR(Publication publication, AggregationID aggregationID) {
		super(publication, aggregationID);

		// if(publication instanceof AggregatedPublication || publication
		// instanceof NWR) System.err.println(" OEUOEUOAEUOAEUOAEUOEAUOE ");

		this.aggregationID = aggregationID;
		computer = new AggregateComputer(aggregationID.getOperatorType(), AggregationType.TIMEBASED);
		aggPub = new AggregatedPublication(publication.duplicate(), aggregationID);
		aggPub.setTimeStamp(this.getTimeStamp());
		messageSources = new HashSet<MessageDestination>();
	}

	public void addPublication(Publication pub) {

		if (pub instanceof AggregatedPublication || pub instanceof NWR) {
			computer.aggregateComputation(((AggregatedPublication) pub).getAggResult());
		} else {

			computer.aggregateComputation(pub.getPairMap().get(aggregationID.getFieldName()).toString());
		}
	}

	public MessageDestination getMessageDestination() {
		return messageDestination;
	}

	public void setDestination(MessageDestination messageDestination) {
		this.messageDestination = messageDestination;
	}

	private String getAggregatedResult() {
		return computer.getResultString(false);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof NWR)) {
			return false;
		}
		NWR n = (NWR) obj;
		return n.getTimeStamp().getTime() == this.getTimeStamp().getTime()
		// && messageDestination.equals(n.getMessageDestination())
				&& this.getAggregationID().equals(n.getAggregationID());
	}

	public AggregatedPublication getAggregatedPublication() {
		aggPub.setAgrResult(getAggregatedResult());
		aggPub.setTimeStamp(new Date(this.getTimeStamp().getTime()));
		return aggPub;
	}

	@Override
	public int hashCode() {
		// System.out.println("\n\t HASH"+(getTimeStamp()+""+aggregationID.hashCode()).hashCode());

		return (/* messageDestination.toString()+ */getTimeStamp().getTime() + "" + aggregationID.aggregationID/*
																												 * +
																												 * aggregationID
																												 * .
																												 * getSubID
																												 * (
																												 * )
																												 */).hashCode();
	}

	@Override
	public int compareTo(NWR o) {
		if (this.equals(o))
			return 0;
		if (this.getTimeStamp().getTime() > o.getTimeStamp().getTime())
			return -1;
		else if (this.getTimeStamp().getTime() < o.getTimeStamp().getTime())
			return 1;
		else
			return (int) (this.getAggregationID().aggregationID - o.getAggregationID().aggregationID);
	}

	@Override
	public Publication duplicate() {
		NWR n = new NWR(aggPub.duplicate(), aggregationID);
		n.setTimeStamp(getTimeStamp());
		n.setMessageSources(messageSources);
		n.setDestination(messageDestination);

		return n;
	}

}
