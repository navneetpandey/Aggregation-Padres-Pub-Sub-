package ca.utoronto.msrg.padres.broker.aggregation.message;

import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.operator.OperatorType;
import ca.utoronto.msrg.padres.common.message.Publication;

public class AggregatedPublication extends Publication {
	protected AggregationID aggregationID;

	private boolean aggregation;

	private OperatorType aggOperator;
	private String agrResult;

	private String aggParam;

	private String windowID;

	public OperatorType getAggOperator() {
		return aggOperator;
	}

	public String getAggParam() {
		return aggParam;
	}

	public String getAggResult() {
		return agrResult;
	}

	@Override
	public Publication duplicate() {
		AggregatedPublication aggregatedPublication = new AggregatedPublication(
				super.duplicate(), aggregationID);
		aggregatedPublication.aggregation = aggregation;
		aggregatedPublication.aggOperator = aggOperator;
		aggregatedPublication.agrResult = agrResult;
		aggregatedPublication.aggParam = aggParam;
		aggregatedPublication.windowID = windowID;
		return aggregatedPublication;
	}

	public void setAggregation(boolean aggregation, OperatorType aggOperator,
			String aggParam, String agrResult) {
		this.aggregation = aggregation;
		this.aggOperator = aggOperator;
		this.aggParam = aggParam;
		this.agrResult = agrResult;
	}

	public AggregatedPublication(Publication publication,
			AggregationID aggregationID) {
		super();
		this.setPubID(publication.getPubID());
		this.setTimeStamp(publication.getTimeStamp());
		this.setPairMap(publication.getPairMap());
		this.setPayload(publication.getPayload());

		this.aggregationID = aggregationID;
		// TODO Auto-generated constructor stub
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -1219889763119346808L;

	public AggregationID getAggregationID() {
		return aggregationID;
	}

	public void setAggregationID(AggregationID aggregationID) {
		this.aggregationID = aggregationID;
	}

	public String getWindowID() {
		// TODO Auto-generated method stub
		return windowID;
	}

	public void setWindowID(String windowID) {
		this.windowID = windowID;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof AggregatedPublication))
			return false;
		AggregatedPublication agg = (AggregatedPublication) obj;
		return agg.getAggregationID().equals(this.aggregationID)
				&& agg.getTimeStamp().equals(this.getTimeStamp());
	}

	public void setAgrResult(String agrResult) {
		this.agrResult = agrResult;
	}

	@Override
	public int hashCode() {
		return aggregationID.hashCode();
	}

}
