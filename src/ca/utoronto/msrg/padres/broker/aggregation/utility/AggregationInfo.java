package ca.utoronto.msrg.padres.broker.aggregation.utility;

import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationImplementationTypes;

public class AggregationInfo {


		private int timeOut;
		private AggregationImplementationTypes type;
		
		private boolean clientAggregation;
		
		public AggregationInfo(int timeOut,
				AggregationImplementationTypes type, boolean clientAggregation) {
			super();
			this.timeOut = timeOut;
			this.type = type;
			if(type == AggregationImplementationTypes.OPTIMAL_AGGREGATION || type == AggregationImplementationTypes.ADAPTIVE_AGGREGATION || type == AggregationImplementationTypes.FG_AGGREGATION) {
				this.clientAggregation = clientAggregation;
			} else
				this.clientAggregation = false;
			
		}

		public int getTimeOut() {
			return timeOut;
		}

		public void setTimeOut(int timeOut) {
			this.timeOut = timeOut;
		}

		public AggregationImplementationTypes getAggregationImplementation() {
			return type;
		}

		public AggregationImplementationTypes getType() {
			return type;
		}

		public boolean isClientAggregation() {
			return clientAggregation;
		}

		public void setClientAggregation(boolean clientAggregation) {
			this.clientAggregation = clientAggregation;
		}
}
