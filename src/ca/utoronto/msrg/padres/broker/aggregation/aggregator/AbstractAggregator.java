package ca.utoronto.msrg.padres.broker.aggregation.aggregator;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import ca.utoronto.msrg.padres.broker.aggregation.Notifier;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregateComputer;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationType;
import ca.utoronto.msrg.padres.broker.aggregation.collector.AbstractCollector;
import ca.utoronto.msrg.padres.broker.aggregation.message.AggregatedPublication;
import ca.utoronto.msrg.padres.broker.aggregation.utility.SortedArrayList;
import ca.utoronto.msrg.padres.broker.aggregation.utility.Stats;
import ca.utoronto.msrg.padres.broker.aggregation.utility.TimeValueComparablePair;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.SubscriptionMessage;

public abstract class AbstractAggregator {

	protected final long MILLISECOND = 1000L;

	protected static final boolean VERBOSE_ON = true;
	
	
	MessageDestination sourceLink;

	// We always assume that end of pub Queue will be always greater than
	// Starting of the current window
	protected long startTimeOfTheCurrentWindow;

	// All times are in millisecond TODO: really? NO they are in second

	protected long windowSize;
	protected long shiftSize;

	protected AggregateComputer aggregateComputer;

	protected AbstractCollector collector;

	protected Notifier notifier;

	protected AggregationID aggregationID;


	// This is vector for storing publication data
	protected SortedArrayList<TimeValueComparablePair<Publication>> timeAndPubValueQueue;
	
	protected Set<MessageDestination> forwardSubIDSet = new HashSet<MessageDestination>();

	public AbstractAggregator(Notifier notifier, AggregationID aggregationID, AggregationType aggregationType, long startTimeOfTheCurrentWindow,
			MessageDestination sourceLink) {

		this.notifier = notifier;

		this.aggregationID = aggregationID;
		this.windowSize = aggregationID.getWindowSize();
		this.shiftSize = aggregationID.getShiftSize();
		
		this.aggregateComputer = new AggregateComputer(aggregationID.getOperatorType(), aggregationType);
		this.startTimeOfTheCurrentWindow = startTimeOfTheCurrentWindow - Math.max((windowSize - shiftSize) * MILLISECOND, 0);
		this.sourceLink = sourceLink;
		timeAndPubValueQueue = new SortedArrayList<TimeValueComparablePair<Publication>>();

	}

	public MessageDestination getSourceLink() {
		return sourceLink;
	}

	public long getStartTimeOfTheCurrentWindow() {
		return startTimeOfTheCurrentWindow;
	}

	public void registerSubscription(SubscriptionMessage sub) {
		forwardSubIDSet.add(sub.getLastHopID());

	}

	public void subscribeCollector(AbstractCollector collector, SubscriptionMessage subMsg) {
		/*
		 * if( collector instanceof CollectorAtBroker)
		 * ((CollectorAtBroker)collector).registerSubscription(subMsg);
		 */
		this.collector = collector;
	}

	public AbstractCollector getCollector() {
		return collector;
	}

	public AggregationID getAggregationID() {
		return aggregationID;
	}

	public AggregateComputer getAggregateComputer() {
		return aggregateComputer;
	}

	public Set<MessageDestination> getForwardSubIDList() {
		return forwardSubIDSet;
	}


	
	protected void processPublication(TimeValueComparablePair<Publication> timeAndPub) {

		String key = aggregationID.getFieldName();
		aggregateComputer.aggregateComputation(timeAndPub.getVal().getPairMap().get(key).toString());

		
		
	}
	
	/*
	 * Publish result of aggregation for the current window
	 * 
	 * @param forClient if the result is for client then prepare the final
	 * result for nondecomposable and indirectdecomposable. For decomposable it
	 * does not matter
	 * 
	 * NOTE:- Right now we are just ignoring this one.
	 */
	public void publishResult(boolean forClient) {

		TimeValueComparablePair<Publication> timeAndPub;

		boolean pubGetComputed = false;
		boolean pubGetDeleted = false;
		String result = null;

		for (int i = 0; i < timeAndPubValueQueue.size(); i++) {
			timeAndPub = timeAndPubValueQueue.get(i);
			pubGetComputed = false;
			pubGetDeleted = false;
			// aggregate if it is withing current period
			if (withinCurrentWindow(timeAndPub.getMessageTime())) {
				if (VERBOSE_ON)
					System.out.println("[AGR-" + notifier.getBrokerCore().getBrokerID() + this.hashCode() + "]publishResult=>"
							+ " Within current window(in startWindow " + getStartTimeOfTheCurrentWindow() + ")  " + timeAndPub.getMessageTime());
				processPublication(timeAndPub);
				pubGetComputed = true;
			}

			// if it is within shift delete it
			if (withinCurrentShift(timeAndPub.getMessageTime()) || isOldPublication(timeAndPub.getMessageTime())) {
				if (isOldPublication(timeAndPub.getMessageTime())) {
					System.out.println("[AGR-" + notifier.getBrokerCore().getBrokerID() + this.hashCode() + "] publishResult=>"
							+ " OLD PUBLICATIONS(in startWindow " + getStartTimeOfTheCurrentWindow() + ") " + timeAndPub.getMessageTime());
				} else
					System.out.println("[AGR-" + notifier.getBrokerCore().getBrokerID() + this.hashCode() + "] publishResult=>"
							+ " Within current Shift(in startWindow " + getStartTimeOfTheCurrentWindow() + ") " + timeAndPub.getMessageTime());
				timeAndPubValueQueue.remove(i);
				i--;
				pubGetDeleted = true;
			}

			if (!(pubGetComputed || pubGetDeleted) || (i + 1) == timeAndPubValueQueue.size()) {
				System.out.println("[AGR-" + notifier.getBrokerCore().getBrokerID() + this.hashCode() + "]publishResult=>" + " pubGetComputed "
						+ pubGetComputed + " pubGetDeleted " + pubGetDeleted + " HasMore " + ((i + 1) == timeAndPubValueQueue.size()));
				if (aggregateComputer.resultExist()) {
					result = aggregateComputer.getResultString(forClient);

					PublicationMessage msg = new PublicationMessage(notifier.createPublication(aggregationID, result, ""
							+ (startTimeOfTheCurrentWindow + windowSize * MILLISECOND), false));
					System.out.println("++++++++++++++++++++++++++" + ((AggregatedPublication) msg.getPublication()).getWindowID()+" dst "+sourceLink);
					msg.setLastHopID(sourceLink);

					collector.addMsg(msg);
					aggregateComputer.reset();

				}
				break;

			}
		}
		if (System.currentTimeMillis() > (startTimeOfTheCurrentWindow + shiftSize * MILLISECOND)) {
			startTimeOfTheCurrentWindow += shiftSize * MILLISECOND;
			System.out.println("[AGR-" + notifier.getBrokerCore().getBrokerID() + this.hashCode() + "]publishResult=>" + "X New Window start: "
					+ startTimeOfTheCurrentWindow);
		}
	}

	public void addPublication(Publication publication) {

		String key = aggregationID.getFieldName();
		Serializable value = publication.getPairMap().get(key);

		if (value != null) {

			long pubTime = publication.getTimeStamp().getTime();

			if (!isOldPublication(pubTime)) {

				// Dont store the message in case aggregation windows are
				// discontinous
				// i.e. if window > shift ( Not( not belong W && belong Shift))
				if (withinCurrentWindow(pubTime) || !withinCurrentShift(pubTime)) {
					timeAndPubValueQueue.add(new TimeValueComparablePair<Publication>(pubTime, publication));//value.toString()));
					if (notifier.getBrokerCore() != null)
						System.out.println("[AGR-" + notifier.getBrokerCore().getBrokerID() + this.hashCode() + "]addPublication=> "
								+ publication.getPairMap().toString() + " | " + sourceLink + " at " + pubTime);
				}

			} else {
				System.out.println("[AGR-" + notifier.getBrokerCore().getBrokerID() + this.hashCode() + "] addPublication=>" + " OLD PUBLICATIONS "
						+ pubTime + " < " + startTimeOfTheCurrentWindow);
				Stats.getInstance("test").addValue("PUBLICATION.OLD", 1);
			}

		}

	}

	public boolean hasNoPublication() {
		if (timeAndPubValueQueue.size() == 0)
			return true;
		else
			return false;
	}



	public boolean withinCurrentWindow(long time) {
		if (startTimeOfTheCurrentWindow <= time && time < (startTimeOfTheCurrentWindow + windowSize * MILLISECOND))
			return true;
		else
			return false;
	}

	public boolean withinCurrentShift(long time) {
		if (startTimeOfTheCurrentWindow <= time && time < (startTimeOfTheCurrentWindow + shiftSize * MILLISECOND))
			return true;
		else
			return false;
	}

	public boolean isOldPublication(long time) { 
		return time < startTimeOfTheCurrentWindow;
	}

	public void resetWindowTime() {
		if (startTimeOfTheCurrentWindow + shiftSize * MILLISECOND >= System.currentTimeMillis())
			startTimeOfTheCurrentWindow += shiftSize * MILLISECOND;

	}

	public static boolean isSlidingWindow(AggregationID aggregationID) {
		return aggregationID.getShiftSize() < aggregationID.getWindowSize();
	}

}
