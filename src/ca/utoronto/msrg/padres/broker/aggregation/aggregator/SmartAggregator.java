package ca.utoronto.msrg.padres.broker.aggregation.aggregator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Set;

import ca.utoronto.msrg.padres.broker.aggregation.Notifier;
import ca.utoronto.msrg.padres.broker.aggregation.ScoredPublication;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationType;
import ca.utoronto.msrg.padres.broker.aggregation.message.AggregatedPublication;
import ca.utoronto.msrg.padres.broker.aggregation.utility.SortedArrayList;
import ca.utoronto.msrg.padres.broker.aggregation.utility.Stats;
import ca.utoronto.msrg.padres.broker.aggregation.utility.TimeValueComparablePair;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;

public class SmartAggregator extends PerOutgoingLinkAbstractAggregator {

	double score;


	Set<Publication> pubToFWD;
	// This is vector for storing publication data
	protected SortedArrayList<TimeValueComparablePair<ScoredPublication>> timeAndPubValueQueue;

	public static class SmartAggregatorFactory implements AggregatorFactory<SmartAggregator> {

		@Override
		public SmartAggregator createAggregator(Notifier notifier, AggregationID aggregationID, AggregationType aggregationType,
				long startTimeOfTheCurrentWindow, MessageDestination sourceLink) {
			return new SmartAggregator(notifier, aggregationID, aggregationType, startTimeOfTheCurrentWindow, sourceLink);
		}

	}

	public SmartAggregator(Notifier notifier, AggregationID aggregationID, AggregationType aggregationType, long startTimeOfTheCurrentWindow,
			MessageDestination sourceLink) {
		super(notifier, aggregationID, aggregationType, startTimeOfTheCurrentWindow, sourceLink);
		timeAndPubValueQueue=new SortedArrayList<TimeValueComparablePair<ScoredPublication>>();
	}


	protected void processScoredPublication(TimeValueComparablePair<ScoredPublication> timeAndPub) {

		String key = aggregationID.getFieldName();
		aggregateComputer.aggregateComputation(timeAndPub.getVal().getPublication().getPairMap().get(key).toString());

	}

	

	public ArrayList<Publication> getPubinCurrentWindow() {
		//Last minute fix
		if (System.currentTimeMillis() < (startTimeOfTheCurrentWindow + shiftSize * MILLISECOND)) return new ArrayList<Publication>();
		
		
		TimeValueComparablePair<ScoredPublication> timeAndPub;

		boolean pubGetComputed = false;
		boolean pubGetDeleted = false;
		String result = null;
		score=0;
		ArrayList<Publication> publicationList = new ArrayList<Publication>();

		for (int i = 0; i < timeAndPubValueQueue.size(); i++) {
			timeAndPub = timeAndPubValueQueue.get(i);
			pubGetComputed = false;
			pubGetDeleted = false;
			// aggregate if it is within current period
			if (withinCurrentWindow(timeAndPub.getMessageTime())) {
				publicationList.add(timeAndPub.getVal().getPublication());
				processScoredPublication(timeAndPub);
				score+=1.0/timeAndPub.getVal().getScore();
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

				break;

			}
		}
		if (System.currentTimeMillis() > (startTimeOfTheCurrentWindow + shiftSize * MILLISECOND)) {
			startTimeOfTheCurrentWindow += shiftSize * MILLISECOND;
			System.out.println("[AGR-" + notifier.getBrokerCore().getBrokerID() + this.hashCode() + "]publishResult=>" + "X New Window start: "
					+ startTimeOfTheCurrentWindow);
		}
		return publicationList;
	}

	private void deletePubSendAsRaw(ArrayList<Publication> pub){
		for(int i=0;i<timeAndPubValueQueue.size();i++){
			if(timeAndPubValueQueue.get(i).getMessageTime()<(startTimeOfTheCurrentWindow + (windowSize - shiftSize) * MILLISECOND)){
				timeAndPubValueQueue.remove(i);
				i--;
			}
		}
	}
	
	@Override
	public void publishResult(boolean forClient) {

		ArrayList<Publication> res = getPubinCurrentWindow();
		if(res.isEmpty())return;
		if (score < 1.0) {
			aggregate=false;
			deletePubSendAsRaw(res);
			synchronized (pubToFWD) {
				pubToFWD.addAll(res);
			}
			System.out.println("Score is too low " + score + " ... forward");
			System.err.println("Forwarding from "+sourceLink+" destination "+destination);
		} else {
			String result;
			aggregate=true;
			if (aggregateComputer.resultExist()) {
				result = aggregateComputer.getResultString(false);

				PublicationMessage msg = new PublicationMessage(notifier.createPublication(aggregationID, result, ""
						+ (startTimeOfTheCurrentWindow + windowSize * MILLISECOND), false));
				System.out.println("++++++++++++++++++++++++++" + ((AggregatedPublication) msg.getPublication()).getWindowID()+ " dst "+sourceLink);
				msg.setLastHopID(sourceLink);

				collector.addMsg(msg);
				aggregateComputer.reset();

				//System.err.println("Aggregating from "+sourceLink+" destination "+destination);
			}

			System.out.println("High SCORE "+score +" aggregate");
		}

	}

	public void setPubToFWD(Set<Publication> pubToFWD) {
		this.pubToFWD = pubToFWD;
	}

	public void addPublication(Publication publication, int score) {

		String key = aggregationID.getFieldName();
		Serializable value = publication.getPairMap().get(key);

		if (value != null) {

			long pubTime = publication.getTimeStamp().getTime();

			if (!isOldPublication(pubTime)) {

				// Dont store the message in case aggregation windows are
				// discontinous
				// i.e. if window > shift ( Not( not belong W && belong Shift))
				if (withinCurrentWindow(pubTime) || !withinCurrentShift(pubTime)) {
					// TODO Fix this hack if subscriptions are not having the same ratio window/shift
					// TODO Fix this hack if we consider hoping windows
					timeAndPubValueQueue.add(new TimeValueComparablePair<ScoredPublication>(pubTime, new ScoredPublication(publication,(int) (score*windowSize/shiftSize))));//value.toString()));
					if (notifier.getBrokerCore() != null)
						System.out.println("[AGR-" + notifier.getBrokerCore().getBrokerID() + this.hashCode() + "]addPublication=> "
								+ publication.getPairMap().toString() + " | " + sourceLink + " at " + pubTime);
					else {
						System.err.println("FATAL: Initialization error: notifier.getBrokerCore() is null");
						System.exit(1);
					}
				}

			} else {
				System.out.println("[AGR-" + notifier.getBrokerCore().getBrokerID() + this.hashCode() + "] addPublication=>" + " OLD PUBLICATIONS "
						+ pubTime + " < " + startTimeOfTheCurrentWindow);
				Stats.getInstance("test").addValue("PUBLICATION.OLD", 1);
				synchronized (pubToFWD) {
					pubToFWD.add(publication);
				}
				
			}

		}

	}

}
