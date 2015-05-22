package ca.utoronto.msrg.padres.broker.aggregation;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import no.uio.ifi.graph.aggregation.TimeConstraintBipartiteGraph;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.message.AggregatedPublication;
import ca.utoronto.msrg.padres.broker.aggregation.utility.AggregationInfo;
import ca.utoronto.msrg.padres.broker.aggregation.utility.SortedArrayList;
import ca.utoronto.msrg.padres.broker.aggregation.utility.Stats;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.common.message.Message;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.common.message.SubscriptionMessage;

public class OptimalAggregationEngine extends AbstractAggregationEngine {

	private static final int PERFECT_COLLECTOR_SIZE = 1000;

	// The aggregator will delay all decision of DELAY ms  
	public static long DELAY=100;//1200000;//10000; 
	
	public static long WAITING_TIME_FOR_LATE=100; // late publication will be sent 100 ms after their arrival
	
	public static long COLLECTING_TIMEOUT = 100;//1000;

	public static long DEFAULT_TIMEOUT = 10L;

	private long processAggregationStartTime;
	final int MILLISECOND = 1000;

	HashMap<MessageDestination, TimeConstraintBipartiteGraph<OptimalAggregationGraphNode, Publication>> graphs;

	HashMap<AggregationID, Long> delay;
	
	SortedArrayList<NWR> perfectCollector=new SortedArrayList<NWR>();

	public OptimalAggregationEngine(BrokerCore brockerCore, AggregationInfo agInfo) {
		super(brockerCore, agInfo);
		currentPublicationProfile.setSelectivelyDisableAggregationRequired();
		graphs = new HashMap<MessageDestination, TimeConstraintBipartiteGraph<OptimalAggregationGraphNode, Publication>>();
		delay = new HashMap<AggregationID, Long>();

		aggregatorTimerService.addOptimalTask(this);
	}

	
	public long getCurrentTime(){
		return System.currentTimeMillis() - DELAY;
	}
	
	public long getWindowStartTime(long time, AggregationID agg) {
		return time - (time % (agg.getShiftSize() * MILLISECOND));
	}

	public long getShiftTime(long time, AggregationID agg) {
		return getWindowStartTime(time, agg) + agg.getShiftSize() * MILLISECOND;
	}

	public long getNextNotificationTime(long time, AggregationID agg) {
		return time - (time % (agg.getShiftSize() * MILLISECOND)) + agg.getShiftSize() * MILLISECOND;
	}

	@Override
	public void processSubscriptionMessage(SubscriptionMessage subMsg, Set<Message> outGoingmessageSet) {

		if(graphs.get(subMsg.getLastHopID())==null){
			graphs.put(subMsg.getLastHopID(), new TimeConstraintBipartiteGraph<OptimalAggregationGraphNode, Publication>());
		}
		delay.put(subMsg.getSubscription().getAggregationID(), DEFAULT_TIMEOUT);
		
		super.processSubscriptionMessage(subMsg, outGoingmessageSet);
	}

	public NWR getNWR(long ntfTime, AggregationID aggregationID, MessageDestination dst) {
		NWR n = new NWR(currentPublicationProfile.getPublication(), aggregationID);
		n.setTimeStamp(new Date(ntfTime));
		n.setDestination(dst);

		return n;
	}

	public List<NWR> getNWRs(AggregationID aggregationID) {
		ArrayList<NWR> aggPubs = new ArrayList<NWR>();
		long pubTime = currentPublicationProfile.getPublicationTime();
		long win = aggregationID.getWindowSize();
		long shift = aggregationID.getShiftSize();
		Set<MessageDestination> dest = new HashSet<MessageDestination>();

		long start = getWindowStartTime(pubTime - win * MILLISECOND, aggregationID);

		Set<MessageDestination> tmp = routingTable.getDestinations(aggregationID, new MessageDestination(currentPublicationProfile
				.getPublicationMessage().getLastHopID().getBrokerId()));

		dest.addAll(tmp);

		if (currentPublicationProfile.isAggregatedPublication()) {
			System.err.println("WTF");
			System.exit(1);
			/*for (MessageDestination d : dest) {
				NWR n = new NWR(currentPublicationProfile.getPublicationMessage());
				//System.out.println("DEST " + d);
				n.setDestination(d);
				if (graphs.get(d).getPartition1().contains(new OptimalAggregationGraphNode(n, n.getTimeStamp().getTime())))
					aggPubs.add(n);
			}
			return aggPubs;*/
		}

		while (start <= pubTime) {
			if (pubTime >= getWindowStartTime(start, aggregationID) && pubTime < start + win * MILLISECOND) {
				for (MessageDestination d : dest) {
					NWR n = getNWR(start + win * MILLISECOND, aggregationID, d);
					n.setMessageSources(routingTable.getDestinations(aggregationID, d));
					aggPubs.add(n);
				}

			}
			start += shift * MILLISECOND;
		}

		return aggPubs;
	}

	public List<NWR> getAllNWRs() {
		List<NWR> result = new ArrayList<NWR>();

		for (AggregationID aggregationID : currentPublicationProfile.getMatchedAggregationSet()) {
			result.addAll(getNWRs(aggregationID));
		}
		return result;

	}

	private void updateWaitingDelay(AggregationID aggregationID) {
		delay.put(aggregationID, Math.max(delay.get(aggregationID), (getCurrentTime() - currentPublicationProfile.getPublicationTime())));
	}

	private boolean processAggregatePublicationMessage() {

		//System.out.println(brokerCore.getBrokerID() + "");

		OptimalAggregationGraphNode pubNode = new OptimalAggregationGraphNode(currentPublicationProfile.getPublication(),
				currentPublicationProfile.getPublicationTime());

		updateWaitingDelay(((AggregatedPublication) currentPublicationProfile.getPublication()).getAggregationID());

		for (MessageDestination aggDest : currentPublicationProfile.getPureAggregateSubDestination()) {
			if (!aggDest.equals(currentPublicationProfile.getPublicationMessage().getLastHopID())) {
				TimeConstraintBipartiteGraph<OptimalAggregationGraphNode, Publication> g = graphs.get(aggDest);

				NWR n = new NWR(currentPublicationProfile.getPublicationMessage()); 
				n.messageDestination = aggDest;
				//if (!currentPublicationProfile.getPublicationMessage().getLastHopID().equals(n.getMessageDestination())) {
					//System.out.println(brokerCore.getBrokerID() + " Adds " + currentPublicationProfile.getPublication() + " to graph for " + aggDest);
				
								
				if(!perfectCollector.contains(n)){
					g.addVerticeAndEdge(new OptimalAggregationGraphNode(n, currentPublicationProfile.getPublicationTime()), pubNode);
					perfectCollector.add(n);
					if(perfectCollector.size()> PERFECT_COLLECTOR_SIZE){
						perfectCollector.remove(0);
					}
				}
					
				//} else
				//	System.out.println("********** Message " + currentPublicationProfile.getPublication() + " dropped for destination: "
				//			+ n.getMessageDestination());

			}
		}
		processPublicationMessageEND();
		return false;
	}

	private boolean processLatePublication() {

		System.out.println("[OPT] " + brokerCore.getBrokerID() + " publication is late " + currentPublicationProfile.getPublicationTime() + " RT: "
				+ getCurrentTime());
		Stats.getInstance("test").addValue("PUBLICATION.LATE", 1);
		for (MessageDestination d : currentPublicationProfile.getPureAggregateSubDestination()) {

			Publication p = currentPublicationProfile.getPublication().duplicate();
			notifier.SendRawPublication(p, d);

			for (AggregationID aggregationID : currentPublicationProfile.getMatchedAggregationSet()) {
				updateWaitingDelay(aggregationID);
				System.err.println(brokerCore.getBrokerID() + " delays of " + delay.get(aggregationID) + "ms for " + aggregationID);
			}

		}
		processPublicationMessageEND();
		return false;
	}

	private boolean addPublicationToGraph(List<NWR> nwrs) {
		OptimalAggregationGraphNode pubNode = new OptimalAggregationGraphNode(currentPublicationProfile.getPublication().duplicate(),
				currentPublicationProfile.getPublicationTime());
		boolean added = false;
		for (NWR n : nwrs) {

			if ((!currentPublicationProfile.getPublicationMessage().getLastHopID().equals(n.getMessageDestination()))
					&& (!currentPublicationProfile.getOverlappLink().contains(n.getMessageDestination()))) {
				TimeConstraintBipartiteGraph<OptimalAggregationGraphNode, Publication> graph = graphs.get(n.getMessageDestination());

				OptimalAggregationGraphNode aggPubNode = new OptimalAggregationGraphNode(n, n.getTimeStamp().getTime());
				added = true;
				graph.addVerticeAndEdge(aggPubNode, pubNode);
			}

		}

		return added;
	}

	private void processPublicationMessageEND() {
		Stats.getInstance("test").addValue("TIME.RECEIVE", getCurrentTime() - processAggregationStartTime);
	}

	@Override
	public void aggregatePublicationMessage() {

		/*
		 * System.out.println("[OPT] " + brokerCore.getBrokerID() +
		 * " publication received " +
		 * currentPublicationProfile.getPublicationTime() + " RT: " +
		 * System.currentTimeMillis());
		 */
		processAggregationStartTime = getCurrentTime();  
				//System.currentTimeMillis();

		if (currentPublicationProfile.isAggregatedPublication()) {
			processAggregatePublicationMessage();
			return;
		}

		List<NWR> nwrs = getAllNWRs();

		if (isPublicationLate(nwrs)) {
			processLatePublication();
			return;
		}

		addPublicationToGraph(nwrs);
		
		setAggregationRequiredOnlyForTheseLinks(currentPublicationProfile.getOverlappLink());
	}

	/*
	 * @Override public void notifyFromAggregatorTimer(int timecount) {
	 * getNotificationFromAggregator(null); }
	 */

	public boolean isPublicationLate(List<NWR> nwrs) {
		long curTime = getCurrentTime();//System.currentTimeMillis();
		if (nwrs.isEmpty())
			return true;
		for (NWR n : nwrs) {
			AggregationID aggID = n.getAggregationID();

			if (n.getTimeStamp().getTime() + delay.get(aggID) < curTime)
				//return true;
				return false; //publications are never late


		}
		return false;
	}

	// long sent=0;
	private void processGraphForDestination(MessageDestination md) {

		List<Publication> pubMsgList;

	
		TimeConstraintBipartiteGraph<OptimalAggregationGraphNode, Publication> graph = graphs.get(md);
		long d = 0;

		if (graph.isP1Empty()) {
			return;
		}

		d = delay.get(((AggregatedPublication) graph.partition1.get(0).getElement()).getAggregationID());
		

		while (!(pubMsgList = graph.getNewestCover(getCurrentTime(), d)).isEmpty()) {
		
			boolean aggPubSent=false;
			
			for (Publication p : pubMsgList) {
				if (p instanceof NWR) {
					NWR n = (NWR) p;
				
					AggregatedPublication agg = n.getAggregatedPublication();
					agg.setWindowID("" + n.getTimeStamp().getTime());
					// System.out.println("\t\tSent "+(++sent));
					notifier.SendPublication(agg, md);
				} else if (p instanceof AggregatedPublication) {
					AggregatedPublication agg = (AggregatedPublication) p;
					agg.setWindowID("" + agg.getTimeStamp().getTime());
					if(!aggPubSent)notifier.SendPublication(agg, md);
					aggPubSent=true;
				} else {
					
					notifier.SendRawPublication(p, md);
				}
				
			}
		}
	}

	public void Notify() {
		long startTime;

		// System.out.println(""+Thread.currentThread().getName()+" enters NTF");
		// Let's kidnap the AggregatorTimer thread.
		// while (true) {
		// System.out.println("NTF "+this.brokerCore.getBrokerID()+" is alive");
		startTime = System.currentTimeMillis();
		for (MessageDestination md : graphs.keySet()) {
			processGraphForDestination(md);

		}
		Stats.getInstance("test").addValue("TIME.NOTIFY", System.currentTimeMillis() - startTime);

		// System.out.println(""+Thread.currentThread().getName()+" leaves NTF");
	}

	// }

	public void setDelay(long i) {
		for (AggregationID id : delay.keySet()) {
			delay.put(id, i);
		}
	}

	public HashMap<MessageDestination, TimeConstraintBipartiteGraph<OptimalAggregationGraphNode, Publication>> getGraphs() {
		return graphs;
	}

	public HashMap<AggregationID, Long> getDelay() {
		return delay;
	}

	public long getMaxDelay() {
		long delay = 0;
		for (Long d : this.delay.values()) {
			if (delay < d)
				delay = d;
		}

		return delay;
	}

}
