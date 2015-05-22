package ca.utoronto.msrg.padres.broker.aggregation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.derby.tools.sysinfo;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.sun.tools.internal.ws.wsdl.document.jaxws.Exception;

import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationID;
import ca.utoronto.msrg.padres.broker.aggregation.aggregationcore.AggregationImplementationTypes;
import ca.utoronto.msrg.padres.broker.aggregation.aggregator.Aggregator;
import ca.utoronto.msrg.padres.broker.aggregation.message.AggregatedPublication;
import ca.utoronto.msrg.padres.broker.aggregation.utility.AggregationInfo;
import ca.utoronto.msrg.padres.broker.aggregation.utility.AggregationTimerService;
import ca.utoronto.msrg.padres.broker.brokercore.BrokerCore;
import ca.utoronto.msrg.padres.broker.router.Router;
import ca.utoronto.msrg.padres.broker.router.matching.MatcherException;
import ca.utoronto.msrg.padres.common.message.AdvertisementMessage;
import ca.utoronto.msrg.padres.common.message.Message;
import ca.utoronto.msrg.padres.common.message.MessageDestination;
import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.Subscription;
import ca.utoronto.msrg.padres.common.message.SubscriptionMessage;

public abstract class AbstractAggregationEngine implements AggregationEngine {

	protected static final int RANDOM_BIG_NUMBER = 1685674697;
	private static final long FORCE_OFFSET = 0;
	
	protected HashMap<String, ClientAggregator> clientAggregatorMap = new HashMap<String, ClientAggregator>();

	protected AggregationRoutingTable routingTable;
	
	protected Set<AggregationID> aggregationIDs;
	
	public AggregationRoutingTable getRoutingTable() {
		return routingTable;
	}

	protected PublicationProfile currentPublicationProfile;

	protected Router router;
	protected BrokerCore brokerCore;
	AggregationInfo agInfo;
	final int MILLISECOND = 1000;
	int DEFAULT_WAITING = 100;

	protected Notifier notifier;

	long largestShiftSize;

	protected Logger logger, exceptionLogger;

	boolean clientAggregation;

	AggregationTimerService aggregatorTimerService;
	private Set<Message> messageSetFromPadres;

	public AbstractAggregationEngine(BrokerCore brockerCore, AggregationInfo agInfo) {
		this.brokerCore = brockerCore;
		this.router = brockerCore.getRouter();
		largestShiftSize = RANDOM_BIG_NUMBER;
		notifier = new Notifier(brockerCore);
		if (agInfo != null)
			this.agInfo = agInfo;
		else
			this.agInfo = new AggregationInfo(DEFAULT_WAITING, AggregationImplementationTypes.LATE_AGGREGATION, true);

		// logger = Logger.getLogger(AbstractAggregationEngine.class);

		logger = Logger.getLogger(AbstractAggregationEngine.class);
		logger.setLevel(Level.INFO);
		exceptionLogger = Logger.getLogger("Exception");
		aggregatorTimerService = new AggregationTimerService(brokerCore.getBrokerConfig().getExperimentDuration());
		currentPublicationProfile = new PublicationProfile(this);
		routingTable=new AggregationRoutingTable(this);
		aggregationIDs=new HashSet<AggregationID>();
	}


	public AggregationTimerService getAggregatorTimerService() {
		return aggregatorTimerService;
	}

	@Override
	public abstract void aggregatePublicationMessage();

	protected long getStartOfTheWindow(SubscriptionMessage subMsg) {
		long startTimeWithOffsetCorrection = genStartOfTheWindow(subMsg.getSubscription().getAggregationID());
		long startOfTheAggregationWindow = subMsg.getStartOfTheAggregationWindow() == -1 ? startTimeWithOffsetCorrection : subMsg
				.getStartOfTheAggregationWindow(); 

		return startOfTheAggregationWindow;
	}

	public void updateLargestShiftSize(AggregationID aggregationID) {
		if (largestShiftSize == RANDOM_BIG_NUMBER)
			largestShiftSize = aggregationID.getShiftSize();
		else if (aggregationID.getShiftSize() > largestShiftSize)
			largestShiftSize = aggregationID.getShiftSize();
	}

	public void processSubscriptionMessage(SubscriptionMessage subMsg1, Set<Message> outGoingmessageSet1) {
		
		if (subMsg1.isControl())
			return;
		 
		SubscriptionMessage subMsg = subMsg1.duplicateAGG();
		
		 Set<Message> outGoingmessageSet = new HashSet<Message>();
		 
		 for(Message message : outGoingmessageSet1) {
			 if( message instanceof SubscriptionMessage)
				 outGoingmessageSet.add( ((SubscriptionMessage)message).duplicateAGG());
			 else {
				System.out.println("SUCIDE");
				System.exit(-1);
			 }
		 }
		 

		System.out.println(subMsg);
		
		if (!subMsg.getLastHopID().isBroker() && brokerCore.getBrokerConfig().getAggregationInfo().isClientAggregation()) {
			addClient(subMsg);
			System.out.println("[ABS_AGR_ADDING_CLIENT]");
			return;
		}
		
		
		registerSubMessageForRouting(subMsg, outGoingmessageSet);

	}
	
	protected void registerSubMessageForRouting(SubscriptionMessage subMsg, Set<Message> outGoingmessageSet){
		Subscription sub = subMsg.getSubscription();
		
		if (sub.isAggregation()) {
			aggregationIDs.add(sub.getAggregationID());
			logger.info("[AGE" + brokerCore.getBrokerID() + "]processSubscriptionMessage=>" + subMsg.getSubscription().getPredicateMap().toString());

			for (Message outMessage : outGoingmessageSet) {
				routingTable.registerSubscription((SubscriptionMessage) outMessage);			
			}
			
			if (!subMsg.getLastHopID().isBroker() &&!brokerCore.getBrokerConfig().getAggregationInfo().isClientAggregation()&& outGoingmessageSet.isEmpty()){
				routingTable.registerSubscription(subMsg);
			}
			
			
			registerSubForPublisher(subMsg);
		}
		
		
	}
	

	private void registerSubForPublisher(SubscriptionMessage subMsg){
		boolean publisherFound = false;
		Set<String> publishers=null;
		try {
			publishers = router.getMatcher().getMatchingAdvFromSubs(subMsg);
		} catch (MatcherException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (String publisher : publishers) {
			AdvertisementMessage adm = router.getAdvertisement(publisher);
			if (adm.getLastHopID().getBrokerId().equals(brokerCore.getBrokerDestination().getBrokerId())) {
				publisherFound = true;
				break;
			}
		}
		if (publisherFound) {
			subMsg.setNextHopID(brokerCore.getBrokerDestination());
			routingTable.registerSubscription(subMsg);
			
		}
		
	}
	
	protected long genStartOfTheWindow(AggregationID aggregationID) {
		long currentTime = System.currentTimeMillis();
		long startTimeWithOffsetCorrection = currentTime - (currentTime % (aggregationID.getShiftSize() * MILLISECOND));
		return startTimeWithOffsetCorrection;
	}

	

	public Notifier getNotifier() {
		return notifier;
	}

	public void setNotifier(Notifier notifier) {
		this.notifier = notifier;
	}

	



	protected void forwardRawMessage() {
		//this function need to change for per outgoing link
		//setAggregationRequiredForMassageSet(true);
		setAggregationRequiredOnlyForTheseLinks(currentPublicationProfile.getOverlappLink());
		for (MessageDestination dest : currentPublicationProfile.getPureAggregateSubDestination()) {
			Publication p = currentPublicationProfile.getPublication().duplicate();
			logger.info("[AGE]" + this.brokerCore.getBrokerID() + " forwared raw message to destination " + dest.toString());
			notifier.SendRawPublication(p, dest);
		}

	}


	protected void setAggregationRequiredOnlyForTheseLinks(
			Set<MessageDestination> overlappLink) {	
		for(Message message : messageSetFromPadres) {
			 if( message instanceof PublicationMessage && overlappLink.contains(message.getNextHopID())) {
				 ((PublicationMessage) message).setAggregationRequired(true);
			 }
		} 
		
	}


	public void addClient(SubscriptionMessage subMsg) {
		long startOfTheAggregationWindow = getStartOfTheWindow(subMsg);
		startOfTheAggregationWindow -= FORCE_OFFSET;
		ClientAggregator clientAggregator = new ClientAggregator(subMsg, startOfTheAggregationWindow, brokerCore, aggregatorTimerService);
		
		String subMsgID =subMsg.getMessageID();
		if (subMsgID.length() > 0 && subMsgID.charAt(subMsgID.length()-1)=='x') {
			subMsgID = subMsgID.substring(0, subMsgID.length()-1);
		  }
		
		clientAggregatorMap.put(subMsgID, clientAggregator);
		//aggregatorTimerService.registerAggregator(clientAggregator.getAggregator());
	}

	private void processPublicationForClient() {
		AggregationID aggregationID;
		if (currentPublicationProfile.isAggregatedPublication()) {
			aggregationID = ((AggregatedPublication) currentPublicationProfile.getPublication()).getAggregationID();
			ClientAggregator aggregator = clientAggregatorMap.get(aggregationID.getSubID());
			if (aggregator != null)
				aggregator.getCollector().addMsg(currentPublicationProfile.getPublicationMessage());

		} else {
			Iterator<MessageDestination> it=currentPublicationProfile.getPureAggregateSubDestination().iterator();
			while(it.hasNext()){
				MessageDestination md = it.next();
				if (!md.isBroker()) {
					for(String subId:currentPublicationProfile.getSubscriptionID()){
						ClientAggregator aggregator = clientAggregatorMap.get(subId);
						Aggregator agr= aggregator.getAggregator();
						agr.addPublication(currentPublicationProfile.getPublication());
						currentPublicationProfile.removeFromMatchedAggregationIDSet(aggregator.getAggregator().getAggregationID());
					}
					it.remove();
					
				}

			}

		}
	}

	//This is quick fix for adaptive case, specially when it forward raw message
	// we need to set back that it require aggregation
	private void setAggregationRequiredForMassageSet(boolean value) {
		for(Message message : messageSetFromPadres) {
			 if( message instanceof PublicationMessage) {
				 ((PublicationMessage) message).setAggregationRequired(value);
			 }
		}
	}

	@Override
	public void processPublicationMessage(PublicationMessage msg1, Set<Message> messageSet1) {
 
		if ( msg1.isControl())
			return;
		PublicationMessage msg = msg1.duplicateAGG();
		
		 Set<Message> messageSet = new HashSet<Message>();

		//this function need to change for per outgoing link
		 messageSetFromPadres = messageSet1;
		 setAggregationRequiredForMassageSet(false);
		 
		 for(Message message : messageSet1) {
			 if( message instanceof PublicationMessage) {
				// ((PublicationMessage) message).setAggregationRequired(false);
				 messageSet.add( ((PublicationMessage)message).duplicateAGG());
			 }
			 else {
				System.out.println("SUCIDE");
				System.exit(-1);
			 }
		 }

		System.out.println("[DEBUG-PUB1 " + msg.toString());
		System.out.println("[DEBUG-MessageSETSIZE1]"+ messageSet.size());
		for( Message message : messageSet) {
			System.out.println("[DEBUG-MessageSET1]"+((PublicationMessage)message).toString());
		}
		
		currentPublicationProfile.generateProfile(msg, messageSet);

		if (brokerCore.getBrokerConfig().getAggregationInfo().isClientAggregation()) {
			processPublicationForClient();
		}

		if (!currentPublicationProfile.getMatchedAggregationSet().isEmpty()){//.isUnmatched())
			aggregatePublicationMessage();
			//System.out.println("[NKDEBUG]11");
		}

		System.out.println("[DEBUG-PUB2 " + msg.toString());
		for( Message message : messageSet) {
			System.out.println("[DEBUG-MessageSET2]"+((PublicationMessage)message).toString());
		}
		System.out.println("[DEBUG-MessageSETSIZE2]"+ messageSet.size());
	}

	public PublicationProfile getCurrentPublicationProfile() {
		return currentPublicationProfile;
	}


	public Set<AggregationID> getAggregationIDs() {
		return aggregationIDs;
	}

}
