package ca.utoronto.msrg.padres.demo.dataset;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;

import javax.swing.Timer;

import ca.utoronto.msrg.padres.client.ClientConfig;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.message.Message;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.Subscription;
import ca.utoronto.msrg.padres.common.message.SubscriptionMessage;
import ca.utoronto.msrg.padres.common.message.parser.ParseException;
import ca.utoronto.msrg.padres.demo.traffic.TrafficDataFileds;

public abstract class DatasetSubscriberClient extends DatasetStepClient {

	public final static long DELAY_BETWEEN_NEW_SUBSCRIPTION = 1000; // ms

	private int NoOfSubscribers = 1;

	// Command-line argument-related constants

	public static final String SUB_DELIMIT_CHAR = "/";

	protected  int window = 10;

	protected  int notification = 5;
	
	private int AGG_SYMBOL_OFFSET=50;

	// For duplicate detection Accounting
	// private final LinkedHashMap<Publication, Object> messageHistory;

	// Synchronization/mutex/locks
	private final Object avgDelaySync = new Object();

	private final Object logSync = new Object();

	// Holds this subscriber's subscription
	private Map<String, Subscription> subId2SubMap;

	// Logging
	private final Timer loggingTimer;

	private int duplicates;

	// Performance Measurements
	private double minDelay;

	private double maxDelay;

	private long received;

	private double cumulativeDelay;

	private int symbolIndex;

	Random rand;

	private int aggregateSubscriptionPercent;
	int experimentDuration;
	private String operator;
	public ArrayList<String> symbols;

	public abstract boolean sendSubscriptions(String[] rates,
			ArrayList<String> symbols);

	public abstract String getNormalSubscription(int index,
			ArrayList<String> symbols, String[] rates);

	public abstract String getAggregateSubscription(int index,
			ArrayList<String> symbols, String[] rates, String operator);

	public abstract boolean sendSubscriptionsFromList(String[] rates);

	public abstract boolean sendSubscriptionsFromSymbols(String[] rates);

	/*
	 * Merely just instantiating the timer object, have not started the timer
	 * yet!
	 */

	public DatasetSubscriberClient(ClientConfig clientConfig, int replication,
			int index, String[] rates, ArrayList<String> symbols,
			String operator, int normalSubscriptionPercent,  Random rand) throws ClientException {
		super(clientConfig);
		// send subscriptions
		NoOfSubscribers = replication;
		this.symbolIndex = index;
		this.operator = operator;
		this.aggregateSubscriptionPercent = normalSubscriptionPercent;
		this.experimentDuration = clientConfig.getExperimentDuration();
		this.rand = rand;
		this.symbols = symbols;
		this.window = clientConfig.getWindow();
		this.notification = clientConfig.getNotification();
		sendSubscriptions(rates, symbols);

		// duplicate detection stuff
		duplicates = 0;
		// messageHistory = new LinkedHashMap<Publication, Object>(
		// MAX_DUPLICATE_HISTORY_SIZE, (float) 0.75, true) {
		//
		// private static final long serialVersionUID = 1L;
		//
		// // We want to limit the history size to MAX_HISTORY_SIZE
		// protected boolean removeEldestEntry(
		// Map.Entry<Publication, Object> eldest) {
		// return size() > MAX_DUPLICATE_HISTORY_SIZE;
		// }
		// };
		// performance metrics
		minDelay = Double.MAX_VALUE;
		maxDelay = Double.MIN_VALUE;
		received = 0;
		cumulativeDelay = 0;
		// For logging
		loggingTimer = setupLoggingTimer();
	}

	private Timer setupLoggingTimer() {
		ActionListener taskPerformer = new ActionListener() {

			public void actionPerformed(ActionEvent evt) {
				synchronized (logSync) {
					logSync.notify();
				}
			}
		};
		return new Timer(clientConfig.logPeriod * 1000, taskPerformer);
	}

	/*
	 * Called by the timer to do logging whenever the logging period is up.
	 */
	private void doLogging() {
		// Calculate the average delay
		double avgDelay = 0;

		synchronized (avgDelaySync) {
			if (received > 0) {
				avgDelay = cumulativeDelay / (double) received;
				cumulativeDelay = 0;
				received = 0;
				// Can't compute the average if no messages were received
			} else {
				avgDelay = 0;
			}
			avgDelaySync.notifyAll();
		}

		clientLogger.info(Math.round(Calendar.getInstance(
				TimeZone.getTimeZone("GMT-5")).getTimeInMillis() / 1000.0)
				+ "\t"
				+ avgDelay
				+ "\t"
				+ (minDelay == Double.MAX_VALUE ? 0 : minDelay)
				+ "\t"
				+ (maxDelay == Double.MIN_VALUE ? 0 : maxDelay)
				+ "\t"
				+ duplicates);

		// reset some metrics
		minDelay = Double.MAX_VALUE;
		maxDelay = Double.MIN_VALUE;
	}

	/**
	 * Nothing else except do periodic logging
	 * 
	 */
	public void run() {
		// write column headers into log file
		// logger.info("# Avg delay | Min delay | Max delay | Duplicates");
		syncStartLoggingTimer();

		Long currTime = System.currentTimeMillis();
		// Wait till the period has come to log
		synchronized (logSync) {
			while ((System.currentTimeMillis() - currTime) > 1000 * experimentDuration) {
				try {
					logSync.wait();
				} catch (InterruptedException e) {
				}
				doLogging();
			}
		}
		shutdown();
	}


	/*
	 * Want to sync the logging to the start of every minute
	 */
	private void syncStartLoggingTimer() {
		Calendar now = Calendar.getInstance(TimeZone.getTimeZone("GMT-5"));
		int seconds = now.get(Calendar.SECOND);
		int milliseconds = now.get(Calendar.MILLISECOND);
		long waitTimeInMs = ((59 - seconds) * 1000) + (1000 - milliseconds);
		try {
			Thread.sleep(waitTimeInMs);
		} catch (Exception e) {
			e.printStackTrace();
		}
		loggingTimer.start();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see universal.rmi.RMIUniversalClient#shutdown()
	 */
	public void shutdown() {
		loggingTimer.stop();
		disconnectAll();
		// doLogging();
	}

	public String getAverageValueFromPublication(String dataFilePath,
			int records, int fieldIndex) {
		String s[];
		float sum = 0;
		try {
			FileReader fr = new FileReader(dataFilePath);
			BufferedReader br = new BufferedReader(fr);
			String currentRecord;
			int noOfRecords = 1;
			while (((currentRecord = br.readLine()) != null)
					&& noOfRecords <= records) {
				s = currentRecord.split(",");
				sum += Float.parseFloat(s[fieldIndex].replaceFirst("]", ""));
				// OCCU_PER 17 speed 19 No of Vehicle 21
				noOfRecords++;
			}
			br.close();
			float average = sum / noOfRecords;
			double randValue = rand.nextGaussian();
			while (randValue > 1.0 || randValue < -1.0) {
				randValue = rand.nextGaussian();
			}
			average *= ((randValue + 1) / 2);
			return String.format("%.2f", average);

		} catch (IOException e) {
			e.printStackTrace();
		}
		return "" + 0;

	}

	public boolean sendSubscriptionsFromList(String[] stockRates,
			String filePath, String field) throws ClientException, IOException {

		BufferedReader reader = null;

		reader = new BufferedReader(new FileReader(filePath));

		List<String> subscriptionsList = new ArrayList<String>();
		String str = reader.readLine();

		while ((str = reader.readLine()) != null) {
			subscriptionsList.add(str);
		}

		reader.close();
		
		
		int symbolSize = subscriptionsList.size();
		int normalSubscriptions = NoOfSubscribers;
		int aggregateSubscriptions = (aggregateSubscriptionPercent * NoOfSubscribers) / 100;
		subId2SubMap = new HashMap<String, Subscription>();
		int index = 0;
		for (String brokerURI : getBrokerURIList()) {

			for (int i = 0; i < normalSubscriptions; i++) {
				index = ((i + symbolIndex) % symbolSize);
				String subStr = subscriptionsList.get(index);
				sendSubscription(subStr, brokerURI, index);

			}

			for (int i = 0; i < aggregateSubscriptions; i++) {
				index = ((i + symbolIndex + AGG_SYMBOL_OFFSET) % symbolSize);

				String subStr = subscriptionsList.get(index + normalSubscriptions)
						+ ",[AGR,eq,'" + operator + "'],[PAR,eq," + field
						+ "],[PRD,eq,'"+window+"'],[NTF,eq,'"+notification+"']";
				sendSubscription(subStr, brokerURI, index);

			}
		}
		return true;
	}
	
	public boolean sendSubscriptionsFromListAggregatePercent(String[] rates,
			String filePath, String field) throws ClientException, IOException {

		BufferedReader reader = null;

		reader = new BufferedReader(new FileReader(filePath));

		List<String> subscriptionsList = new ArrayList<String>();
		String str = reader.readLine();

		while ((str = reader.readLine()) != null) {
			subscriptionsList.add(str);
		}

		reader.close();
		
		
		int symbolSize = subscriptionsList.size();
		int aggregateSubscriptions = (aggregateSubscriptionPercent * NoOfSubscribers) / 100;
		int normalSubscriptions = NoOfSubscribers-aggregateSubscriptions;
		subId2SubMap = new HashMap<String, Subscription>();
		int index = 0;
		for (String brokerURI : getBrokerURIList()) {

			for (int i = 1; i < NoOfSubscribers+1; i++) {
				index = ((i + symbolIndex) % symbolSize);
				String subStr = subscriptionsList.get(index);
				if(i>normalSubscriptions)  
					subStr = subStr
						+ ",[AGR,eq,'" + operator + "'],[PAR,eq," + field
						+ "],[PRD,eq,'"+window+"'],[NTF,eq,'"+notification+"']";
				sendSubscription(subStr, brokerURI, index);

			}
		}
		return true;
	}

	public boolean sendSubscriptionsFromClassifiedList(String[] stockRates,
			String filePathNormal, String filePathAggregate, String field)
			throws ClientException, IOException {

		BufferedReader readerNormal = null;

		readerNormal = new BufferedReader(new FileReader(filePathNormal));

		List<String> subscriptionsListNormal = new ArrayList<String>();
		String strNor;// = readerNormal.readLine();

		while ((strNor = readerNormal.readLine()) != null) {
			subscriptionsListNormal.add(strNor);
		}

		BufferedReader readerAggregated = null;

		readerAggregated = new BufferedReader(new FileReader(filePathAggregate));

		List<String> subscriptionsListAggregated = new ArrayList<String>();
		String strAgg = readerAggregated.readLine();

		while ((strAgg = readerAggregated.readLine()) != null) {
			subscriptionsListAggregated.add(strAgg);
		}

		readerNormal.close();
		readerAggregated.close();

		//int symbolSize = subscriptionsListNormal.size();

		int normalSubscriptions = NoOfSubscribers;
		int aggregateSubscriptions = (aggregateSubscriptionPercent * NoOfSubscribers) / 100;
		//  Following two lines must be deleted after this experiment 
	//	normalSubscriptions = 0;
		//aggregateSubscriptions = NoOfSubscribers;
		subId2SubMap = new HashMap<String, Subscription>();
		int index = 0;
		for (String brokerURI : getBrokerURIList()) {

			for (int i = 0; i < normalSubscriptions; i++) {
				index = ((i + symbolIndex)  );
				String subStr = subscriptionsListNormal.get(index);
				sendSubscription(subStr, brokerURI, index);

			}

			for (int i = 0; i < aggregateSubscriptions; i++) {
				index = ((i + symbolIndex) );

				String subStr = subscriptionsListAggregated.get(index)
						+ ",[AGR,eq,'" + operator + "'],[PAR,eq," + field
						+ "],[PRD,eq,'"+ window +"'],[NTF,eq,'"+ notification+"']";
				sendSubscription(subStr, brokerURI, index);

			}
		}
		return true;
	}
 

	private void sendSubscription(String subStr, String brokerURI, int index) {
		System.out.println(subStr + "symbolIndex" + symbolIndex + "index"
				+ index);
		try {
			SubscriptionMessage subMsg = subscribe(subStr, brokerURI);
			subId2SubMap.put(subMsg.getMessageID(), subMsg.getSubscription());
			Thread.sleep(DELAY_BETWEEN_NEW_SUBSCRIPTION);
		} catch (ParseException e) {
			exceptionLogger.error(e.getMessage());
		} catch (ClientException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}


	public boolean sendSubscriptionsFromSymbols(String[] rates,
			ArrayList<String> symbols) {
		int normalSubscriptions = NoOfSubscribers;
		int aggregateSubscriptions = (aggregateSubscriptionPercent * NoOfSubscribers) / 100;
		subId2SubMap = new HashMap<String, Subscription>();
		int index = 0;
		for (String brokerURI : getBrokerURIList())
			// for (String subStr : subStrList) {
			for (int i = 0; i < (normalSubscriptions + aggregateSubscriptions); i++) {
				index = ((i + symbolIndex) % symbols.size());

				String subStr;
				if (i < normalSubscriptions)
					subStr = getNormalSubscription(index, symbols, rates);
					
				else
					subStr = getAggregateSubscription(index, symbols, rates,
							operator);

				System.out.println(subStr + "symbolIndex" + symbolIndex
						+ "index" + index);
				SubscriptionMessage subMsg;
				try {
					subMsg = subscribe(subStr, brokerURI);

					subId2SubMap.put(subMsg.getMessageID(),
							subMsg.getSubscription());
				} catch (ClientException e1) { 
					e1.printStackTrace();
				} catch (ParseException e1) { 
					e1.printStackTrace();
				}
				try {
					Thread.sleep(DELAY_BETWEEN_NEW_SUBSCRIPTION);
				} catch (Exception e) {
				}
				// }
			}
		return true;
	}

	

	public String getAverageIntegerValueFromPublication(String data_file,
			int records, TrafficDataFileds TDF) {
		String s[];
		float sum = 0;
		try {
			FileReader fr = new FileReader(getDataPath() + data_file);
			BufferedReader br = new BufferedReader(fr);
			String currentRecord;
			int noOfRecords = 1;
			while (((currentRecord = br.readLine()) != null)
					&& noOfRecords <= records) {
				s = currentRecord.split(",");
				sum += Float
						.parseFloat(s[TDF.getValue()].replaceFirst("]", ""));
				// OCCU_PER 17 speed 19 No of Vehicle 21
				noOfRecords++;
			}
			br.close();
			float average = sum / noOfRecords;
			double randValue = rand.nextGaussian();
			while (randValue > 1.0 || randValue < -1.0) {
				randValue = getRand().nextGaussian();
			}
			average *= ((randValue + 1) / 2);
			return "" + Math.round(average);

		} catch (IOException e) {
			e.printStackTrace();
		}
		return "" + 0;

	}
	public double getMinDelay() {
		return minDelay;
	}

	public void setMinDelay(double value) {
		minDelay = value;
	}

	public double getMaxDelay() {
		return maxDelay;
	}

	public void setMaxDelay(double value) {
		maxDelay = value;
	}

	public Random getRand() {
		return rand;
	}

}
