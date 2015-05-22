package ca.utoronto.msrg.padres.demo.dataset;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import ca.utoronto.msrg.padres.client.BrokerState;
import ca.utoronto.msrg.padres.client.ClientConfig;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.message.Advertisement;
import ca.utoronto.msrg.padres.common.message.AdvertisementMessage;
import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.parser.MessageFactory;
import ca.utoronto.msrg.padres.common.message.parser.ParseException;

public abstract class DatasetPublisherClient extends DatasetStepClient implements Runnable{

	private final String symbol;
	
	// Keeps track of the brokers that we visited
	protected LinkedList<String> brokerHistory = null;

	// CONSTANTS
	// Size of the hash map in publication pay load.
	protected final short DEFAULT_CONNECTION_CAPACITY = 4; // assume likely 3
															// connections

	public final String TOPIC_CLIENT_CONTROL = "CLIENT_CONTROL";

	public final String CMD_CONNECT = "CONNECT";

	public final String CMD_DISCONNECT = "DISCONNECT";

	// ///////FROM STOCK AND TRAFFIC CLIENT////////////////////
	public final String PADRES_HOME = System.getenv("PADRES_HOME") == null ? "."
			+ File.separator
			: System.getenv("PADRES_HOME") + File.separator;

	protected final String DEFAULT_PROPS_FILE_PATH = PADRES_HOME + "etc"
			+ File.separator + "broker.properties";

	private int REPLICATION = 1;

	// Time in between publishing in milliseconds.
	private final long publishPeriod;

	private final double rate; // just for reference when logging

	// Advertisement for this publisher. This doesn't change after it is
	// initialized
	private final Advertisement advertisement;

	// Useful when we want to disconnect from a specified broker and when we
	// want to send a
	// publication to all of our current connected brokers
	private final HashMap<String, List<String>> brokerIdAdvIdMap;

	// Used for reading in publications to publish
	private BufferedReader reader;

	// The number of publication sent
	private long pubCount;

	// The number of times the entire publication file was published
	private int publicationRounds;

	private Publication lastPublished = null;

	private boolean stop = false;

	// to read from a file or just to repeat one publication again and again
	// it is a hack for profiling experiments; keep it "false" for normal
	// StockPublisher functions
	private boolean repeatPub = false;

	private String finishDir;

	Properties properties;

	private int experimentDuration; 
	private int emptyingTime;

	protected abstract String getDefaultAdvertisement(); 

	protected abstract void customizeAdvertisement(Advertisement adv);

	private void setup(ClientConfig clientConfig) throws ClientException {

		if (brokerHistory == null) {
			brokerHistory = new LinkedList<String>();
		}

	}

	public DatasetPublisherClient(ClientConfig clientConfig, String pubSymbol,
			double pubRate, int replication, String finishDir) throws ClientException {
		super(clientConfig);
		this.symbol = pubSymbol;
		setup(clientConfig);
		rate = pubRate;
		this.finishDir = finishDir;
		publishPeriod = toMilliSecondPeriod(rate); // convert to pub/sec
		brokerIdAdvIdMap = new HashMap<String, List<String>>(
				DEFAULT_CONNECTION_CAPACITY);
		pubCount = 0;
		publicationRounds = 0;
		REPLICATION = replication;
		properties = new Properties();
		try {
			properties.load(new FileInputStream(DEFAULT_PROPS_FILE_PATH));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		experimentDuration = clientConfig.getExperimentDuration();
		emptyingTime = clientConfig.getEmptyingTime();
		initializeReader(getDataFilePath(symbol));
		// advertise to the brokers and update our bookkeeping
		advertisement = initializeAdvertisement();
		for (String brokerURI : getBrokerURIList()) {

			AdvertisementMessage advMsg = null;
			List<String> advMsgList = new ArrayList();
			for (int i = 0; i < REPLICATION; i++) {
				advMsgList.add(advertise(advertisement, brokerURI)
						.getMessageID());

			}
			brokerIdAdvIdMap.put(brokerURI, advMsgList);
		}
	}

	/*
	 * Makes an advertisement by examining the first publication in the
	 * publication file
	 */
	private Advertisement initializeAdvertisement() {
		Advertisement adv = null;
		try {
			String pubString = reader.readLine();
			pubString += ",[num,0]";
			adv = Advertisement.toAdvertisement(pubString);
			// change op for attribute symbol from "isPresent" to "eq"
			customizeAdvertisement(adv);

		} catch (Exception e) {
			System.out.println(e);
			System.out
					.println("Failed: (Publisher) Error making advertisement.  Using default:"
							+ getDefaultAdvertisement());

			try {
				adv = MessageFactory
						.createAdvertisementFromString(getDefaultAdvertisement());
			} catch (ParseException e1) {
				exceptionLogger.error(e.getMessage());
				return null;

			}
		}

		return adv;
	}

	private void initializeReader(String filePath) {
		reader = null;
		try {
			exceptionLogger.debug("Initialize reader count: "
					+ publicationRounds);
			reader = new BufferedReader(new FileReader(filePath));
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}

	private long toMilliSecondPeriod(double msgsPerMinute) {
		return Math.round(1000.0 / (msgsPerMinute / 60.0));
	}

	public void run() {
		System.out.println("[ PUBLISHER] Going to monitor file " + finishDir
				+ "startPublish.signal");
		File trigger = new File(finishDir + "startPublish.signal");
		if (trigger.exists())
			monitorFile(trigger);
		else {
			System.err.println("[PUBLISHER] No file to monitor");
			pause(5000);
		}
		pause(clientConfig.getWindow()-(System.currentTimeMillis()%clientConfig.getWindow()));
		Long publicationStartTime = System.currentTimeMillis();
		while (!stop) {
			// let's delay first so that we give the advertisement some time to
			// propagate
			pause(publishPeriod - (System.currentTimeMillis()%publishPeriod) );
			// Time to publish!
			for (int i = 0; i < REPLICATION; i++)
				publish();

			if ((System.currentTimeMillis() - publicationStartTime) > (1000 * experimentDuration)
					- 1000*emptyingTime)
				stop = true;
		} 
		shutdown();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see universal.rmi.RMIUniversalClient#shutdown()
	 */
	public void publish() {
		String publicationStr = null;

		// If the end of the file is reached, go back to the first line
		try {
			if ((publicationStr = reader.readLine()) == null) {
				stop = true;
				 System.out.println("Publisher for " + symbol + " STOPPED");
			} else {
				System.out.println("Either reached EOF or no file to read");
			}
		} catch (IOException e) {
			exceptionLogger.error(e.getMessage());
		}

		publicationStr += ",[num," + pubCount + "]";

		// Send the publication to all brokers that we're connected to
		for (String brokerURI : brokerIdAdvIdMap.keySet()) {
			Publication pub = null;
			PublicationMessage pubId = null;

			try {
				pub = MessageFactory
						.createPublicationFromString(publicationStr);
				lastPublished = pub;
				pubId = null;
				pubId = super.publish(pub, brokerURI);
				exceptionLogger.debug("Sent publication (" + pubId + "): "
						+ pub);
				pubCount++;
			} catch (ClientException e) {
				exceptionLogger.error(e.getMessage());
			} catch (ParseException e) {
				exceptionLogger.error(e.getMessage()
						+ publicationStr.toString());
			}
		}
	}

	private void monitorFile(File file) {
		final int POLL_INTERVAL = 100;
		FileReader reader;
		try {
			reader = new FileReader(file);

			BufferedReader buffered = new BufferedReader(reader);

			while (true) {
				String line = buffered.readLine();
				if (line == null) {
					Thread.sleep(POLL_INTERVAL);
				} else {
					System.out
							.println("[Traffic_PUBLISHER] Publishing started");
					reader.close();
					return;
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void shutdown() {
		StringBuffer output = new StringBuffer();
		output.append("Summary log:\n");
		output.append("Publisher ID  	: " + getClientID() + "\n");
		// output.append("Publication   	: " + symbol + "\n");
		output.append("Publication Rate	: " + rate + "msgs/min\n");
		output.append("Broker History	: " + getBrokerHistory() + "\n");
		output.append("Messages sent 	: " + pubCount + "\n");
		output.append("Rounds        	: " + publicationRounds + "\n");
		output.append("Last published   : " + lastPublished + "\n");

		exceptionLogger.info(output);
		disconnectAll();
		// dummyObj.notify();

	}

	/*
	 * Initializes the reader to start reading from the beginning of the file
	 */

	private void pause(long delay) {
		try {
			Thread.sleep(delay);
		} catch (Exception e) {
		}
	}

	/**
	 * Returns a list of all broker IDs of brokers that serviced this client in
	 * the past
	 * 
	 * @return
	 */
	public String getBrokerHistory() {
		return brokerHistory.toString();
	}

	/**
	 * Upon stopping or exiting the simulator
	 * 
	 */
	public void terminate() {
		brokerHistory.clear();
	}

	public BrokerState connect(String addr) throws ClientException {
		BrokerState brokerState = super.connect(addr);

		// record the set of brokers visited
		if (brokerState != null) {
			if (brokerHistory == null) {
				brokerHistory = new LinkedList<String>();
			}
			brokerHistory.addLast(brokerState.getBrokerAddress().getNodeURI());
		}

		return brokerState;
	}

	public String getSymbol(){
		return symbol;
	}
}
