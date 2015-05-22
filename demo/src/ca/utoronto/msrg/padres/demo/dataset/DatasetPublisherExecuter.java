package ca.utoronto.msrg.padres.demo.dataset;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import ca.utoronto.msrg.padres.client.Client;
import ca.utoronto.msrg.padres.client.ClientConfig;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.util.CommandLine;
import ca.utoronto.msrg.padres.common.util.LogException;
import ca.utoronto.msrg.padres.common.util.LogSetup;
import ca.utoronto.msrg.padres.demo.stockquote.StockPublisher;
import ca.utoronto.msrg.padres.demo.traffic.TrafficPublisher;

public class DatasetPublisherExecuter {

	// Command-line argument-related constants
	private final static String CMD_ARG_SYMBOL = "s";

	private final static String CMD_ARG_RATE = "r";

	private final static String CMD_ARG_PUB_DELAY = "d";

	protected final static String DEFAULT_CLIENT_ID = getCurrentDateTime();

	private final static String DEFAULT_PUBLISHER_ID = "P" + DEFAULT_CLIENT_ID;

	private final static String DEFAULT_PUB_RATE = "1.0"; // msg/min

	private final static String DEFAULT_PUB_DELAY = "0";

	private final static long DELAY_BETWEEN_NEW_PUBLISHER = 100; // ms

	// make sure .split() uses "\\" for special chars
	public final static String PUB_DELIMIT_CHAR = "/";

	static String[] dataSymbols;
	static String[] dataRates;
	static CommandLine cmdLine;
	static String pubDelay;
	static int replication = 1;
	static String finishDir;
	static String datasetType="TRAFFIC";

	public static void loader(String[] args) {
		try {
			new LogSetup(null);
		} catch (LogException e) {
			e.printStackTrace();
			System.exit(1);
		}

		// interceptor to extract no of replication
		final List<String> list = new ArrayList<String>();
		// defalut value of replicaiton

		Collections.addAll(list, args);
		Iterator itr = list.iterator();

		while (itr.hasNext()) {
			String curr = (String) itr.next();
			if (curr.equals("-NP")) {
				itr.remove();
				replication = Integer.parseInt((String) itr.next());
				itr.remove();
			}
			if (curr.equals("-FINISH_DIR")) {
				itr.remove();
				finishDir = (String) itr.next();
				itr.remove();
			}
			if (curr.equals("-DATASET")) {
				itr.remove();
				datasetType = (String) itr.next();
				itr.remove();
			}
		}
		if (finishDir == "")
			finishDir = ClientConfig.PADRES_HOME + "finish";
		args = list.toArray(new String[list.size()]);

		String[] cliKeys = ClientConfig.getCommandLineKeys();
		String[] fullCLIKeys = new String[cliKeys.length + 3];
		System.arraycopy(cliKeys, 0, fullCLIKeys, 0, cliKeys.length);
		fullCLIKeys[cliKeys.length] = CMD_ARG_SYMBOL + ":";
		fullCLIKeys[cliKeys.length + 1] = CMD_ARG_RATE + ":";
		fullCLIKeys[cliKeys.length + 2] = CMD_ARG_PUB_DELAY + ":";
		cmdLine = new CommandLine(fullCLIKeys);
		try {
			cmdLine.processCommandLine(args);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		String rateString = cmdLine.getOptionValue(CMD_ARG_RATE,
				DEFAULT_PUB_RATE);
		String dataString = cmdLine.getOptionValue(CMD_ARG_SYMBOL);
		pubDelay = cmdLine.getOptionValue(CMD_ARG_PUB_DELAY, DEFAULT_PUB_DELAY);

		// Now that logging is set up, we can start logging
		Logger logger = Logger.getLogger(Client.class);
		// Exit if data symbol is not specified
		if (dataString == null) {
			logger.fatal("Don't know what to publish because data String not specified.");
			showUsage();
			System.exit(1);
			// Exit if no broker address is specified
		} else if (cmdLine.getOptionValue(ClientConfig.CLI_OPTION_BROKER_LIST) == null) {
			logger.fatal("Don't know which broker to connect to.");
			showUsage();
			System.exit(1);
		}

		dataSymbols = dataString.split(PUB_DELIMIT_CHAR);
		dataRates = rateString.split(PUB_DELIMIT_CHAR);

		if (dataSymbols.length != dataRates.length) {
			System.err
					.println("Number of stock symbols does not match with number of rates provided! "
							+ dataSymbols.length + " != " + dataRates.length);
			System.exit(1);
		}

	}

	public static void start(String[] args) {

		DatasetPublisherClient[] publishers = createPublisherArray();
		Thread pubThread[] = new Thread[dataSymbols.length];
		for (int i = 0; i < dataSymbols.length; i++) {
			// Before logging and exiting with error, need to instantiate the
			// publisher
			// class so that logging is initialized before we use it
			try {
				ClientConfig clientConfig = new ClientConfig();
				clientConfig.clientID = DEFAULT_PUBLISHER_ID;
				clientConfig.overwriteWithCmdLineArgs(cmdLine);
				clientConfig.clientID = clientConfig.clientID + "_" + i;
 				publishers[i] = getPublisher(clientConfig,
						dataSymbols[i], Double.parseDouble(dataRates[i]),
						replication);
				// Let it publish forever until this program is killed
				pubThread[i] = new Thread(publishers[i]);
				pubThread[i].start();
				Thread.sleep(DELAY_BETWEEN_NEW_PUBLISHER);
			} catch (ClientException e) {
				System.out.println(e.getMessage());
				System.exit(1);
			} catch (InterruptedException e) {
			}
		}

		for (int i = 0; i < dataSymbols.length; i++) {
			try {
				pubThread[i].join();
			} catch (InterruptedException ie) {
				ie.printStackTrace();
			}
		}
	}

	private static DatasetPublisherClient getPublisher(ClientConfig clientConfig,
			String dataSymbol, double dataRate, 
			int replication) throws ClientException {
		if (datasetType.equals("TRAFFIC"))
			return new TrafficPublisher(clientConfig,
				dataSymbol , dataRate,replication, finishDir);
		else 
			return new StockPublisher(clientConfig,
					dataSymbol ,  dataRate,replication, finishDir);
 
	}

	private static DatasetPublisherClient[] createPublisherArray() {
		if (datasetType.equals("TRAFFIC"))
			return new TrafficPublisher[dataSymbols.length];
		else
			return new StockPublisher[dataSymbols.length];
	}

	private static void showUsage() {
		System.out
				.printf("Usage: java TrafficPublisher -%s <id> -%s <broker_uri>\n\t"
						+ "-%s <symbol> -%s <pub_rate/min> -%s <pub_start_time>\n",
						ClientConfig.CLI_OPTION_ID,
						ClientConfig.CLI_OPTION_BROKER_LIST, CMD_ARG_SYMBOL,
						CMD_ARG_RATE, CMD_ARG_PUB_DELAY);
	}

	protected static String getCurrentDateTime() {
		return new SimpleDateFormat("MMdd_HH.mm.ss").format(new Date());
	}
	
	public static void main(String[] args) {
		loader(args);
		start(args);

	}
}
