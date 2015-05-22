package ca.utoronto.msrg.padres.demo.dataset;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.log4j.Logger;

import ca.utoronto.msrg.padres.client.Client;
import ca.utoronto.msrg.padres.client.ClientConfig;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.util.CommandLine;
import ca.utoronto.msrg.padres.common.util.LogException;
import ca.utoronto.msrg.padres.common.util.LogSetup;
import ca.utoronto.msrg.padres.demo.stockquote.StockSubscriber;
import ca.utoronto.msrg.padres.demo.traffic.TrafficSubscriber;

public class DatasetSubscriberExecuter {

	public static final String PADRES_HOME = System.getenv("PADRES_HOME") == null ? "."
			+ File.separator
			: System.getenv("PADRES_HOME") + File.separator;

	protected static final String DEFAULT_PROPS_FILE_PATH = PADRES_HOME + "etc"
			+ File.separator + "broker.properties";

	protected static final String DEFAULT_CLIENT_ID = getCurrentDateTime();

	private final static String DEFAULT_SUBSCRIBER_ID = "Sub-"
			+ DEFAULT_CLIENT_ID;

	private static final String CMD_ARG_SUBSCRIPTION = "s";

	private static Properties properties; 

	private static String operator = "max";

	static ClientConfig clientConfig;

	static int replication = 1;

	static Random rand;
	static int symbolIndex = 0;
	static String[] rates = null;
	static ArrayList<String> symbols = new ArrayList<String>();

	static String datasetType = "TRAFFIC";
	private static int normalSubscriptionPercent = 0;

	public static void loader(String[] args) {

		properties = new Properties();
		try {
			properties.load(new FileInputStream(DEFAULT_PROPS_FILE_PATH));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		 // interceptor to extract no of replication
		final List<String> list = new ArrayList<String>();

		Collections.addAll(list, args);
		Iterator<String> itr = list.iterator();

		while (itr.hasNext()) {
			String curr = (String) itr.next();
			if (curr.equals("-NS")) {
				itr.remove();
				replication = Integer.parseInt((String) itr.next());
				itr.remove();
			}

			if (curr.equals("-symbolIndex")) {
				itr.remove();
				symbolIndex = Integer.parseInt((String) itr.next());
				itr.remove();
			}
			if (curr.equals("-OP")) {
				itr.remove();
				operator = (String) itr.next();
				itr.remove();
			}
			if (curr.equals("-NORMAL_PERCENT")) {
				itr.remove();
				normalSubscriptionPercent = Integer.parseInt((String) itr
						.next());
				itr.remove();
			}
			if (curr.equals("-r")) {
				itr.remove();
				String rateString = (String) itr.next();
				rates = rateString.split("/");
				itr.remove();
			}
			if (curr.equals("-s")) {
				itr.remove();
				String symbolString = (String) itr.next();
				String[] symbolsArray = symbolString.split("/");
				 Collections.addAll( symbols,symbolsArray);
				itr.remove();
			}

			if (curr.equals("-DATASET")) {
				itr.remove();
				datasetType = (String) itr.next();
				itr.remove();
			}
		}
		args = list.toArray(new String[list.size()]);
		initRandom(symbolIndex);

		String[] cliKeys = ClientConfig.getCommandLineKeys();
		String[] fullCLIKeys = new String[cliKeys.length];
		System.arraycopy(cliKeys, 0, fullCLIKeys, 0, cliKeys.length);
		// fullCLIKeys[cliKeys.length] = CMD_ARG_SUBSCRIPTION + ":";
		CommandLine cmdLine = new CommandLine(fullCLIKeys);
		try {
			cmdLine.processCommandLine(args);
		} catch (Exception e) {
			e.printStackTrace();
			showUsage();
			System.exit(1);
		}
		// String subscription = cmdLine.getOptionValue(CMD_ARG_SUBSCRIPTION);

		try {
			new LogSetup(null);
		} catch (LogException e1) {
			e1.printStackTrace();
			System.exit(1);
		}

		Logger logger = Logger.getLogger(Client.class);
		try {
			clientConfig = new ClientConfig();
			clientConfig.clientID = DEFAULT_SUBSCRIBER_ID;
			clientConfig.overwriteWithCmdLineArgs(cmdLine);
			/*
			 * if (subscription == null) { logger.fatal(
			 * "Don't know what to subscribe because subscription is not specified."
			 * ); showUsage(); System.exit(1); } else
			 */
			if (clientConfig.connectBrokerList == null
					|| clientConfig.connectBrokerList.length == 0) {
				logger.fatal("Don't know which broker to connect to.");
				showUsage();
				System.exit(1);
			}
		} catch (ClientException e) {
			e.printStackTrace();
			System.exit(1);
		}

	}


	public static void initRandom(int index) {
		Random seed = new Random(77777);
		Long longRandom = 0L;
		for (int i = 0; i < index + 1; i++) {
			longRandom = seed.nextLong();
		}

		rand = new Random(longRandom);
	}

	private static void showUsage() {
		System.out.printf(
				"Usage: java  Publisher -%s <id> -%s <broker_uri>\n\t"
						+ "-%s <subscription>\n", ClientConfig.CLI_OPTION_ID,
				ClientConfig.CLI_OPTION_BROKER_LIST, CMD_ARG_SUBSCRIPTION);
	}

	protected static String getCurrentDateTime() {
		return new SimpleDateFormat("MMdd_HH.mm.ss").format(new Date());
	}
	

	public static void main(String[] args) {
		loader(args);
		DatasetSubscriberClient subscriber;
		try {
			if (datasetType.equals("TRAFFIC")) {

				subscriber = new TrafficSubscriber(clientConfig, replication,
						symbolIndex, rates, symbols, operator,
						normalSubscriptionPercent, rand);

			} else {
				symbols = StockSubscriber.getSymbols();
				subscriber = new StockSubscriber(clientConfig, replication,
						symbolIndex, rates, symbols, operator,
						normalSubscriptionPercent, rand);

			}
			subscriber.run();
		} catch (ClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
