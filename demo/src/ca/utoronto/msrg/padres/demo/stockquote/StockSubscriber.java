package ca.utoronto.msrg.padres.demo.stockquote;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import ca.utoronto.msrg.padres.client.ClientConfig;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.demo.dataset.DatasetSubscriberClient;

// import loadbalancing.mediator.Mediator;

/**
 * A subscriber that subscribes to stock quotes and logs delivery delay to file.
 * Logging is synced at the beginning of each second (almost in fact).
 * 
 * @author cheung
 * 
 */
public class StockSubscriber extends DatasetSubscriberClient {

	// Constants

	// private static final String STOCK_VAL_RANGE_SCRIPT =
	// ClientConfig.PADRES_HOME
	// + "demo/bin/stockquote/findStockRange.sh";

	private final static String STOCK_DATA_PATH = ClientConfig.PADRES_HOME
			+ "demo/data/stockquotes/";

	public StockSubscriber(ClientConfig clientConfig, int replication,
			int index, String[] stockRates, ArrayList<String> symbols,
			String operator, int normalSubscriptionPercent, Random rand) throws ClientException {

		super(clientConfig, replication, index, stockRates, symbols, operator,
				normalSubscriptionPercent , rand);
	}

	public boolean sendSubscriptions(String[] stockRates,
			ArrayList<String> symbols) {

		//return sendSubscriptionsFromSymbols(stockRates, symbols);
		//return sendSubscriptionsFromList(stockRates);
		try {
			return sendSubscriptionsFromListAggregatePercent(stockRates, getDataPath()
					+ "NORMAL_SUBSCRIPTIONS",   "Volume");
		} catch (ClientException e) { 
			e.printStackTrace();
		} catch (IOException e) { 
			e.printStackTrace();
		}
		return false;
	}

	public boolean sendSubscriptionsFromList(String[] stockRates) {
		try {
			return sendSubscriptionsFromClassifiedList(stockRates, getDataPath()
					+ "NORMAL_SUBSCRIPTIONS", getDataPath()+"AGGREGATE_SUBSCRIPTIONS", "Volume");
		} catch (ClientException e) { 
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;

	}

	public String getAverageValueOfTheStock(String symbol, int records) {
		return getAverageValueFromPublication(STOCK_DATA_PATH + symbol,
				records, 5);
	}

	@Override
	protected String getDataPath() {

		if(clientConfig.clientProps.getProperty("Stock.DataPath")!=null)
			return clientConfig.clientProps.getProperty("Stock.DataPath");
		else 
		return ClientConfig.PADRES_HOME + "/demo/data/stockquotes/";
	}

	@Override
	protected String getDataFilePath(String symbols) {
		return getDataPath() + symbols;
	}

	public String getNormalSubscription(int index, ArrayList<String> symbols,
			String[] rates) {
		return "[class,eq,'STOCK'],[symbol,eq,'"
				+ symbols.get(index)
				+ "'],[open,>,"
				+ getAverageValueOfTheStock((String) symbols.get(index),
						Integer.parseInt(rates[index])) + "]";
	}

	public String getAggregateSubscription(int index, ArrayList<String> symbols,
			String[] rates, String operator) {
		return "[class,eq,'STOCK'],[symbol,eq,'"
				+ symbols.get(index)
				+ "'],[open,>,"
				+ getAverageValueOfTheStock((String) symbols.get(index),
						Integer.parseInt(rates[index])) + "],[AGR,eq,'"
				+ operator + "'],[PAR,eq,volume]"+
				",[PRD,eq,'"+window+"'],[NTF,eq,'"+notification+"']";
	}

	public static ArrayList<String> getSymbols() {
		String[] symbols = { "AAII", "AMAT", "AULT", "CKSW", "CSPI", "EBAY",
				"GLOV", "IEIB", "IPIX", "MANC", "NSSC", "ORCL", "RGEN", "STEM",
				"TAYD", "UTBI", "XEDA", "AAPL", "AMZN", "BITS", "CLWT", "CVNS",
				"ELSE", "GMTC", "IIJI", "IRSN", "MIKR", "NTAP", "PARS", "RHAT",
				"SUNH", "TIBX", "VRTS", "XXIA", "ABIX", "ANLT", "BPUR", "CMGI",
				"DELL", "EVST", "GNLB", "IKNX", "ISNS", "MRGO", "NWFL", "PROX",
				"SCON", "SUNW", "TRNI" };

		return new ArrayList<String>(Arrays.asList(symbols));
	}

	public boolean sendSubscriptionsFromSymbols(String[] rates) {
		return super.sendSubscriptionsFromSymbols(rates, symbols);
	}

}
