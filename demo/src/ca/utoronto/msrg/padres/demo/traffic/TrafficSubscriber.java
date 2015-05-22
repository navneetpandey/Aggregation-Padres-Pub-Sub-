package ca.utoronto.msrg.padres.demo.traffic;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;

import javax.swing.Timer;

import org.apache.log4j.Logger;

import ca.utoronto.msrg.padres.client.Client;
import ca.utoronto.msrg.padres.client.ClientConfig;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.message.Message;
import ca.utoronto.msrg.padres.common.message.Publication;
import ca.utoronto.msrg.padres.common.message.PublicationMessage;
import ca.utoronto.msrg.padres.common.message.Subscription;
import ca.utoronto.msrg.padres.common.message.SubscriptionMessage;
import ca.utoronto.msrg.padres.common.message.parser.ParseException;
import ca.utoronto.msrg.padres.common.util.CommandLine;
import ca.utoronto.msrg.padres.common.util.LogException;
import ca.utoronto.msrg.padres.common.util.LogSetup;
import ca.utoronto.msrg.padres.demo.dataset.DatasetSubscriberClient;

public class TrafficSubscriber extends DatasetSubscriberClient {
 
	/**
	 * Constructor
	 * 
	 * @param id
	 * @param strSubscription
	 * @throws ClientException
	 * @throws RemoteException
	 */
	public TrafficSubscriber(ClientConfig clientConfig, int replication,
			int index, String[] rates, ArrayList<String> symbols,String operator, int normalSubscriptionPercent, Random rand) throws ClientException  {
		super(clientConfig, replication, index, rates, symbols, operator, normalSubscriptionPercent, rand);
	}

 
	@Override
	public boolean sendSubscriptions(String[] rates, ArrayList<String> symbols) {
		//return sendSubscriptionsFromList(rates);
		try {
			return 	sendSubscriptionsFromListAggregatePercent(rates, getDataPath()
					+ "trafficSubscriptions.txt", "speed");
		} catch (ClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
 	
	public  String getAverageFloatValueFromPublication(String data_file, int records, TrafficDataFileds TDF) {
		return getAverageValueFromPublication(getDataPath() + data_file, records,TDF.getValue());
	}
	
	@Override
	protected String getDataPath() {

		if(clientConfig.clientProps.getProperty("Traffic.DataPath")!=null)
			return clientConfig.clientProps.getProperty("Traffic.DataPath");
		else
			return ClientConfig.PADRES_HOME + "demo/data/trafficPubs/";
	}

	@Override
	protected String getDataFilePath(String symbol) {
		return getDataPath() + symbol + ".data";
	}
	
	public String getNormalSubscription(int index, ArrayList<String> symbols, String[] rates){
		return  "[class,eq,'traffic'],[ucID,=," + (index+1) + "],"
				+ "[speed,>,"
				+ getAverageIntegerValueFromPublication(""+symbols.get(index)+".data",
						Integer.parseInt(rates[index]),TrafficDataFileds.SPEED)
				+ "]";
	}
	public String getAggregateSubscription(int index, ArrayList<String> symbols, String[] rates, String operator){
		return "[class,eq,'traffic']," +
				" [ucID,=," + (index+1) + "]"+
				",[speed,>," 
				 + getAverageIntegerValueFromPublication(""+symbols.get(index)+".data",
						Integer.parseInt(rates[index]),TrafficDataFileds.SPEED)
				+ "],[AGR,eq,'" + operator
				+ "'],[PAR,eq,speed],[PRD,eq,'"+window+"'],[NTF,eq,'"+notification+"']";
	}


 


	@Override
	public boolean sendSubscriptionsFromList(String[] rates) {
		 
		try {
			return super.sendSubscriptionsFromList(rates, getDataPath()
					+ "trafficSubscriptions.txt", "speed");
		} catch (ClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}



	@Override
	public boolean sendSubscriptionsFromSymbols(String[] rates) {
		return super.sendSubscriptionsFromSymbols(rates, symbols);
	}





}
