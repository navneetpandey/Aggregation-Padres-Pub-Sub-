package ca.utoronto.msrg.padres.broker.aggregation.utility;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Properties;

public class Stats {

	private static Stats statistics;
	OutputStream outputStream;

	public static final String PADRES_HOME = System.getenv("PADRES_HOME") == null ? "."
			+ File.separator
			: System.getenv("PADRES_HOME") + File.separator;
	protected static final String DEFAULT_STATS_FILE_PATH = PADRES_HOME + "etc"
			+ File.separator + "broker.properties";

	Properties properties;
	
	boolean headerPrinted=false;

	private LinkedHashMap<String, LinkedHashMap> data;

	private Stats(String file_location) {
		StatsConst("", file_location);
	}
	private Stats(String dir_name, String file_location) {
		StatsConst("", file_location);
	}
	private void StatsConst(String dir_name, String file_location) {
		setup();
		try {
			String directory="";
			if(dir_name == "")
				directory = properties
						.getProperty("padres.stats.basedir");
			else
				directory = dir_name;
			String full_file_path = directory
					+ File.separator
					+ file_location;
			File file = new File(full_file_path);
			//System.out.println("STATSXXXXXXXXXXXXXFILEPATH"+full_file_path);
			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.getParentFile().mkdirs();
				file.createNewFile();
			}
			else{
				file.delete();
				file.createNewFile();
			}
				
			outputStream = new FileOutputStream(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	private Stats() {
		setup();
		outputStream = System.out;
	}

	public Stats(OutputStream outStream) {
		setup();
		outputStream =outStream;
	}

	public static synchronized Stats getInstance(OutputStream outStream) {
		if (statistics == null) {
			statistics = new Stats(outStream);
		}
		return statistics;
	}
	
	public static synchronized Stats getInstance(String file_location) {
		if (statistics == null) {
			statistics = new Stats(file_location);
		}
		return statistics;
	}
	
	public static synchronized Stats getInstance(String dir_name, String file_location) {
		if (statistics == null) {
			statistics = new Stats( dir_name, file_location);
		}
		return statistics;
	}

	
	public static synchronized Stats getInstance() {
		if (statistics == null) {
			statistics = new Stats();
		}
		return statistics;
	}

	private void setup() {
		data = new LinkedHashMap<String, LinkedHashMap>();
		properties = new Properties();
		InputStream propFileStream = null;
		try {
			propFileStream = new FileInputStream(DEFAULT_STATS_FILE_PATH);
			properties.load(propFileStream);
			propFileStream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public boolean addStatsField(String key) {
	
			
		
		LinkedHashMap<String, Object> newSubFields = new LinkedHashMap<String, Object>();
		if (key.contains(".")) {
			String fields[] = key.split("\\.");
			
			if (data.containsKey(fields[0])) {
				newSubFields = data.get(fields[0]);
				if(newSubFields.containsKey(fields[1]))
					return false;
			} 
			
			newSubFields.put(fields[1], null);
			synchronized (data) {
			data.put(fields[0], newSubFields);
			}

		} else {
			newSubFields.put("default", null);
		
				
			synchronized (data) {
			data.put(key, newSubFields);
			}
		}

		return true;

	}

	public boolean addValue(String key, Object Value) {
		LinkedHashMap<String, Object> subFields;
		String firstField,secondField;
		if (key.contains(".")) {
			String fields[] = key.split("\\.");
			firstField = fields[0];
			secondField = fields[1];
		} else {
			firstField = key;
			secondField = "default";
		}
		
		if (data.containsKey(firstField)) {
				subFields = data.get(firstField);
				if(subFields.containsKey(secondField)) {
 
					subFields.put(secondField, updateValue(Value, subFields.get(secondField)));
					return true;
				}
			} 
			
				return true;

	}
	
	private Object updateValue(Object val1, Object val2){
		if(val2==null) { 
			return val1;
		}
		else if(val1 instanceof Float && val2 instanceof Float) 
			return (Float)val1 + (Float)val2;
		else if(val1 instanceof Integer && val2 instanceof Integer) 
			return (Integer)val1 + (Integer)val2;
		else if(val1 instanceof String && val2 instanceof String) 
			return  (String) val2 + (String)val1;
		else if(val1 instanceof Double && val2 instanceof Double) 
			return  (Double)val1 + (Double) val2;
		else if(val1 instanceof Long && val2 instanceof Long)
			return (Long)val1 + (Long) val2;
		else if(val2 instanceof String )
			return (String)val2 + (String) val1;
		else if(val2 instanceof Double || val1 instanceof Double)
			return (Double)val2 + Double.valueOf(""+val1);
		else if(val2 instanceof Float || val1 instanceof Float )
			return (Float)val2 + Float.valueOf(""+val1);
		else if(val2 instanceof Long || val1 instanceof Long)
			return (Long)val2 + Long.valueOf(""+val1);
		else if(val2 instanceof Integer || val1 instanceof Integer)
			return (Integer)val2 + Integer.valueOf(""+val1);
		else
			return ""+val2+val1;
	}

	public synchronized void printResult() throws IOException{
		if(!headerPrinted) {
			printHeader();
			headerPrinted=true;
		}
			
		String output = "";
		Iterator<String> itKey = data.keySet().iterator();
 		while(itKey.hasNext()) {
 			LinkedHashMap<String, Object> subArray = data.get(itKey.next());
 			Iterator<String> itSecondKey = subArray.keySet().iterator();
			while(itSecondKey.hasNext()){
				String secondKey = itSecondKey.next();
				Object temp = subArray.get(secondKey);
				subArray.put(secondKey, null);
				if(temp != null)
					output +=temp;
				else
					output +=0;
				if(itSecondKey.hasNext())
					output+="-";
			}
			if(itKey.hasNext())
				output +=",";
		}
 		output+="\n";
 		if(!output.contains("-"))
 			System.out.println("I am debbuging here");
 		outputStream.write(output.getBytes());
 		outputStream.flush();
			
	 
	}
	
	private void printHeader() throws IOException {
 
		int headNo =1;
		int subheadNo =1;
		String header = "";
		Iterator<String> itKey = data.keySet().iterator();
		String temp;
 		while(itKey.hasNext()) {
 			subheadNo=1;
 			temp=itKey.next();
 			header +=((headNo++)+temp+":"); 
 			Iterator<String> itSecondKey = data.get(temp).keySet().iterator();
			while(itSecondKey.hasNext()){
				header +=((subheadNo++)+itSecondKey.next());
				if(itSecondKey.hasNext())
					header+="-";
			}
			if(itKey.hasNext())
				header +=",";
		}
 		
 		header+="\n";
 		
 		outputStream.write(header.getBytes());
 		outputStream.flush();
	}

	public OutputStream getStream(){
		return outputStream;
	}
}
