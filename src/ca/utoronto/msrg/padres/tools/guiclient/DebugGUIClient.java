package ca.utoronto.msrg.padres.tools.guiclient;

import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

import javax.swing.JFrame;

import ca.utoronto.msrg.padres.client.ClientConfig;
import ca.utoronto.msrg.padres.client.ClientException;
import ca.utoronto.msrg.padres.common.util.CommandLine;

public class DebugGUIClient implements Runnable {

	protected static final String CONFIG_FILE_PATH = String.format(
			"%s/etc/guiclient/client.properties", ClientConfig.PADRES_HOME);
	 String []args;
	
	public DebugGUIClient( String []  args){
		this.args = args;
		
		
	}
	
	
	@Override
	public void run() {
		try {
			// process the command line arguments
			CommandLine cmdLine = new CommandLine(ClientConfig.getCommandLineKeys());
			cmdLine.processCommandLine(args);
			String configFile = cmdLine.getOptionValue(ClientConfig.CLI_OPTION_CONFIG_FILE,
					CONFIG_FILE_PATH);
			// load the client configuration
			ClientConfig userConfig = new ClientConfig(configFile);
			// overwrite the client configurations from the config file with configuration
			// parameters from the command line
			userConfig.overwriteWithCmdLineArgs(cmdLine);

			// create the client
			final GUIClient guiClient = new GUIClient(userConfig);
			// create the GUI for the client
			JFrame clientWindow = guiClient.createJFrame();
			// add a WindowLister to the frame to gracefully shutdown the client
			clientWindow.addWindowListener(new WindowAdapter() {

				public void windowClosing(WindowEvent e) {
					guiClient.exitClient();
					System.exit(0);
				}
			});
			// display the GUI
			clientWindow.pack();
			clientWindow.setVisible(true);
			guiClient.printActiveConnections();
		} catch (ClientException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}

