package gr.katsip.synefo.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import gr.katsip.cestorm.db.CEStormDatabaseManager;
import gr.katsip.synefo.storm.api.Pair;

public class Synefo {

	private ServerSocket serverSocket;

	private Integer serverPort;

	private boolean killCommand;

	private HashMap<String, ArrayList<String>> physicalTopology;

	private HashMap<String, ArrayList<String>> activeTopology;

	private HashMap<String, Integer> nameToIdMap;

	private HashMap<String, String> taskIPs;

	private HashMap<String, Pair<Number, Number>> resourceThresholds;

	private String zooHost;

	private Integer zooIP;
	
	private AtomicBoolean operationFlag;
	
	private boolean demoMode = false;
	
	private AtomicInteger queryId;
	
	private CEStormDatabaseManager ceDb = null;

	public Synefo(String zooHost, Integer zooIP, HashMap<String, Pair<Number, Number>> _resource_thresholds, CEStormDatabaseManager ceDb) {
		physicalTopology = new HashMap<String, ArrayList<String>>();
		activeTopology = new HashMap<String, ArrayList<String>>();
		nameToIdMap = new HashMap<String, Integer>();
		taskIPs = new HashMap<String, String>();
		serverSocket = null;
		serverPort = -1;
		try {
			serverSocket = new ServerSocket(5555);
			serverPort = serverSocket.getLocalPort();
			System.out.println("+efo-INFO#" + serverSocket.getInetAddress().getHostAddress() + ":" + serverPort);
		} catch (IOException e) {
			e.printStackTrace();
		}
		killCommand = false;
		resourceThresholds = _resource_thresholds;
		this.zooHost = zooHost;
		this.zooIP = zooIP;
		operationFlag = new AtomicBoolean(false);
		if(ceDb != null) {
			this.demoMode = true;
			queryId = new AtomicInteger(-1);
			this.ceDb = ceDb;
		}else {
			this.demoMode = false;
			queryId = new AtomicInteger(-1);
			this.ceDb = null;
		}
	}

	public void runServer() {
		Socket _stormComponent = null;
		OutputStream _out = null;
		InputStream _in = null;
		(new Thread(new SynefoCoordinatorThread(zooHost, zooIP, resourceThresholds, physicalTopology, 
				activeTopology, nameToIdMap, taskIPs, operationFlag, demoMode, queryId, ceDb))).start();
		while(killCommand == false) {
			try {
				_stormComponent = serverSocket.accept();
				_out = _stormComponent.getOutputStream();
				_in = _stormComponent.getInputStream();
				(new Thread(new SynEFOthread(physicalTopology, activeTopology, nameToIdMap, _in, _out, taskIPs, 
						operationFlag, demoMode, queryId))).start();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
