package gr.katsip.synefo.server2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import gr.katsip.cestorm.db.CEStormDatabaseManager;
import gr.katsip.synefo.storm.api.Pair;

public class Synefo {

	private ServerSocket serverSocket;

	private boolean killCommand;

	private ConcurrentHashMap<String, ArrayList<String>> physicalTopology;

	private ConcurrentHashMap<String, ArrayList<String>> activeTopology;

	private ConcurrentHashMap<String, Integer> taskIdentifierIndex;

	private ConcurrentHashMap<String, String> taskIPs;
	
	private ConcurrentHashMap<Integer, JoinOperator> taskToJoinRelation;
	
	private ConcurrentHashMap<String, Integer> taskWorkerPortIndex;

	private HashMap<String, Pair<Number, Number>> resourceThresholds;

	private String zooHost;
	
	private AtomicBoolean operationFlag;
	
	private boolean demoMode = false;
	
	private AtomicInteger queryId;
	
	private AtomicInteger taskNumber;
	
	private CEStormDatabaseManager ceDb = null;
	
	private ConcurrentLinkedQueue<String> pendingAddressUpdates;

	public Synefo(String zooHost, HashMap<String, Pair<Number, Number>> _resource_thresholds, CEStormDatabaseManager ceDb) {
		physicalTopology = new ConcurrentHashMap<String, ArrayList<String>>();
		activeTopology = new ConcurrentHashMap<String, ArrayList<String>>();
		taskIdentifierIndex = new ConcurrentHashMap<String, Integer>();
		taskWorkerPortIndex = new ConcurrentHashMap<String, Integer>();
		taskIPs = new ConcurrentHashMap<String, String>();
		taskToJoinRelation = new ConcurrentHashMap<Integer, JoinOperator>();
		pendingAddressUpdates = new ConcurrentLinkedQueue<String>();
		serverSocket = null;
		try {
			serverSocket = new ServerSocket(5555);
		} catch (IOException e) {
			e.printStackTrace();
		}
		killCommand = false;
		resourceThresholds = _resource_thresholds;
		this.zooHost = zooHost;
		operationFlag = new AtomicBoolean(false);
		taskNumber = new AtomicInteger(-1);
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
		(new Thread(new SynefoMaster(zooHost, resourceThresholds, physicalTopology,
				activeTopology, taskIdentifierIndex, taskWorkerPortIndex, taskIPs, operationFlag, demoMode, queryId, ceDb, taskNumber, taskToJoinRelation, pendingAddressUpdates))).start();
		while(killCommand == false) {
			try {
				_stormComponent = serverSocket.accept();
				_out = _stormComponent.getOutputStream();
				_in = _stormComponent.getInputStream();
				(new Thread(new SynefoSlave(physicalTopology, activeTopology, taskIdentifierIndex, taskWorkerPortIndex, _in, _out, taskIPs,
						operationFlag, demoMode, queryId, taskNumber, taskToJoinRelation, pendingAddressUpdates))).start();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
