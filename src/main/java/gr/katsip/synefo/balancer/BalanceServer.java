package gr.katsip.synefo.balancer;

import gr.katsip.synefo.utils.Pair;
import gr.katsip.synefo.utils.JoinOperator;

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

public class BalanceServer {

	private ServerSocket serverSocket;

	private boolean killCommand;

	private ConcurrentHashMap<String, ArrayList<String>> physicalTopology;

	private ConcurrentHashMap<String, ArrayList<String>> activeTopology;

	private ConcurrentHashMap<String, Integer> taskIdentifierIndex;

	private ConcurrentHashMap<String, String> taskAddressIndex;
	
	private ConcurrentHashMap<Integer, JoinOperator> taskToJoinRelation;
	
	private ConcurrentHashMap<String, Integer> taskWorkerPortIndex;

	private HashMap<String, Pair<Number, Number>> resourceThresholds;

	private String zooHost;
	
	private AtomicInteger taskNumber;
	
	private ConcurrentLinkedQueue<String> pendingAddressUpdates;

	private boolean INIT_MINIMAL_RESOURCES;

	public BalanceServer(String zooHost, HashMap<String, Pair<Number, Number>> _resource_thresholds, boolean INIT_MINIMAL_RESOURCES) {
		physicalTopology = new ConcurrentHashMap<String, ArrayList<String>>();
		activeTopology = new ConcurrentHashMap<String, ArrayList<String>>();
		taskIdentifierIndex = new ConcurrentHashMap<String, Integer>();
		taskWorkerPortIndex = new ConcurrentHashMap<String, Integer>();
		taskAddressIndex = new ConcurrentHashMap<String, String>();
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
		taskNumber = new AtomicInteger(-1);
        this.INIT_MINIMAL_RESOURCES = INIT_MINIMAL_RESOURCES;
	}

	public void runServer() {
		Socket _stormComponent = null;
		OutputStream _out = null;
		InputStream _in = null;
		(new Thread(new NewLoadMaster(zooHost, resourceThresholds, physicalTopology,
				activeTopology, taskIdentifierIndex, taskAddressIndex,
				taskNumber, taskToJoinRelation, INIT_MINIMAL_RESOURCES))).start();
		while(killCommand == false) {
			try {
				_stormComponent = serverSocket.accept();
				_out = _stormComponent.getOutputStream();
				_in = _stormComponent.getInputStream();
				(new Thread(new LoadSlave(physicalTopology, activeTopology, taskIdentifierIndex, taskWorkerPortIndex,
                        _in, _out, taskAddressIndex, taskNumber, taskToJoinRelation))).start();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
