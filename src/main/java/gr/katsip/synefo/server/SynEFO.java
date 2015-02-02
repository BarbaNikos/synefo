package gr.katsip.synefo.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import gr.katsip.synefo.storm.api.Pair;
import gr.katsip.synefo.storm.lib.Topology;

public class SynEFO {

	private ServerSocket _serverSocket;
	
	private Integer _serverPort;
	
	private boolean _killCommand;
	
	private Topology physicalTopology;
	
	private Topology runningTopology;
	
	private HashMap<String, Integer> nameToIdMap;
	
	private HashMap<String, String> _task_ips;
	
	private HashMap<String, Pair<Number, Number>> resource_thresholds;
	
	private String zooHost;
	
	private Integer zooIP;
	
	public SynEFO(String zooHost, Integer zooIP, HashMap<String, Pair<Number, Number>> _resource_thresholds) {
		physicalTopology = new Topology();
		runningTopology = new Topology();
		nameToIdMap = new HashMap<String, Integer>();
		_task_ips = new HashMap<String, String>();
		_serverSocket = null;
		_serverPort = -1;
		try {
			_serverSocket = new ServerSocket(0);
			_serverPort = _serverSocket.getLocalPort();
			System.out.println("synEFO started on IP: " + _serverSocket.getInetAddress().getHostAddress() + ":" + _serverPort);
		} catch (IOException e) {
			e.printStackTrace();
		}
		_killCommand = false;
		resource_thresholds = _resource_thresholds;
		this.zooHost = zooHost;
		this.zooIP = zooIP;
	}
	
	public void runServer() {
		Socket _stormComponent = null;
		OutputStream _out = null;
		InputStream _in = null;
		(new Thread(new SynEFOCoordinatorThread(zooHost, zooIP, resource_thresholds, physicalTopology, runningTopology, nameToIdMap, _task_ips))).start();
		while(_killCommand == false) {
			try {
				_stormComponent = _serverSocket.accept();
				_out = _stormComponent.getOutputStream();
				_in = _stormComponent.getInputStream();
				(new Thread(new SynEFOthread(physicalTopology, runningTopology, nameToIdMap, _in, _out, _task_ips))).start();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
