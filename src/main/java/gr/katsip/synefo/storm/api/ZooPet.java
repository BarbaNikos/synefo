package gr.katsip.synefo.storm.api;

import java.io.IOException;
import java.util.StringTokenizer;



//import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooPet {
	
//	private final static Logger LOG = Logger.getLogger(ZooPet.class.getName());
	
	Logger logger = LoggerFactory.getLogger(ZooPet.class);

	enum BoltState { INIT, SYNEFO_READY, REGISTERED, ACTIVE };

	private volatile BoltState state;

	private ZooKeeper zk;

	private String zoo_ip;

	private Integer zoo_port;

	private Integer task_id;

	private String task_name;

	public Pair<Double, Double> cpu;

	public Pair<Double, Double> mem;

	public Pair<Integer, Integer> latency;

	public Pair<Integer, Integer> throughput;

	public String scaleOutZnodeName;
	
	public String scaleInZnodeName;

	public volatile String pendingCommand;
	
	private boolean submittedScaleTask = false;

	private String task_ip;

	public ZooPet(String zoo_ip, Integer zoo_port, String task_name, Integer task_id, String task_ip) {
		this.zoo_ip = zoo_ip;
		this.zoo_port = zoo_port;
		this.task_id = task_id;
		this.task_name = task_name;
		state = BoltState.INIT;
		scaleOutZnodeName = "";
		scaleInZnodeName = "";
		pendingCommand = null;
		this.task_ip = task_ip;
		cpu = new Pair<Double, Double>();
		mem = new Pair<Double, Double>();
		latency = new Pair<Integer, Integer>();
		throughput = new Pair<Integer, Integer>();
	}

	Watcher boltWatcher = new Watcher() {
		public void process(WatchedEvent e) {
			if(e.getType() == Event.EventType.NodeDataChanged) {
				if(e.getPath().equals("/synefo/bolt-tasks/" + task_name + ":" + task_id + "@" + task_ip)) {
					/**
					 * Get scale command, and clean up the directory
					 */
//					LOG.info("boltWatcher.process(): Children of node " + e.getPath() + " have changed. Time to check the scale-command.");
					logger.info("boltWatcher.process(): Children of node " + e.getPath() + " have changed. Time to check the scale-command.");
					getScaleCommand();
					setBoltNodeWatch();
				}
			}
		}
	};

	public synchronized void getScaleCommand() {
		Stat stat = new Stat();
		try {
			pendingCommand = new String(zk.getData("/synefo/bolt-tasks/" + task_name + ":" + task_id + "@" + task_ip, 
					boltWatcher, 
					stat));
//			LOG.info("getScaleCommand(): Received scale command \"" + pendingCommand + "\" (" + task_name + ":" + task_id + "@" + task_ip + ")");
			logger.info("getScaleCommand(): Received scale command \"" + pendingCommand + "\" (" + task_name + ":" + task_id + "@" + task_ip + ")");
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public synchronized String returnScaleCommand() {
		String returnCommand = pendingCommand;
		pendingCommand = null;
		return returnCommand;
	}

	public void start() {
		try {
			zk = new ZooKeeper(zoo_ip + ":" + zoo_port, 100000, boltWatcher);
			while(zk.getState() != ZooKeeper.States.CONNECTED) {
				Thread.sleep(100);
			}
			if(zk.exists("/synefo/bolt-tasks", false) != null) {
				zk.create("/synefo/bolt-tasks/" + task_name + ":" + task_id + "@" + task_ip, 
						("/synefo/bolt-tasks/" + task_name + ":" + task_id + "@" + task_ip).getBytes(), 
						Ids.OPEN_ACL_UNSAFE, 
						CreateMode.PERSISTENT);
				Stat stat = new Stat();
				String thresholds = new String(zk.getData("/synefo/scale-out-event", false, stat));
				StringTokenizer strTok = new StringTokenizer(thresholds, ",");
				cpu.upperBound = Double.parseDouble(strTok.nextToken());
				mem.upperBound = Double.parseDouble(strTok.nextToken());
				latency.upperBound = Integer.parseInt(strTok.nextToken());
				throughput.upperBound = Integer.parseInt(strTok.nextToken());
				
				thresholds = new String(zk.getData("/synefo/scale-in-event", false, stat));
				strTok = new StringTokenizer(thresholds, ",");
				cpu.lowerBound = Double.parseDouble(strTok.nextToken());
				mem.lowerBound = Double.parseDouble(strTok.nextToken());
				latency.lowerBound = Integer.parseInt(strTok.nextToken());
				throughput.lowerBound = Integer.parseInt(strTok.nextToken());
				state = BoltState.ACTIVE;
//				LOG.info("start(): Initialization successful (" + task_name + ":" + task_id + "@" + task_ip + ")");
				logger.info("start(): Initialization successful (" + task_name + ":" + task_id + "@" + task_ip + ")");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch(InterruptedException e1) {
			e1.printStackTrace();
		}catch(KeeperException e2) {
			e2.printStackTrace();
		}
	}

	public void stop() {
		try {
			zk.close();
//			LOG.info("stop(): Closing connection (" + task_name + ":" + task_id + "@" + task_ip + ")");
			logger.info("stop(): Closing connection (" + task_name + ":" + task_id + "@" + task_ip + ")");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void setBoltNodeWatch() {
		zk.getData("/synefo/bolt-tasks/" + task_name + ":" + task_id + "@" + task_ip, 
				boltWatcher, 
				boltNodeDataCallback, 
				null);
	}

	private DataCallback boltNodeDataCallback = new DataCallback() {
		public void processResult(int rc, String path, Object ctx, byte[] data,
				Stat stat) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
//				LOG.info("boltNodeDataCallback(): CONNECTIONLOSS");
				logger.info("boltNodeDataCallback(): CONNECTIONLOSS");
				setBoltNodeWatch();
				break;
			case OK:
//				LOG.info("boltNodeDataCallback(): OK");
				logger.info("boltNodeDataCallback(): OK");
				break;
			default:
//				LOG.error("boltNodeDataCallback(): Unexpected scenario: " + 
//						KeeperException.create(Code.get(rc), path));
				logger.info("boltNodeDataCallback(): Unexpected scenario: " + 
						KeeperException.create(Code.get(rc), path));
				break;

			}
		}
	};

	public void setStatisticData(double cpu, double memory, Integer latency, Integer throughput) {
		if(state == BoltState.ACTIVE && scaleOutZnodeName.equals("") && scaleInZnodeName.equals("") && submittedScaleTask == false) {
			if(this.cpu.upperBound < cpu || this.mem.upperBound < memory) {
				/**
				 * Create also a node under scale-out-event znode with the name of the bolt.
				 * This way, the SynEFO coordination thread will understand that the bolt 
				 * is overloaded and that it needs to scale-out.
				 */
//				LOG.info("setStatisticData(): Over-utilization detected. Generating scale out command (" + task_name + ":" + task_id + "@" + task_ip + ")...");
				logger.info("setStatisticData(): Over-utilization detected. Generating scale out command (" + task_name + ":" + task_id + "@" + task_ip + ")...");
				createScaleOutTask();
				submittedScaleTask = true;
			}else if(this.cpu.lowerBound > cpu || this.mem.lowerBound > memory) {
				/**
				 * Create also a node under scale-in-event znode with the name of the bolt.
				 * This way, the SynEFO coordination thread will understand that the bolt 
				 * is overloaded and that it needs to scale-out.
				 */
//				LOG.info("setStatisticData(): Under-utilization detected. Generating scale out command (" + task_name + ":" + task_id + "@" + task_ip + ")...");
				logger.info("setStatisticData(): Under-utilization detected. Generating scale out command (" + task_name + ":" + task_id + "@" + task_ip + ")...");
				createScaleInTask();
				submittedScaleTask = true;
			}
		}
	}
	
	public void resetSubmittedScaleFlag() {
		submittedScaleTask = false;
	}

	private void createScaleOutTask() {
		zk.create("/synefo/scale-out-event/" + task_name + ":" + task_id + "@" + task_ip + "-", 
				(task_name + ":" + task_id + "@" + task_ip).getBytes(), 
				Ids.OPEN_ACL_UNSAFE, 
				CreateMode.PERSISTENT_SEQUENTIAL, 
				createScaleOutEventCallback, 
				null);
	}

	StringCallback createScaleOutEventCallback = new StringCallback() {
		public void processResult(int rc, String path, Object ctx,
				String name) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
//				LOG.info("createScaleOutEventCallback(): CONNECTIONLOSS for scale-event-creation");
				logger.info("createScaleOutEventCallback(): CONNECTIONLOSS for scale-event-creation");
				createScaleOutTask();
				break;
			case NONODE:
//				LOG.info("createScaleOutEventCallback(): NONODE for scale-event-creation");
				logger.info("createScaleOutEventCallback(): NONODE for scale-event-creation");
				break;
			case NODEEXISTS:
//				LOG.info("createScaleOutEventCallback(): NODEEXISTS for scale-event-creation");
				System.out.println("createScaleOutEventCallback(): NODEEXISTS for scale-event-creation");
				break;
			case OK:
				scaleOutZnodeName = name;
//				LOG.info("createScaleOutEventCallback(): OK for scale-event-creation");
				logger.info("createScaleOutEventCallback(): OK for scale-event-creation");
				break;
			default:
//				LOG.error("createScaleOutEventCallback(): Unexpected scenario: " + 
//						KeeperException.create(Code.get(rc), path));
				logger.info("createScaleOutEventCallback(): Unexpected scenario: " + 
						KeeperException.create(Code.get(rc), path));
				break;

			}
		}
	};
	
	private void createScaleInTask() {
		zk.create("/synefo/scale-in-event/" + task_name + ":" + task_id + "@" + task_ip + "-", 
				(task_name + ":" + task_id + "@" + task_ip).getBytes(), 
				Ids.OPEN_ACL_UNSAFE, 
				CreateMode.PERSISTENT_SEQUENTIAL, 
				createScaleInEventCallback, 
				null);
	}
	
	StringCallback createScaleInEventCallback = new StringCallback() {
		public void processResult(int rc, String path, Object ctx,
				String name) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
//				LOG.info("createScaleInEventCallback(): CONNECTIONLOSS for scale-event-creation");
				logger.info("createScaleInEventCallback(): CONNECTIONLOSS for scale-event-creation");
				createScaleOutTask();
				break;
			case NONODE:
//				LOG.info("createScaleInEventCallback(): NONODE for scale-event-creation");
				logger.info("createScaleInEventCallback(): NONODE for scale-event-creation");
				break;
			case NODEEXISTS:
//				LOG.info("createScaleInEventCallback(): NODEEXISTS for scale-event-creation");
				logger.info("createScaleInEventCallback(): NODEEXISTS for scale-event-creation");
				break;
			case OK:
				scaleInZnodeName = name;
//				LOG.info("createScaleInEventCallback(): OK for scale-event-creation");
				logger.info("createScaleInEventCallback(): OK for scale-event-creation");
				break;
			default:
//				LOG.error("createScaleInEventCallback(): Unexpected scenario: " + 
//						KeeperException.create(Code.get(rc), path));
				logger.info("createScaleInEventCallback(): Unexpected scenario: " + 
						KeeperException.create(Code.get(rc), path));
				break;

			}
		}
	};

	public void scaleTaskGC() {
		if(scaleOutZnodeName.equals("") == false) {
			zk.delete("/synefo/scale-out-event/" + scaleOutZnodeName, 
					-1, scaleOutTaskGCcallback, null);
		}
		if(scaleInZnodeName.equals("") == false) {
			zk.delete("/synefo/scale-in-event/" + scaleInZnodeName, 
					-1, scaleInTaskGCcallback, null);
		}
	}

	private VoidCallback scaleOutTaskGCcallback = new VoidCallback() {
		public void processResult(int rc, String path, Object ctx) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				scaleTaskGC();
				break;
			case NONODE:
				scaleOutZnodeName = "";
				break;
			case OK:
				scaleOutZnodeName = "";
				break;
			default:
				logger.info("scaleOutTaskGCcallback(): Unexpected scenario: " + 
						KeeperException.create(Code.get(rc), path));
				break;

			}
		}
	};
	
	private VoidCallback scaleInTaskGCcallback = new VoidCallback() {
		public void processResult(int rc, String path, Object ctx) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				scaleTaskGC();
				break;
			case NONODE:
				scaleInZnodeName = "";
				break;
			case OK:
				scaleInZnodeName = "";
				break;
			default:
				logger.info("scaleInTaskGCcallback(): Unexpected scenario: " + 
						KeeperException.create(Code.get(rc), path));
				break;

			}
		}
	};

}
