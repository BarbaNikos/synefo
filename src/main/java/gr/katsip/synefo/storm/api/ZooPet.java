package gr.katsip.synefo.storm.api;

import java.io.IOException;
import java.util.StringTokenizer;
//import org.apache.zookeeper.AsyncCallback.DataCallback;
//import org.apache.zookeeper.AsyncCallback.VoidCallback;
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

/**
 * ZooPet is the main class for handling interactions between a Storm component and the ZooKeeper service. Every time scale-out/in circumstances 
 * are met, ZooPet handles the creation of the appropriate z-nodes, so that Synefo takes the appropriate actions. Also, ZooPet is responsible for 
 * creating the required z-nodes during the initialization phase.
 * 
 * @author Nick R. Katsipoulakis
 *
 */
public class ZooPet {

	Logger logger = LoggerFactory.getLogger(ZooPet.class);

	enum BoltState { 
		INIT, 
		SYNEFO_READY, 
		REGISTERED, 
		ACTIVE };

		private volatile BoltState state;

		private ZooKeeper zk;

		private String zooIP;

		private Integer zooPort;

		private Integer taskID;

		private String taskName;

		public Pair<Double, Double> cpu;

		public Pair<Double, Double> mem;

		public Pair<Integer, Integer> latency;

		public Pair<Integer, Integer> throughput;

		//		public String scaleOutZnodeName;
		//
		//		public String scaleInZnodeName;

		public volatile String pendingCommand;

		private boolean submittedScaleTask = false;

		private String taskIP;

		/**
		 * The default constructor of the ZooPet class
		 * @param zoo_ip the IP address of the ZooKeeper server
		 * @param zoo_port the port of the ZooKeeper server
		 * @param task_name the corresponding task's name
		 * @param task_id the corresponding task's id
		 * @param task_ip the corresponding task's IP
		 */
		public ZooPet(String zoo_ip, Integer zoo_port, String task_name, Integer task_id, String task_ip) {
			this.zooIP = zoo_ip;
			this.zooPort = zoo_port;
			this.taskID = task_id;
			this.taskName = task_name;
			state = BoltState.INIT;
			//			scaleOutZnodeName = "";
			//			scaleInZnodeName = "";
			pendingCommand = null;
			this.taskIP = task_ip;
			cpu = new Pair<Double, Double>();
			mem = new Pair<Double, Double>();
			latency = new Pair<Integer, Integer>();
			throughput = new Pair<Integer, Integer>();
		}

		/**
		 * The watcher object responsible for handling incoming notifications 
		 * of newly created scale-out/in commands from Synefo. Every time a change 
		 * in the z-node's children for a component are changed (normally added new 
		 * children).
		 */
		Watcher boltWatcher = new Watcher() {
			public void process(WatchedEvent e) {
				if(e.getType() == Event.EventType.NodeDataChanged) {
					if(e.getPath().equals("/synefo/bolt-tasks/" + taskName + ":" + taskID + "@" + taskIP)) {
						/**
						 * Get scale command, and clean up the directory
						 */
						logger.info("boltWatcher.process(): Children of node " + e.getPath() + 
								" have changed. Time to check the scale-command.");
						getScaleCommand();
						//						setBoltNodeWatch();
					}
				}
			}
		};

		/**
		 * This function retrieves the newly-added child's data (the scale-out/in command) 
		 * and sets the inner pendingCommand object. The call to get the children is synchronous.
		 */
		public synchronized void getScaleCommand() {
			Stat stat = new Stat();
			try {
				pendingCommand = new String(zk.getData("/synefo/bolt-tasks/" + taskName + ":" + 
						taskID + "@" + taskIP, 
						boltWatcher, 
						stat));
				logger.info("getScaleCommand(): Received scale command \"" + pendingCommand + 
						"\" (" + taskName + ":" + taskID + "@" + taskIP + ")");
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		/**
		 * 
		 * @return the pendingCommand retrieved from a newly added z-node
		 */
		public synchronized String returnScaleCommand() {
			if(pendingCommand.toUpperCase().contains("ADD") || pendingCommand.toUpperCase().contains("REMOVE")) {
				String returnCommand = pendingCommand;
				pendingCommand = null;
				return returnCommand;
			}else {
				return null;
			}
		}

		/**
		 * The initialization function. In this function the corresponding 
		 * z-node for the component is created and will be under the 
		 * /synefo/bolt-tasks z-node with name task-name:task-id@task-IP .
		 * Also, the pre-defined thresholds are retrieved from the 
		 * /synefo/scale-out-event and /synefo/scale-in-event z-nodes.
		 * The ZooPet's state becomes ACTIVE.
		 */
		public void start() {
			try {
				zk = new ZooKeeper(zooIP + ":" + zooPort, 100000, boltWatcher);
				while(zk.getState() != ZooKeeper.States.CONNECTED) {
					Thread.sleep(100);
				}
				if(zk.exists("/synefo/bolt-tasks", false) != null) {
					zk.create("/synefo/bolt-tasks/" + taskName + ":" + taskID + "@" + taskIP, 
							("/synefo/bolt-tasks/" + taskName + ":" + taskID + "@" + taskIP).getBytes(), 
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
					logger.info("start(): Initialization successful (" + 
							taskName + ":" + taskID + "@" + taskIP + ")");
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch(InterruptedException e1) {
				e1.printStackTrace();
			}catch(KeeperException e2) {
				e2.printStackTrace();
			}
		}

		/**
		 * This function disconnects from the zookeeper ensemble.
		 */
		public void stop() {
			try {
				zk.close();
				logger.info("stop(): Closing connection (" + taskName + ":" + taskID + "@" + taskIP + ")");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		/**
		 * Function that sets an asynchronous watch on the z-node for the component
		 */
		//		public void setBoltNodeWatch() {
		//			zk.getData("/synefo/bolt-tasks/" + task_name + ":" + task_id + "@" + task_ip, 
		//					boltWatcher, 
		//					boltNodeDataCallback, 
		//					null);
		//		}

		/**
		 * Callback object for @setBoltNodeWatch() function
		 */
		//		private DataCallback boltNodeDataCallback = new DataCallback() {
		//			public void processResult(int rc, String path, Object ctx, byte[] data,
		//					Stat stat) {
		//				switch(Code.get(rc)) {
		//				case CONNECTIONLOSS:
		//					logger.info("boltNodeDataCallback(): CONNECTIONLOSS");
		//					setBoltNodeWatch();
		//					break;
		//				case OK:
		//					logger.info("boltNodeDataCallback(): OK");
		//					break;
		//				default:
		//					logger.info("boltNodeDataCallback(): Unexpected scenario: " + 
		//							KeeperException.create(Code.get(rc), path));
		//					break;
		//
		//				}
		//			}
		//		};

		/**
		 * This function checks the usage statistics of the component, and if they 
		 * are below the minimum thresholds, a scale-in request is created in the ZooKeeper ensemble. 
		 * Similarly, if the usage statistics are above the maximum thresholds, a scale-out 
		 * request is created in the ZooKeeper ensemble.
		 * @param cpu
		 * @param memory
		 * @param latency
		 * @param throughput
		 */
		public void setStatisticData(double cpu, double memory, Integer latency, Integer throughput) {
			//			if(state == BoltState.ACTIVE && scaleOutZnodeName.equals("") && scaleInZnodeName.equals("") && submittedScaleTask == false) {
			if(state == BoltState.ACTIVE && submittedScaleTask == false) {
				if(this.cpu.upperBound < cpu || this.mem.upperBound < memory) {
					/**
					 * Create also a node under scale-out-event znode with the name of the bolt.
					 * This way, the SynEFO coordination thread will understand that the bolt 
					 * is overloaded and that it needs to scale-out.
					 */
					logger.info("setStatisticData(): Over-utilization detected. Generating scale out command (" + taskName + ":" + taskID + "@" + taskIP + ")...");
					createScaleOutTask();
					submittedScaleTask = true;
				}else if(this.cpu.lowerBound > cpu || this.mem.lowerBound > memory) {
					/**
					 * Create also a node under scale-in-event znode with the name of the bolt.
					 * This way, the SynEFO coordination thread will understand that the bolt 
					 * is overloaded and that it needs to scale-out.
					 */
					logger.info("setStatisticData(): Under-utilization detected. Generating scale out command (" + taskName + ":" + taskID + "@" + taskIP + ")...");
					createScaleInTask();
					submittedScaleTask = true;
				}
			}
		}

		/**
		 * This function works as a way of avoiding re-submitting 
		 * scale-out/in requests on the server.
		 */
		public void resetSubmittedScaleFlag() {
			submittedScaleTask = false;
		}

		/**
		 * Function to create a scale-out task.
		 */
		private void createScaleOutTask() {
			zk.create("/synefo/scale-out-event/" + taskName + ":" + taskID + "@" + taskIP + "-", 
					(taskName + ":" + taskID + "@" + taskIP).getBytes(), 
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
					logger.info("createScaleOutEventCallback(): CONNECTIONLOSS for scale-event-creation");
					createScaleOutTask();
					break;
				case NONODE:
					logger.info("createScaleOutEventCallback(): NONODE for scale-event-creation");
					break;
				case NODEEXISTS:
					System.out.println("createScaleOutEventCallback(): NODEEXISTS for scale-event-creation");
					break;
				case OK:
					//					scaleOutZnodeName = name;
					logger.info("createScaleOutEventCallback(): OK for scale-event-creation");
					break;
				default:
					logger.info("createScaleOutEventCallback(): Unexpected scenario: " + 
							KeeperException.create(Code.get(rc), path));
					break;

				}
			}
		};

		private void createScaleInTask() {
			zk.create("/synefo/scale-in-event/" + taskName + ":" + taskID + "@" + taskIP + "-", 
					(taskName + ":" + taskID + "@" + taskIP).getBytes(), 
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
					logger.info("createScaleInEventCallback(): CONNECTIONLOSS for scale-event-creation");
					createScaleOutTask();
					break;
				case NONODE:
					logger.info("createScaleInEventCallback(): NONODE for scale-event-creation");
					break;
				case NODEEXISTS:
					logger.info("createScaleInEventCallback(): NODEEXISTS for scale-event-creation");
					break;
				case OK:
					//					scaleInZnodeName = name;
					logger.info("createScaleInEventCallback(): OK for scale-event-creation");
					break;
				default:
					logger.info("createScaleInEventCallback(): Unexpected scenario: " + 
							KeeperException.create(Code.get(rc), path));
					break;

				}
			}
		};

		//		public void scaleTaskGC() {
		//			if(scaleOutZnodeName.equals("") == false) {
		//				zk.delete("/synefo/scale-out-event/" + scaleOutZnodeName, 
		//						-1, scaleOutTaskGCcallback, null);
		//			}
		//			if(scaleInZnodeName.equals("") == false) {
		//				zk.delete("/synefo/scale-in-event/" + scaleInZnodeName, 
		//						-1, scaleInTaskGCcallback, null);
		//			}
		//		}
		//
		//		private VoidCallback scaleOutTaskGCcallback = new VoidCallback() {
		//			public void processResult(int rc, String path, Object ctx) {
		//				switch(Code.get(rc)) {
		//				case CONNECTIONLOSS:
		//					scaleTaskGC();
		//					break;
		//				case NONODE:
		//					scaleOutZnodeName = "";
		//					break;
		//				case OK:
		//					scaleOutZnodeName = "";
		//					break;
		//				default:
		//					logger.info("scaleOutTaskGCcallback(): Unexpected scenario: " + 
		//							KeeperException.create(Code.get(rc), path));
		//					break;
		//				}
		//			}
		//		};
		//
		//		private VoidCallback scaleInTaskGCcallback = new VoidCallback() {
		//			public void processResult(int rc, String path, Object ctx) {
		//				switch(Code.get(rc)) {
		//				case CONNECTIONLOSS:
		//					scaleTaskGC();
		//					break;
		//				case NONODE:
		//					scaleInZnodeName = "";
		//					break;
		//				case OK:
		//					scaleInZnodeName = "";
		//					break;
		//				default:
		//					logger.info("scaleInTaskGCcallback(): Unexpected scenario: " + 
		//							KeeperException.create(Code.get(rc), path));
		//					break;
		//				}
		//			}
		//		};

}
