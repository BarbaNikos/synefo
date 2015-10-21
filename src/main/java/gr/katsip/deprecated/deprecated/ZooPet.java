package gr.katsip.deprecated.deprecated;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import gr.katsip.deprecated.server2.ZooMaster;
import gr.katsip.synefo.utils.Pair;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZooPet is the main class for handling interactions between a Storm component and the ZooKeeper service. Every time scale-out/in circumstances 
 * are met, ZooPet handles the creation of the appropriate z-nodes, so that BalanceServer takes the appropriate actions. Also, ZooPet is responsible for
 * creating the required z-nodes during the initialization phase.
 * 
 * @author Nick R. Katsipoulakis
 * @deprecated
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

		private String zookeeperAddress;

		private Integer taskIdentifier;

		private String taskName;

		public Pair<Double, Double> cpu;

		public Pair<Double, Double> mem;

		public Pair<Long, Long> latency;

		public Pair<Integer, Integer> throughput;
		
		public ConcurrentLinkedQueue<String> pendingCommands;
		
		private boolean submittedScaleOutTask = false;
		
		private boolean submittedScaleInTask = false;

		private String taskAddress;

		/**
		 * The default constructor of the ZooPet class
		 * @param zookeeperAddress the IP address of the ZooKeeper server in the form "ip-1:port-1,ip-2:port-2,...,ip-N:port-N"
		 * @param taskName the corresponding task's name
		 * @param taskIdentifier the corresponding task's id
		 * @param taskAddress the corresponding task's IP
		 */
		public ZooPet(String zookeeperAddress, String taskName, Integer taskIdentifier, String taskAddress) {
			this.zookeeperAddress = zookeeperAddress;
			this.taskIdentifier = taskIdentifier;
			this.taskName = taskName;
			state = BoltState.INIT;
			this.taskAddress = taskAddress;
			pendingCommands = new ConcurrentLinkedQueue<String>();
		}

		/**
		 * The watcher object responsible for handling incoming notifications 
		 * of newly created scale-out/in commands from BalanceServer. Every time a change
		 * in the z-node's children for a component are changed (normally added new 
		 * children).
		 */
		Watcher boltWatcher = new Watcher() {
			public void process(WatchedEvent e) {
				if (e.getType() == Event.EventType.NodeDataChanged) {
					if (e.getPath().equals("/synefo/bolt-tasks/" + taskName + ":" + taskIdentifier + "@" + taskAddress)) {
						/**
						 * Get scale command, and clean up the directory
						 */
						logger.info("children of node " + e.getPath() +
								" have changed. Time to check the scale-command.");
						getScaleCommand();
					}else if (e.getPath().equals("/synefo/active-top")) {
						logger.info(" active topology has changed.");
					}
				}
			}
		};

	public void createJoinStateNode(String taskName, Integer taskIdentifier) {
		try {
			if (zk.exists("/synefo/join-state/" + taskName + ":" + taskIdentifier, false) != null) {
				zk.delete("/synefo/join-state/" + taskName + ":" + taskIdentifier, -1);
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			zk.create("/synefo/join-state/" + taskName + ":" + taskIdentifier, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void setJoinStateDataScaleIn(String taskName, Integer taskIdentifier, HashMap<String, List<String>> keyToTaskMap) {
		try {
			zk.setData("/synefo/join-state/" + taskName + ":" + taskIdentifier, keyToTaskMap.toString().getBytes("UTF-8"), -1);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	public void createChildJoinStateDataScaleOut(String receivingTaskName, Integer receivingTaskIdentifier, List<String> keys) {
		try {
			zk.create("/synefo/join-state/" + receivingTaskName + ":" + receivingTaskIdentifier,
					keys.toString().getBytes("UTF-8"), Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT_SEQUENTIAL);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
	}

	public List<Integer> getActiveTopology(String taskName, Integer taskIdentifier, String taskAddress) {
		Stat stat = new Stat();
		ConcurrentHashMap<String, ArrayList<String>> activeTopology = null;
		try {
			activeTopology = ZooMaster.deserializeTopology(new String(zk.getData("/synefo/active-top", boltWatcher, stat), "UTF-8"));
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		if (activeTopology.contains(taskName + ":" + taskIdentifier + "@" + taskAddress)) {
			ArrayList<String> activeDownstreamTaskNames = activeTopology.get(taskName + ":" + taskIdentifier + "@" + taskAddress);
			List<Integer> activeDownstreamTaskIdentifiers = new ArrayList<Integer>();
			for (String activeTask : activeDownstreamTaskNames) {
				Integer task = Integer.parseInt(activeTask.split("[:@]")[1]);
				activeDownstreamTaskIdentifiers.add(task);
			}
			return activeDownstreamTaskIdentifiers;
		}else {
			return null;
		}
	}

		/**
		 * This function retrieves the newly-added child's data (the scale-out/in command) 
		 * and sets the inner pendingCommand object. The call to get the children is synchronous.
		 */
		public synchronized void getScaleCommand() {
			Stat stat = new Stat();
			try {
				String pendingCommand = new String(zk.getData("/synefo/bolt-tasks/" + taskName + ":" +
								taskIdentifier + "@" + taskAddress,
						boltWatcher, 
						stat), "UTF-8");
				logger.info("getScaleCommand(): Received scale command \"" + pendingCommand + 
						"\" (" + taskName + ":" + taskIdentifier + "@" + taskAddress + ")");
				pendingCommands.offer(pendingCommand);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		}

		/**
		 * 
		 * @return the pendingCommand retrieved from a newly added z-node
		 */
		public synchronized String returnScaleCommand() {
			String returnCommand = pendingCommands.poll();
			if(returnCommand != null && (returnCommand.toUpperCase().contains("ADD") || returnCommand.toUpperCase().contains("REMOVE") || 
					returnCommand.toUpperCase().contains("ACTIVATE") || returnCommand.toUpperCase().contains("DEACTIVATE"))) {
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
				zk = new ZooKeeper(zookeeperAddress, 100000, boltWatcher);
				while(zk.getState() != ZooKeeper.States.CONNECTED) {
					Thread.sleep(100);
				}
				if(zk.exists("/synefo/bolt-tasks", false) != null) {
					if(zk.exists("/synefo/bolt-tasks/" + taskName + ":" + taskIdentifier + "@" + taskAddress, false) == null) {
						zk.create("/synefo/bolt-tasks/" + taskName + ":" + taskIdentifier + "@" + taskAddress,
								("/synefo/bolt-tasks/" + taskName + ":" + taskIdentifier + "@" + taskAddress).getBytes("UTF-8"),
								Ids.OPEN_ACL_UNSAFE, 
								CreateMode.PERSISTENT);
					}
					Stat stat = new Stat();
					state = BoltState.ACTIVE;
					logger.info("start(): Initialization successful (" + 
							taskName + ":" + taskIdentifier + "@" + taskAddress + ")");
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
				logger.info("stop(): Closing connection (" + taskName + ":" + taskIdentifier + "@" + taskAddress + ")");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
}
