package gr.katsip.synefo.server2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

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
 * ZooMaster is the class responsible for handling communication with the ZooKeeper ensemble, on 
 * the synefo side. One of its main responsibilities is to initialize the required z-node structure 
 * on the ZooKeeper ensemble, so that the synefo cluster can operate. Also, it is responsible to 
 * track changes of children of the directories /synefo/scale-out-event/ and /synefo/scale-in-event. 
 * Every time a children is added in the aforementioned directories, the scale function component of 
 * ZooMaster is responsible for deciding the scale-out or scale-in node accordingly.
 * 
 * @author Nick R. Katsipoulakis
 *
 */
public class ZooMaster {

	Logger logger = LoggerFactory.getLogger(ZooMaster.class);

	private enum SynefoState { INIT, BOOTSTRAPPED };

	private ZooKeeper zk;

	private String zoo_ip;

	private Integer zoo_port;

	private SynefoState state;

	public ConcurrentHashMap<String, ArrayList<String>> physicalTopology;

	public ScaleFunction scaleFunction;

	private ConcurrentLinkedQueue<String> scaleRequests;

	private ConcurrentHashMap<String, Boolean> servedScaleRequests;
	
	private ConcurrentHashMap<Integer, JoinOperator> taskToJoinRelation = null;

	/**
	 * Watcher object responsible for tracking storm components' requests 
	 * for scale-out/in operations. According to the z-node path of children 
	 * change, a different process is called.
	 */
	Watcher synefoWatcher = new Watcher() {
		public void process(WatchedEvent e) {
			if(e.getType() == Event.EventType.NodeChildrenChanged) {
				System.out.println("synefoWatcher # Children change detected");
				if(e.getPath().equals("/synefo/scale-out-event")) {
					/**
					 * Somehow decide the action to take 
					 * and have the scale-out-command as String <ACTION{ADD|REMOVE}>-<TASK_ID>
					 */
					System.out.println("synefoWatcher # scale-out event added.");
					setScaleOutEventWatch();
				}else if(e.getPath().equals("/synefo/scale-in-event")) {
					/**
					 * Somehow decide the action to take 
					 * and have the scale-out-command as String <ACTION{ADD|REMOVE}>-<TASK_ID>
					 */
					System.out.println("synefoWatcher # scale-in event added.");
					setScaleInEventWatch();
				}
				String scaleRequest = null;
				while((scaleRequest = scaleRequests.poll()) != null) {
					String[] tokens = scaleRequest.split("#");
					if(servedScaleRequests.containsKey(scaleRequest) == false) {
						servedScaleRequests.put(scaleRequest, true);
						if(tokens[0].equals("scale-out")) {
							/**
							 * New child located: Time to set the scale-out 
							 * command for that child
							 */
							System.out.println("ZooMaster # identified new scale-out request: " + scaleRequest);
							String[] childTokens = tokens[1].split("-");
							String childWorker = childTokens[0];
							String upstream_task = ScaleFunction.getParentNode(
									scaleFunction.physicalTopology,
									childWorker.substring(0, childWorker.lastIndexOf(':')),
									childWorker.substring(childWorker.lastIndexOf(':') + 1, childWorker.length()));
							String command = scaleFunction.produceScaleOutCommand(upstream_task, childWorker);
							String activateCommand = ScaleFunction.produceActivateCommand(command);
							System.out.println("ZooMaster # produced command: " + command + ", along with activate command: " + 
									activateCommand);
							if(command.equals("") == false) {
								ArrayList<String> peerParents = ScaleFunction.getInverseTopology(physicalTopology)
										.get(command.substring(command.lastIndexOf("~") + 1, command.length()));
								peerParents.remove(peerParents.indexOf(upstream_task));
								setScaleCommand(upstream_task, command, peerParents, activateCommand);
							}else {
								System.out.println("ZooMaster # no scale-out command produced" + 
										"(synefo-component:" + childWorker + ", upstream-component: " + upstream_task + ")."
										);
							}
						}else if(tokens[0].equals("scale-in")) {
							System.out.println("ZooMaster # identified new scale-in request: " + scaleRequest);
							String[] childTokens = tokens[1].split("-");
							String childWorker = childTokens[0];
							String upstream_task = ScaleFunction.getParentNode(
									scaleFunction.physicalTopology,
									childWorker.substring(0, childWorker.lastIndexOf(':')),
									childWorker.substring(childWorker.lastIndexOf(':') + 1, childWorker.length()));
							System.out.println("(1) ZooMaster # upstream_task: " + upstream_task);
							String command = scaleFunction.produceScaleInCommand(upstream_task, childWorker);
							String deActivateCommand = ScaleFunction.produceDeactivateCommand(command);
							System.out.println("(2) ZooMaster # produced command: " + command + ", along with deactivate command: " 
									+ deActivateCommand);
							if(command.equals("") == false) {
								ArrayList<String> peerParents = ScaleFunction.getInverseTopology(physicalTopology)
										.get(command.substring(command.lastIndexOf("~") + 1, command.length()));
								peerParents.remove(peerParents.indexOf(upstream_task));
								setScaleCommand(upstream_task, command, peerParents, deActivateCommand);
							}else {
								System.out.println("ZooMaster # no scale-in command produced" + 
										"(synefo-component:" + childWorker + ", upstream-component: " + upstream_task + ")."
										);
							}
						}
					}else {
						System.out.println("ZooMaster # request: " + scaleRequest + " has already been served.");
					}
				}
			}
		}
	};

	/**
	 * default constructor of the ZooMaster component.
	 * @param zoo_ip the ZooKeeper ensemble IP
	 * @param zoo_port the ZooKeeper ensemble port
	 * @param physicalTopology an object reference to the physical-topology submitted to synefo
	 * @param activeTopology an object reference to the initially active components in the topology submitted to synefo
	 */
	public ZooMaster(String zoo_ip, Integer zoo_port, 
			ConcurrentHashMap<String, ArrayList<String>> physicalTopology, 
			ConcurrentHashMap<String, ArrayList<String>> activeTopology, 
			ConcurrentHashMap<Integer, JoinOperator> taskToJoinRelation) {
		this.zoo_ip = zoo_ip;
		this.zoo_port = zoo_port;
		state = SynefoState.INIT;
		this.physicalTopology = physicalTopology;
		this.taskToJoinRelation = taskToJoinRelation;
		this.scaleFunction = new ScaleFunction(physicalTopology, activeTopology, taskToJoinRelation);
		scaleRequests = new ConcurrentLinkedQueue<String>();
		servedScaleRequests = new ConcurrentHashMap<String, Boolean>();
	}

	/**
	 * the function called to initialize the z-nodes on the ZooKeeper ensemble side. 
	 * Attention need to be paid to the fact that start() does not handle the setup of 
	 * usage thresholds for nodes.
	 */
	public void start() {
		try {
			zk = new ZooKeeper(zoo_ip + ":" + zoo_port, 100000	, synefoWatcher);
			while(zk.getState() != ZooKeeper.States.CONNECTED) {
				Thread.sleep(100);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			if(zk.exists("/synefo", false) != null) {
				if(zk.exists("/synefo/physical-top", false) != null)
					zk.delete("/synefo/physical-top", -1);
				if(zk.exists("/synefo/active-top", false) != null)
					zk.delete("/synefo/active-top", -1);
				if(zk.exists("/synefo/scale-out-event", false) != null) {
					List<String> children = zk.getChildren("/synefo/scale-out-event", false);
					for(String child : children) {
						zk.delete("/synefo/scale-out-event/" + child, -1);
					}
					zk.delete("/synefo/scale-out-event", -1);
				}
				if(zk.exists("/synefo/scale-in-event", false) != null) {
					List<String> children = zk.getChildren("/synefo/scale-in-event", false);
					for(String child : children) {
						zk.delete("/synefo/scale-in-event/" + child, -1);
					}
					zk.delete("/synefo/scale-in-event", -1);
				}
				if(zk.exists("/synefo/bolt-tasks", false) != null) {
					List<String> children = zk.getChildren("/synefo/bolt-tasks", false);
					for(String child : children) {
						zk.delete("/synefo/bolt-tasks/" + child, -1);
					}
					zk.delete("/synefo/bolt-tasks", -1);
				}
				zk.delete("/synefo", -1);
			}
			zk.create("/synefo", "/synefo".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zk.create("/synefo/scale-out-event", "/synefo/scale-out-event".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zk.create("/synefo/scale-in-event", "/synefo/scale-in-event".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zk.create("/synefo/bolt-tasks", "/synefo/bolt-tasks".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zk.create("/synefo/active-top", "/synefo/active-top".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zk.create("/synefo/physical-top", "/synefo/physical-top".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			state = SynefoState.BOOTSTRAPPED;
		}catch(KeeperException e) {
			e.printStackTrace();
		}catch(InterruptedException e1) {
			e1.printStackTrace();
		}
	}

	/**
	 * This function stores the physical topology of the submitted topology to 
	 * the ZooKeeper ensemble. This is done mainly for availability purposes, in case 
	 * a synefo component (bolt or spout) fails.
	 */
	public void setPhysicalTopology() {
		try {
			zk.setData("/synefo/physical-top", serializeTopology(scaleFunction.physicalTopology).getBytes(), -1);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This function stores the active topology of the submitted topology to 
	 * the ZooKeeper ensemble. This is done mainly for availability purposes, in case 
	 * a synefo component (bolt or spout) fails.
	 */
	public void setActiveTopology() {
		try {
			zk.setData("/synefo/active-top", serializeTopology(scaleFunction.getActiveTopology()).getBytes(), -1);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This function sets the upper usage thresholds in the ZooKeeper ensemble. setScaleOutThresholds() 
	 * needs to be called after the start() function, in order to achieve successful synefo operation.
	 * @param cpu the upper limit CPU percentage used
	 * @param memory the upper limit of memory percentage used
	 * @param latency the upper limit of latency
	 * @param throughput the lower limit of throughput
	 */
	public void setScaleOutThresholds(double cpu, double memory, int latency, int throughput) {
		String thresholds = cpu + "," + memory + "," + latency + "," + throughput;
		try {
			zk.setData("/synefo/scale-out-event", thresholds.getBytes(), -1);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This function sets the lowest usage thresholds in the ZooKeeper ensemble. setScaleOutThresholds() 
	 * needs to be called after the start() function, in order to achieve successful synefo operation.
	 * @param cpu the lowest limit CPU percentage used
	 * @param memory the lowest limit of memory percentage used
	 * @param latency the lowest limit of latency
	 * @param throughput the upper limit of throughput
	 */
	public void setScaleInThresholds(double cpu, double memory, int latency, int throughput) {
		String thresholds = cpu + "," + memory + "," + latency + "," + throughput;
		try {
			zk.setData("/synefo/scale-in-event", thresholds.getBytes(), -1);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void setScaleOutEventWatch() {
		List<String> children = null;
		try {
			children = zk.getChildren("/synefo/scale-out-event", synefoWatcher);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
		if(children != null) {
			System.out.println("ZooMaster.scaleOutEventWatch() # OK children received: " + 
					children);
			if(state == SynefoState.BOOTSTRAPPED) {
				for(String child : children) {
					if(!scaleRequests.contains("scale-out#" + child) && 
							servedScaleRequests.containsKey("scale-out#" + child) == false) {
						System.out.println("ZooMaster.scaleOutEventWatch() # identified new scale-out request: " + 
								child);
						scaleRequests.offer("scale-out#" + child);
					}
				}
			}
		}
	}

	public void setScaleInEventWatch() {
		List<String> children = null;
		try {
			children = zk.getChildren("/synefo/scale-in-event", synefoWatcher);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
		if(children != null) {
			System.out.println("ZooMaster.scaleInEventWatch() # OK children received: " + 
					children);
			if(state == SynefoState.BOOTSTRAPPED) {
				for(String child : children) {
					if(!scaleRequests.contains("scale-in#" + child) && 
							servedScaleRequests.containsKey("scale-in#" + child) == false) {
						System.out.println("ZooMaster.scaleInEventWatch() # identified new scale-in request: " + 
								child);
						scaleRequests.offer("scale-in#" + child);
					}
				}
			}
		}
	}

	/**
	 * this function is called in order to set a scale-out/in command for the synefo 
	 * components.
	 * @param upstreamTask the upstream-task component of the about-to-scale-out/in component
	 * @param command either ADD or REMOVE
	 */
	public void setScaleCommand(String upstreamTask, String command, List<String> peerParents, String activateCommand) {
		try {
			Stat stat = zk.setData("/synefo/bolt-tasks/" + upstreamTask, (command).getBytes(), -1);
			System.out.println("setScaleCommand(): Setting command: \"" + command + "\", on path: " + 
					"\"/synefo/bolt-tasks/" + upstreamTask + "\" returned version stat: " + stat.getVersion() + ".");
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
		for(String parent : peerParents) {
			try {
				Stat stat = zk.setData("/synefo/bolt-tasks/" + parent, (activateCommand).getBytes(), -1);
				System.out.println("setScaleCommand(): Setting command: \"" + activateCommand + "\", on path: " + 
						"\"/synefo/bolt-tasks/" + parent + "\" returned version stat: " + stat.getVersion() + ".");
			} catch (KeeperException | InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * this function is called in order to close communication with the ZooKeeper ensemble.
	 */
	public void stop() {
		try {
			zk.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static String serializeTopology(ConcurrentHashMap<String, ArrayList<String>> topology) {
		StringBuilder strBuild = new StringBuilder();
		strBuild.append("{");
		Iterator<Entry<String, ArrayList<String>>> itr = topology.entrySet().iterator();
		while(itr.hasNext()) {
			Entry<String, ArrayList<String>> entry = itr.next();
			String task = entry.getKey();
			strBuild.append(task + ":");
			for(String d_task : entry.getValue()) {
				strBuild.append(d_task + ",");
			}
			if(strBuild.length() > 0 && strBuild.charAt(strBuild.length() - 1) == ',') {
				strBuild.setLength(strBuild.length() - 1);
			}
			strBuild.append("|");
		}
		if(strBuild.length() > 0 && strBuild.charAt(strBuild.length() - 1) == '|') {
			strBuild.setLength(strBuild.length() - 1);
		}
		strBuild.append("}");
		return strBuild.toString();
	}

	public static HashMap<String, ArrayList<String>> deserializeTopology(String topology) {
		HashMap<String, ArrayList<String>> top = new HashMap<String, ArrayList<String>>();
		String[] tokens = topology.split("{|}");
		for(String task : tokens) {
			if(task != null && task != "") {
				String[] tasks = task.split(":");
				String up_task = tasks[0];
				ArrayList<String> d_tasks = new ArrayList<String>();
				for(int i = 1; i < tasks.length; i++) {
					d_tasks.add(tasks[i]);
				}
				top.put(up_task, d_tasks);
			}
		}
		return top;
	}

}
