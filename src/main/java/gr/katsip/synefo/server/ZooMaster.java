package gr.katsip.synefo.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException.Code;

public class ZooMaster {

	private enum SynefoState { INIT, BOOTSTRAPPED };

	private ZooKeeper zk;

	private String zoo_ip;

	private Integer zoo_port;

	private SynefoState state;

	private List<String> scaleOutEventChildren;
	
	private List<String> scaleInEventChildren;

	public HashMap<String, ArrayList<String>> physicalTopology;

	public HashMap<String, ArrayList<String>> activeTopology;

	public ScaleFunction scaleFunction;

	Watcher synefoWatcher = new Watcher() {
		public void process(WatchedEvent e) {
			if(e.getType() == Event.EventType.NodeChildrenChanged) {
				System.out.println("synefoWatcher.process(): Children changed");
				if(e.getPath().equals("/synefo/scale-out-event")) {
					/**
					 * Somehow decide the action to take 
					 * and have the scale-out-command as String <ACTION{ADD|REMOVE}>-<TASK_ID>
					 */
					System.out.println("synefoWatcher.process(): Scale-out event added");
					setScaleOutEventWatch();
				}else if(e.getPath().equals("/synefo/scale-in-event")) {
					/**
					 * Somehow decide the action to take 
					 * and have the scale-out-command as String <ACTION{ADD|REMOVE}>-<TASK_ID>
					 */
					System.out.println("synefoWatcher.process(): Scale-in event added");
					setScaleInEventWatch();
				}
			}
		}

	};

	public ZooMaster(String zoo_ip, Integer zoo_port, 
			HashMap<String, ArrayList<String>> physicalTopology, 
			HashMap<String, ArrayList<String>> activeTopology) {
		this.zoo_ip = zoo_ip;
		this.zoo_port = zoo_port;
		state = SynefoState.INIT;
		this.physicalTopology = physicalTopology;
		this.activeTopology = activeTopology;
		this.scaleFunction = new ScaleFunction(physicalTopology, activeTopology);
		scaleOutEventChildren = new ArrayList<String>();
		scaleInEventChildren = new ArrayList<String>();
	}

	public void start() {
		try {
			zk = new ZooKeeper(zoo_ip, zoo_port, synefoWatcher);
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			if(zk.exists("/synefo", false) != null) {
				zk.delete("/synefo/physical-top", -1);
				zk.delete("/synefo/active-top", -1);
				List<String> children = zk.getChildren("/synefo/scale-out-event", false);
				for(String child : children) {
					zk.delete("/synefo/scale-out-event/" + child, -1);
				}
				zk.delete("/synefo/scale-out-event", -1);
				children = zk.getChildren("/synefo/scale-in-event", false);
				for(String child : children) {
					zk.delete("/synefo/scale-in-event/" + child, -1);
				}
				zk.delete("/synefo/scale-in-event", -1);
				children = zk.getChildren("/synefo/bolt-tasks", false);
				for(String child : children) {
					zk.delete("/synefo/bolt-tasks/" + child, -1);
				}
				zk.delete("/synefo/bolt-tasks", -1);
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

	public void setPhysicalTopology() {
		try {
			zk.setData("/synefo/physical-top", serializeTopology(scaleFunction.physicalTopology).getBytes(), -1);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void setActiveTopology() {
		try {
			zk.setData("/synefo/active-top", serializeTopology(scaleFunction.activeTopology).getBytes(), -1);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

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
		zk.getChildren("/synefo/scale-out-event", 
				synefoWatcher, 
				scaleOutEventChildrenCallback, 
				null);
	}

	ChildrenCallback scaleOutEventChildrenCallback = new ChildrenCallback() {
		public void processResult(int rc, String path, Object ctx,
				List<String> children) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				System.out.println("SynEFO.setScaleOutEventWatch(): CONNECTIONLOSS");
				setScaleOutEventWatch();
				break;
			case OK:
				System.out.println("SynEFO.scaleOutEventChildrenCallback(): OK children received: " + 
						children);
				for(String child : children) {
					if(scaleOutEventChildren.lastIndexOf(child) < 0) {
						/**
						 * New child located: Time to set the scale-out 
						 * command for that child
						 */
						System.out.println("SynEFO.scaleOutEventChildrenCallback(): Identified new scale-out request.");
						StringTokenizer strTok = new StringTokenizer(child, "-");
						String childWorker = strTok.nextToken();
						String upstream_task = scaleFunction.getParentNode(
								childWorker.substring(0, childWorker.lastIndexOf(':')),
								childWorker.substring(childWorker.lastIndexOf(':') + 1, childWorker.length()));
						String command = scaleFunction.produceScaleOutCommand(upstream_task, childWorker);
						System.out.println("SynEFO.scaleOutEventChildrenCallback(): produced command: " + command);
						if(command.equals("") == false)
							setScaleCommand(upstream_task, command);
					}
				}
				scaleOutEventChildren = new ArrayList<String>(children);
				break;
			case NONODE:
				setScaleOutEventWatch();
				break;
			default:
				System.out.println("SynEFO.scaleOutEventChildrenCallback() unexpected error: " + KeeperException.create(Code.get(rc)));
				break;

			}
		}	
	};

	public void setScaleInEventWatch() {
		zk.getChildren("/synefo/scale-in-event", 
				synefoWatcher, 
				scaleInEventChildrenCallback, 
				null);
	}

	ChildrenCallback scaleInEventChildrenCallback = new ChildrenCallback() {
		public void processResult(int rc, String path, Object ctx,
				List<String> children) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				System.out.println("SynEFO.setScaleInEventWatch(): CONNECTIONLOSS");
				setScaleInEventWatch();
				break;
			case OK:
				System.out.println("SynEFO.scaleInEventChildrenCallback(): OK children received: " + 
						children);
				for(String child : children) {
					if(scaleInEventChildren.lastIndexOf(child) < 0 && state == SynefoState.BOOTSTRAPPED) {
						/**
						 * New child located: Time to set the scale-out 
						 * command for that child
						 */
						System.out.println("SynEFO.scaleInEventChildrenCallback(): Identified new scale-in request.");
						StringTokenizer strTok = new StringTokenizer(child, "-");
						String childWorker = strTok.nextToken();
						String upstream_task = scaleFunction.getParentNode(
								childWorker.substring(0, childWorker.lastIndexOf(':')),
								childWorker.substring(childWorker.lastIndexOf(':') + 1, childWorker.length()));
						String command = scaleFunction.produceScaleInCommand(childWorker);
						System.out.println("SynEFO.scaleInEventChildrenCallback(): produced command: " + command);
						if(command.equals("") == false)
							setScaleCommand(upstream_task, command);
					}
				}
				scaleInEventChildren = new ArrayList<String>(children);
				break;
			case NONODE:
				setScaleInEventWatch();
				break;
			default:
				System.out.println("SynEFO.scaleInEventChildrenCallback() unexpected error: " + KeeperException.create(Code.get(rc)));
				break;

			}
		}
	};

	public void setScaleCommand(String upstreamTask, String command) {
		zk.setData("/synefo/bolt-tasks/" + upstreamTask, 
				(command).getBytes(), 
				-1, 
				setScaleCommandCallback, 
				null);
	}

	StatCallback setScaleCommandCallback = new StatCallback() {
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch(Code.get(rc)) {
			case OK:
				System.out.println("SynEFO.setScaleCommandCallback(): Scale Command has been set properly.");
				break;
			default:
				System.out.println("SynEFO.setScaleCommandCallback(): Scale Command has had an unexpected result: " + 
						KeeperException.create(Code.get(rc)));
				break;
			}
		}

	};

	public void stop() {
		try {
			zk.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public String serializeTopology(HashMap<String, ArrayList<String>> topology) {
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

	public HashMap<String, ArrayList<String>> deserializeTopology(String topology) {
		HashMap<String, ArrayList<String>> top = new HashMap<String, ArrayList<String>>();
		StringTokenizer strTok = new StringTokenizer(topology, "{|}");
		while(strTok.hasMoreTokens()) {
			String task = strTok.nextToken();
			if(task != null && task != "") {
				StringTokenizer strTok1 = new StringTokenizer(task, ":,");
				String up_task = strTok1.nextToken();
				ArrayList<String> d_tasks = new ArrayList<String>();
				while(strTok1.hasMoreTokens()) {
					String d_task = strTok1.nextToken();
					d_tasks.add(d_task);
				}
				top.put(up_task, d_tasks);
			}
		}
		return top;
	}

}
