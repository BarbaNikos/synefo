package gr.katsip.synefo.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
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

	private List<String> scale_event_children;

	private HashMap<String, ArrayList<String>> physical_topology;

	private HashMap<String, ArrayList<String>> active_topology;

	private ScaleFunction scaleFunction;

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

	public ZooMaster(String zoo_ip, Integer zoo_port, ScaleFunction scaleFunction) {
		this.zoo_ip = zoo_ip;
		this.zoo_port = zoo_port;
		state = SynefoState.INIT;
		this.scaleFunction = scaleFunction;
		scale_event_children = new ArrayList<String>();
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

	public void set_physical_top(HashMap<String, ArrayList<String>> topology) {
		physical_topology = new HashMap<String, ArrayList<String>>(topology);
		scaleFunction.physical_topology = physical_topology;
//		zk.setData("/synefo/physical-top", serializeTopology(topology).getBytes(), -1, setTopologyCallback, topology);
		try {
			zk.setData("/synefo/physical-top", serializeTopology(topology).getBytes(), -1);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void set_active_top(HashMap<String, ArrayList<String>> topology) {
		active_topology = new HashMap<String, ArrayList<String>>(topology);
		scaleFunction.active_topology = active_topology;
//		zk.setData("/synefo/active-top", serializeTopology(topology).getBytes(), -1, setTopologyCallback, topology);
		try {
			zk.setData("/synefo/active-top", serializeTopology(topology).getBytes(), -1);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

//	StatCallback setTopologyCallback = new StatCallback() {
//		public void processResult(int rc, String path, Object ctx, Stat stat) {
//			switch(Code.get(rc)) {
//			case CONNECTIONLOSS:
//				System.out.println("SynEFO.setTopologyCallback(): CONNECTIONLOSS (" + path + ")");
//				if(path.equals("/synefo/physical-top")) {
//					set_physical_top((HashMap<String, ArrayList<String>>) ctx);
//				}else {
//					set_active_top((HashMap<String, ArrayList<String>>) ctx);
//				}
//				break;
//			case NONODE:
//				System.out.println("SynEFO.setTopologyCallback(): NONODE (" + path + ")");
//				if(path.equals("/synefo/physical-top")) {
//					createChildNodePhysicalTop();
//					set_physical_top((HashMap<String, ArrayList<String>>) ctx);
//				}else {
//					createChildNodeActiveTop();
//					set_active_top((HashMap<String, ArrayList<String>>) ctx);
//				}
//				break;
//			case OK:
//				System.out.println("SynEFO.setTopologyCallback(): OK (" + path + ")");
//				break;
//			default:
//				System.out.println("SynEFO.setTopologyCallback(): default case (" + path + "). Reason: " + 
//						KeeperException.create(Code.get(rc), path));
//				break;
//
//			}
//		}
//	};

	public void set_scaleout_thresholds(double cpu, double memory, int latency, int throughput) {
		String thresholds = cpu + "," + memory + "," + latency + "," + throughput;
		try {
			zk.setData("/synefo/scale-out-event", thresholds.getBytes(), -1);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void set_scalein_thresholds(double cpu, double memory, int latency, int throughput) {
		String thresholds = cpu + "," + memory + "," + latency + "," + throughput;
		try {
			zk.setData("/synefo/scale-in-event", thresholds.getBytes(), -1);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
//
//	StatCallback setThresholdsCallback = new StatCallback() {
//		public void processResult(int rc, String path, Object ctx, Stat stat) {
//			StringTokenizer strTok = new StringTokenizer(new String((String) ctx), ",");
//			double cpu = Double.parseDouble(strTok.nextToken());
//			double mem = Double.parseDouble(strTok.nextToken());
//			int latency = Integer.parseInt(strTok.nextToken());
//			int throughput = Integer.parseInt(strTok.nextToken());
//			switch(Code.get(rc)) {
//			case CONNECTIONLOSS:
//				System.out.println("SynEFO.setThresholdsCallback(): CONNECTIONLOSS (path: " + path + ").");
//				set_scaleout_thresholds(cpu, mem, latency, throughput);
//				break;
//			case NODEEXISTS:
//				System.out.println("SynEFO.setThresholdsCallback(): NODEEXISTS (path: " + path + ").");
//				break;
//			case NONODE:
//				System.out.println("SynEFO.setThresholdsCallback(): NONODE (path: " + path + ").");
//				set_scaleout_thresholds(cpu, mem, latency, throughput);
//				break;
//			case OK:
//				System.out.println("SynEFO.setThresholdsCallback(): OK (path: " + path + ").");
//				break;
//			default:
//				System.out.println("SynEFO.setThresholdsCallback(): default case (" + path + "). Reason: " + 
//						KeeperException.create(Code.get(rc), path));
//				break;
//
//			}
//		}
//	};

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
					if(scale_event_children.lastIndexOf(child) < 0 && state == SynefoState.BOOTSTRAPPED) {
						/**
						 * New child located: Time to set the scale-out 
						 * command for that child
						 */
						StringTokenizer strTok = new StringTokenizer(child, "-");
						String childWorker = strTok.nextToken();
						String upstream_task = scaleFunction.getParentNode(
								childWorker.substring(0, childWorker.lastIndexOf(':')),
								childWorker.substring(childWorker.lastIndexOf(':') + 1, childWorker.length()));
						String command = scaleFunction.produceScaleOutCommand(childWorker);
						if(command.equals("") == false)
							setScaleCommand(upstream_task, command);
					}
				}
				scale_event_children = new ArrayList<String>(children);
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

	/**
	 * TODO: Recheck this one! It is not complete
	 */
	ChildrenCallback scaleInEventChildrenCallback = new ChildrenCallback() {
		public void processResult(int rc, String path, Object ctx,
				List<String> children) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				System.out.println("SynEFO.setScaleInEventWatch(): CONNECTIONLOSS");
				setScaleOutEventWatch();
				break;
			case OK:
				System.out.println("SynEFO.scaleInEventChildrenCallback(): OK children received: " + 
						children);
				for(String child : children) {
					if(scale_event_children.lastIndexOf(child) < 0 && state == SynefoState.BOOTSTRAPPED) {
						/**
						 * New child located: Time to set the scale-out 
						 * command for that child
						 */
						StringTokenizer strTok = new StringTokenizer(child, "-");
						String childWorker = strTok.nextToken();
						String upstream_task = scaleFunction.getParentNode(
								childWorker.substring(0, childWorker.lastIndexOf(':')),
								childWorker.substring(childWorker.lastIndexOf(':') + 1, childWorker.length()));
						String command = scaleFunction.produceScaleInCommand(childWorker);
						if(command.equals("") == false)
							setScaleCommand(upstream_task, command);
					}
				}
				scale_event_children = new ArrayList<String>(children);
				break;
			case NONODE:
				setScaleOutEventWatch();
				break;
			default:
				System.out.println("SynEFO.scaleInEventChildrenCallback() unexpected error: " + KeeperException.create(Code.get(rc)));
				break;

			}
		}
	};

	public void setScaleCommand(String child, String command) {
		zk.setData("/synefo/bolt-tasks/" + child, 
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

	public void initSynefo() {
		zk.getData("/synefo", false, synefoCheckCallback, null);
	}

	private DataCallback synefoCheckCallback = new DataCallback() {
		public void processResult(int rc, String path, Object ctx,
				byte[] data, Stat stat) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				initSynefo();
				break;
			case NODEEXISTS:
				deleteSynefo();
			case NONODE:
				createSynefo();
				return;
			case OK:
				deleteSynefo();
			default:
				break;
			}
		}
	};

	private void deleteSynefo() {
		zk.delete("/synefo", -1, deleteSynefoCallback, null);
	}

	private VoidCallback deleteSynefoCallback = new VoidCallback() {
		public void processResult(int rc, String path, Object ctx) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				deleteSynefo();
				break;
			case NONODE:
				createSynefo();
				break;
			case OK:
				createSynefo();
				break;
			default:
			}
		}

	};

	private void createSynefo() {
		zk.create("/synefo", 
				"/synefo".getBytes(), 
				Ids.OPEN_ACL_UNSAFE, 
				CreateMode.PERSISTENT, 
				createSynefoNodeCallback, 
				null);
	}

	StringCallback createSynefoNodeCallback = new StringCallback() {
		public void processResult(int rc, String path, Object ctx,
				String name) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				System.out.println("SynEFO.createSynefoNodeCallback(): CONNECTIONLOSS (for path: " + path + ").");
				createSynefo();
				break;
			case NONODE:
				System.out.println("SynEFO.createSynefoNodeCallback(): NONODE (for path: " + path + ").");
				createSynefo();
				break;
			case OK:
				System.out.println("SynEFO.createSynefoNodeCallback(): OK (for path: " + path + ").");
				if(state != SynefoState.BOOTSTRAPPED) {
					createChildNodeScaleEvent();
					createChildNodeActiveTop();
					createChildNodePhysicalTop();
					createChildNodeTasks();
				}
				break;
			case NODEEXISTS:
				System.out.println("SynEFO.createSynefoNodeCallback(): NODEEXISTS (for path: " + path + ").");
				if(state != SynefoState.BOOTSTRAPPED) {
					createChildNodeScaleEvent();
					createChildNodeActiveTop();
					createChildNodePhysicalTop();
					createChildNodeTasks();
				}
				break;
			default:
				System.out.println("SynEFO.createSynefoNodeCallback(): default case (" + path + "). Reason: " + 
						KeeperException.create(Code.get(rc), path));
			}
		}
	};

	private void createChildNodeScaleEvent() {
		if(state != SynefoState.BOOTSTRAPPED)
			createSynefoNode("/synefo/scale-out-event", "/synefo/scale-out-event".getBytes());
	}

	private void createChildNodeActiveTop() {
		if(state != SynefoState.BOOTSTRAPPED)
			createSynefoNode("/synefo/physical-top", "/synefo/physical-top".getBytes());
	}

	private void createChildNodePhysicalTop() {
		if(state != SynefoState.BOOTSTRAPPED)
			createSynefoNode("/synefo/active-top", "/synefo/active-top".getBytes());
	}

	private void createChildNodeTasks() {
		if(state != SynefoState.BOOTSTRAPPED)
			createSynefoNode("/synefo/bolt-tasks", "/synefo/bolt-tasks".getBytes());
	}

	private void createSynefoNode(String path, byte[] data) {
		if(state != SynefoState.BOOTSTRAPPED)
			zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createSynefoChildNodeCallback, data);
	}

	StringCallback createSynefoChildNodeCallback = new StringCallback() {
		public void processResult(int rc, String path, Object ctx,
				String name) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				createSynefoNode(path, path.getBytes());
				break;
			case NODEEXISTS:
				System.out.println("SynEFO.createSynefoChildNodeCallback(): NODEEXISTS (" + path + ")");
				if(path.equals("/synefo/bolt-tasks")) {
					state = SynefoState.BOOTSTRAPPED;
				}
				break;
			case NONODE:
				System.out.println("SynEFO.createSynefoChildNodeCallback(): NONODE (" + path + ")");
				break;
			case OK:
				System.out.println("SynEFO.createSynefoChildNodeCallback(): OK (" + path + ")");
				if(path.equals("/synefo/bolt-tasks")) {
					System.out.println("SynEFO.createSynefoChildNodeCallback(): BOOTSTRAPPED");
					state = SynefoState.BOOTSTRAPPED;
				}
				return;
			default:
				System.out.println("SynEFO.createSynefoChildNodeCallback(): default case (" + path + "). Reason: " + 
						KeeperException.create(Code.get(rc), path));
				break;
			}
		}

	};

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
