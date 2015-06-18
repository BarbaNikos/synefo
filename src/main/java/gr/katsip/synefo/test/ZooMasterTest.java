package gr.katsip.synefo.test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

import gr.katsip.synefo.server.ScaleFunction;
import gr.katsip.synefo.server.ZooMaster;


public class ZooMasterTest {

	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		ZooKeeper zk = null;
		HashMap<String, ArrayList<String>> physicalTopology = 
				new HashMap<String, ArrayList<String>>();
		ArrayList<String> _tmp;
		_tmp = new ArrayList<String>();
		_tmp.add("join_bolt_1");
		_tmp.add("join_bolt_2");
		physicalTopology.put("spout_1a", new ArrayList<String>(_tmp));
		physicalTopology.put("spout_1b", new ArrayList<String>(_tmp));
		physicalTopology.put("spout_2a", new ArrayList<String>(_tmp));
		physicalTopology.put("spout_2b", new ArrayList<String>(_tmp));
		_tmp = new ArrayList<String>();
		_tmp.add("drain_bolt");
		physicalTopology.put("join_bolt_1", new ArrayList<String>(_tmp));
		physicalTopology.put("join_bolt_2", new ArrayList<String>(_tmp));
		physicalTopology.put("drain_bolt", new ArrayList<String>());
		
		HashMap<String, ArrayList<String>> activeTopology = ScaleFunction.getInitialActiveTopology(
				physicalTopology, ScaleFunction.getInverseTopology(physicalTopology));
		ZooMaster zooMaster = new ZooMaster("127.0.0.1:2181", physicalTopology, activeTopology);
		
		Watcher boltWatcher = new Watcher() {
			public void process(WatchedEvent e) {
				
			}
		};
		
		zk = new ZooKeeper("127.0.0.1:2181", 100000, boltWatcher);
		zk.create("/synefo/bolt-tasks/spout_1a", 
				("/synefo/bolt-tasks/spout_1a").getBytes(), 
				Ids.OPEN_ACL_UNSAFE, 
				CreateMode.PERSISTENT);
		zk.create("/synefo/bolt-tasks/spout_1b", 
				("/synefo/bolt-tasks/spout_1b").getBytes(), 
				Ids.OPEN_ACL_UNSAFE, 
				CreateMode.PERSISTENT);
		zk.create("/synefo/bolt-tasks/spout_2a", 
				("/synefo/bolt-tasks/spout_2a").getBytes(), 
				Ids.OPEN_ACL_UNSAFE, 
				CreateMode.PERSISTENT);
		zk.create("/synefo/bolt-tasks/spout_2b", 
				("/synefo/bolt-tasks/spout_2b").getBytes(), 
				Ids.OPEN_ACL_UNSAFE, 
				CreateMode.PERSISTENT);
		zooMaster.start();
		
		List<String> peerParents = new ArrayList<String>();
		peerParents.add("spout_1b");
		peerParents.add("spout_2a");
		peerParents.add("spout_2b");
		zooMaster.setScaleCommand("spout_1a", "ADD~join_bolt_2", peerParents, "ACTIVATE~join_bolt_2");
		peerParents.remove(peerParents.indexOf("spout_1b"));
		peerParents.add("spout_1a");
		zooMaster.setScaleCommand("spout_1b", "REMOVE~join_bolt_1", peerParents, "DEACTIVATE~join_bolt_1");
	}

}
