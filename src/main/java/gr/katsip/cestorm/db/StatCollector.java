package gr.katsip.cestorm.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class StatCollector {

	private ZooKeeper zk;
	
	private CopyOnWriteArrayList<String> operators;
	
	private Watcher dataRetrieverWatcher = new Watcher() {
		@Override
		public void process(WatchedEvent event) {
			String path = event.getPath();
			/**
			 * When you retrieve the path, call the getDataAndWatch() function to 
			 * retrieve data and set watch again
			 */
			System.out.print("process Event: " + event.getType().toString() + " ");
			if(event.getType() == Event.EventType.NodeDataChanged) {
				System.out.println(" about to retrieve new data.");
				String operator = path.substring(path.lastIndexOf("/") + 1, path.length());
				getDataAndWatch(operator);
			}else if(event.getType() == Event.EventType.NodeChildrenChanged) {
				getChildrenAndWatch();
			}
		}
	};
	
	public StatCollector(String zookeeperAddress) {
		operators = new CopyOnWriteArrayList<String>();
		try {
			zk = new ZooKeeper(zookeeperAddress, 1000000, dataRetrieverWatcher);
		} catch (IOException e) {
			e.printStackTrace();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void init() {
		getChildrenAndWatch();
		System.out.println("Operators received...");
		System.out.println("Press ENTER to stop execution...");
		try {
			System.in.read();
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			zk.close();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void getChildrenAndWatch() {
		zk.getChildren("/data", 
				true, 
				getChildrenCallback, 
				"/data".getBytes());
	}

	private Children2Callback getChildrenCallback = new Children2Callback() {
		@Override
		public void processResult(int rc, String path, Object ctx,
				List<String> children, Stat stat) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				System.out.println("getChildrenCallback(): CONNECTIONLOSS");
				getChildrenAndWatch();
				break;
			case NONODE:
				System.out.println("getChildrenCallback(): NONODE");
				break;
			case OK:
				/**
				 * children received
				 */
				System.out.println("getChildrenCallback() returned nodes: " + Arrays.toString(children.toArray()));
				List<String> childrenDifference = new ArrayList<String>(children);
				childrenDifference.removeAll(operators);
				operators.addAllAbsent(children);
				for(String child : childrenDifference)
					getDataAndWatch(child);
				break;
			default:
				System.out.println("getChildrenCallback(): Unexpected scenario: " + 
						KeeperException.create(Code.get(rc), path) );
				break;
			}
		}
	};
	
	public void getDataAndWatch(String operator) {
		zk.getData("/data/" + operator, 
				true, 
				getDataCallback, 
				operator);
	}

	private DataCallback getDataCallback = new DataCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, byte[] data,
				Stat stat) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				String operator = new String((String) ctx);
				System.out.println("getDataCallback(): CONNECTIONLOSS");
				getDataAndWatch(operator);
				break;
			case NONODE:
				System.out.println("getDataCallback(): NONODE");
				break;
			case OK:
				String readableData = new String(data);
				String operatorIdentifier = (String) ctx;
				System.out.println("getDataCallback(): Ok path " + operatorIdentifier + " has new data: " + readableData);
				if(readableData.contains("/data") == true)
					return;
				break;
			default:
				System.out.println("getDataCallback(): Unexpected scenario: " + 
						KeeperException.create(Code.get(rc), path) );
				break;
			}
		}
	};
}
