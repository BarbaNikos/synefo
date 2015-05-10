package gr.katsip.cestorm.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class OperatorStatisticCollector {

	private ZooKeeper zk;

//	private Connection connection;
	
	private CopyOnWriteArrayList<String> operators;

	private Watcher dataRetrieverWatcher = new Watcher() {
		@Override
		public void process(WatchedEvent event) {
			String path = event.getPath();
			/**
			 * When you retrieve the path, call the getDataAndWatch() function to 
			 * retrieve data and set watch again
			 */
			if(event.getType() == Event.EventType.NodeDataChanged) {
				//Retrieve operator
				String operator = path.substring(path.lastIndexOf("/"), path.length());
				getDataAndWatch(operator);
			}else if(event.getType() == Event.EventType.NodeChildrenChanged) {
				//Retrieve new children
				getChildrenAndWatch();
			}
		}
	};

	public OperatorStatisticCollector(String zookeeperAddress, 
			String dbIP, String user, String password, String dbName) {
		operators = new CopyOnWriteArrayList<String>();
		try {
			zk = new ZooKeeper(zookeeperAddress, 1000000, dataRetrieverWatcher);
		} catch (IOException e) {
			e.printStackTrace();
		}
//		try {
//			connection = DriverManager.getConnection(dbIP, user, password);
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
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
	}

		public void getChildrenAndWatch() {
			zk.getChildren("/data", 
					dataRetrieverWatcher, 
					getChildrenCallback, 
					"/data/".getBytes());
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
					System.out.println("getChildrenCallback");
					List<String> childrenDifference = new ArrayList<String>(children);
					childrenDifference.retainAll(operators);
					operators.addAllAbsent(children);
					for(String child : childrenDifference) {
						getDataAndWatch(child);
					}
					System.out.println("getChildrenCallback(): OK call, received new children: " + Arrays.toString(childrenDifference.toArray()) + 
							", operators size: " + Arrays.toString(operators.toArray()));
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
				dataRetrieverWatcher, 
				getDataCallback, 
				operator);
	}

	private DataCallback getDataCallback = new DataCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, byte[] data,
				Stat stat) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				String operator = new String(data);
				System.out.println("getDataCallback(): CONNECTIONLOSS");
				getDataAndWatch(operator);
				break;
			case NONODE:
				System.out.println("getDataCallback(): NONODE");
				break;
			case OK:
				System.out.println("getDataCallback(): Successfully retrieved stats: " + 
						new String(data));
				/**
				 * TODO: Insert them to database
				 */
				break;
			default:
				System.out.println("getDataCallback(): Unexpected scenario: " + 
						KeeperException.create(Code.get(rc), path) );
				break;
			}
		}

	};

}
