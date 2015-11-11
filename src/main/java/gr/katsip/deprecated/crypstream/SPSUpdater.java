package gr.katsip.deprecated.crypstream;

import java.io.IOException;
import java.io.Serializable;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SPSUpdater implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1027397682338333911L;

	private String zooIP;

	private Integer zooPort;

	private ZooKeeper zk = null;
	
	Logger logger = LoggerFactory.getLogger(DataCollector.class);

	private Watcher dataCollectorWatcher = new Watcher() {
		@Override
		public void process(WatchedEvent event) {

		}
	};

	public SPSUpdater(String zooIP, Integer zooPort) {
		this.zooIP = zooIP;
		this.zooPort = zooPort;
		try {
			zk = new ZooKeeper(this.zooIP + ":" + this.zooPort, 100000, dataCollectorWatcher);
			if(zk.exists("/SPS", false) != null ) {
				zk.delete("/SPS", -1);
				System.out.println("znode /sps deleted");
			}
			zk.create("/SPS", (new String("/SPS")).getBytes(), 
					Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			System.out.println("znode /sps created");
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
	}
	
	public void createChildNode(byte[] statBuffer) {
		String nodePath = "/SPS";
		zk.setData(nodePath, statBuffer, -1, setSPSCallback, statBuffer);
		System.out.println("znode created for SPS: " + new String(statBuffer));
	}
	
	StatCallback setSPSCallback = new StatCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx, Stat stat) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				byte[] statBuffer = (byte[]) ctx;
				createChildNode(statBuffer);
				logger.error("SPSUpdater.getDataCallback(): CONNECTIONLOSS for: " + path + ". Attempting again.");
//				System.out.println("SPSUpdater.getDataCallback(): CONNECTIONLOSS for: " + path + ". Attempting again.");
				break;
			case NONODE:
				logger.error("SPSUpdater.getDataCallback(): NONODE with name: " + path);
//				System.out.println("SPSUpdater.getDataCallback(): NONODE with name: " + path);
				break;
			case OK:
//				logger.info("SPSUpdater.getDataCallback(): OK data have been set successfully.");
//				System.out.println("SPSUpdater.getDataCallback(): OK data have been set successfully.");
				break;
			default:
				logger.error("SPSUpdater.getDataCallback(): Unexpected scenario: " + 
						KeeperException.create(Code.get(rc), path));
//				System.out.println("SPSUpdater.getDataCallback(): Unexpected scenario: " + 
//						KeeperException.create(Code.get(rc), path));
				break;
			}
		}
	};
}
