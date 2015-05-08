package gr.katsip.synefo.storm.operators.synefo_comp_ops;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

public class dataCollector implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5782301300725371212L;
	
	private int byteCounter = 0;
	
	private int bufferSize;
	
	private String opId;
	
	byte[] buffer;
	
	private String zooIP;
	
	private Integer zooPort;
	
	private ZooKeeper zk = null;

	public dataCollector(String zooIP, Integer zooPort, int maxBuffer, String ID) {
		bufferSize = maxBuffer;
		opId = ID;
		buffer = new byte[maxBuffer];
		this.zooIP = zooIP;
		this.zooPort = zooPort;
		try {
			zk = new ZooKeeper(this.zooIP + ":" + this.zooPort, 100000, null);
			if(zk.exists("/data/" + opId, false) != null ) {
				zk.delete("/data/" + opId, -1);
			}
			zk.create("/data/"+ opId, buffer ,Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
	}

	public void addToBuffer(String tuple) {
		tuple = tuple + ";";
		byte[] newArray = tuple.getBytes();
		if(byteCounter + newArray.length > bufferSize) {
			byteCounter=0;
			byte[] statBuffer = Arrays.copyOf(buffer, buffer.length);
			createChildNode(statBuffer);
			buffer = new byte[bufferSize];
			for(int i=0; i<newArray.length; i++) {
				buffer[byteCounter]=newArray[i];
				byteCounter++;
			}
		}else {
			for(int i=0; i<newArray.length; i++) {
				buffer[byteCounter]=newArray[i];
				byteCounter++;
			}
		}
	}



	public void createChildNode(byte[] statBuffer) {
		String newChildPath = "/data/" + opId + "/";
//		String nodePath = "/data/" + opId;
//TODO: Do we need the data twice?? Both in the /data/opId node and in the /data/opId/n node??
		zk.create(newChildPath, statBuffer, Ids.OPEN_ACL_UNSAFE, 
				CreateMode.PERSISTENT_SEQUENTIAL, createChildNodeCallback, statBuffer);
	}
	
	private StringCallback createChildNodeCallback = new StringCallback() {
		@Override
		public void processResult(int rc, String path, Object ctx,
				String name) {
			switch(Code.get(rc)) {
			case CONNECTIONLOSS:
				byte[] statBuffer = (byte[]) ctx;
				createChildNode(statBuffer);
				System.err.println("dataCollector.createChildNodeCallback(): CONNECTIONLOSS for: " + path + ". Attempting again.");
				break;
			case NONODE:
				System.err.println("dataCollector.createChildNodeCallback(): NONODE with name: " + path);
				break;
			case NODEEXISTS:
				System.err.println("dataCollector.createChildNodeCallback(): NODEEXISTS with name: " + path);
				break;
			case OK:
				System.err.println("dataCollector.createChildNodeCallback(): OK buffer written successfully.");
				break;
			default:
				System.err.println("dataCollector.createChildNodeCallback(): Unexpected scenario: " + 
						KeeperException.create(Code.get(rc), path));
				break;
			}
		}
		
	};
}
//public void startStats(int id){
//String path = "/crypto/"+id+"/plain_percent";
//ZkConnector zkc = new ZkConnector();
//try {
//	zkc.connect("localhost");
//	ZooKeeper zk = zkc.getZooKeeper();
//	zk.create(path,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
//	zk.create(path5,null,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
//} catch (IOException e) {
//	// TODO Auto-generated catch block
//	e.printStackTrace();
//} catch (InterruptedException e) {
//	// TODO Auto-generated catch block
//	e.printStackTrace();
//} catch (KeeperException e) {
//	// TODO Auto-generated catch block
//	e.printStackTrace();
//}
//
//}
//
//public void sendStats(int id, HashMap<String, Integer> encryptionMap){
//String path = "/crypto/"+id+"/plain_percent";
//String path2 = "/crypto/"+id+"/RND_percent";
//String path3 = "/crypto/"+id+"/DET_percent";
//String path4 = "/crypto/"+id+"/OPE_percent";
//String path5 = "/crypto/"+id+"/HOM_percent";
//
//ZkConnector zkc = new ZkConnector();
//try {
//	zkc.connect("localhost");
//	ZooKeeper zk = zkc.getZooKeeper();
//	zk.setData(path, encryptionMap.get("PLN").toString().getBytes(), 0);
//	zk.setData(path2, encryptionMap.get("RND").toString().getBytes(), 0);
//	zk.setData(path3, encryptionMap.get("DET").toString().getBytes(), 0);
//	zk.setData(path4, encryptionMap.get("OPE").toString().getBytes(), 0);
//	zk.setData(path5, encryptionMap.get("RND").toString().getBytes(), 0);
//} catch (IOException e) {
//	// TODO Auto-generated catch block
//	e.printStackTrace();
//} catch (InterruptedException e) {
//	// TODO Auto-generated catch block
//	e.printStackTrace();
//} catch (KeeperException e) {
//	// TODO Auto-generated catch block
//	e.printStackTrace();
//}
//}
