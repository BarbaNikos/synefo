package gr.katsip.synefo.storm.operators.synefo_comp_ops;

import java.io.IOException;
import java.io.Serializable;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

public class dataCollector implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5782301300725371212L;
	
	private int byteCounter=0;
	
	private int bufferSize;
	
	private String opId;
	
	private int counter=0;
	
	byte[] buffer;
	
	private String zooIP;
	
	private Integer zooPort;
	
	private ZooKeeper zk = null;

	public dataCollector(String zooIP, Integer zooPort, int maxBuffer, String ID){
		bufferSize =maxBuffer;
		opId=ID;
		buffer = new byte[maxBuffer];
		this.zooIP = zooIP;
		this.zooPort = zooPort;
		try {
			zk = new ZooKeeper(zooIP + ":" + zooPort, 100000, null);
			if(zk.exists("/data/"+opId, false) != null){
				zk.delete("/data/"+opId, -1);
			}
			if(zk.exists("/data", false) != null){
				zk.delete("/data", -1);
			}
			zk.create("/data", buffer ,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			zk.create("/data/"+opId, buffer ,Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
	}

	public void addToBuffer(String tuple){
		tuple = tuple+";";
		byte[] newArray = tuple.getBytes();
		if(byteCounter+newArray.length > bufferSize){
			byteCounter=0;
			createChildNode();
			buffer = new byte[bufferSize];
			for(int i=0; i<newArray.length; i++){
				buffer[byteCounter]=newArray[i];
				byteCounter++;
			}
		}else{
			for(int i=0; i<newArray.length; i++){
				buffer[byteCounter]=newArray[i];
				byteCounter++;
			}
		}
	}



	public void createChildNode(){
		String path = "/data/"+opId+"/";
		String path2 = "/data/"+opId;
		System.out.println("Creating Child Node: "+new String(buffer));
		try {
			zk.setData(path2, buffer, counter++);
			zk.create(path, buffer ,Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
	}
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
