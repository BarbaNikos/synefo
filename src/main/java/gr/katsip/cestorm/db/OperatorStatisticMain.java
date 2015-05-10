package gr.katsip.cestorm.db;

public class OperatorStatisticMain {

	public static void main(String[] args) {
		if(args.length < 5) {
			System.out.println("Expected arguments: <zookeeperIP:zookeeperPort> <db-IP:db-Port> <dbUser> <dbPass> <dbName>");
			System.exit(1);
		}else {
			OperatorStatisticCollector collector = new OperatorStatisticCollector(args[0], 
					args[1], args[2], args[3], args[4]);
			collector.init();
		}
	}

}
