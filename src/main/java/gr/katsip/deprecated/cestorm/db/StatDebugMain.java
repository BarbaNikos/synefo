package gr.katsip.deprecated.cestorm.db;

public class StatDebugMain {

	public static void main(String[] args) {
		if(args.length < 1) {
			System.err.println("Please provide zookeeper ip and port <zooIP:port>");
			System.exit(1);
		}else {
			StatCollector statCollector = new StatCollector(args[0]);
			statCollector.init();
		}
	}

}
