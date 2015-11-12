package gr.katsip.synefo.storm.topology;

/**
 * Created by nick on 10/13/15.
 */
public class JoinSubmitter {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Arguments: topology-driver.conf");
            System.exit(1);
        }else {
            TopologyDriver driver = new TopologyDriver();
            driver.configure(args[0]);
            try {
                driver.submit();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
