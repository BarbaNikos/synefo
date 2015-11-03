package gr.katsip.synefo.storm.topology;

/**
 * Created by nick on 11/3/15.
 */
public class ScaleDebugJoinSubmitter {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Arguments: topology-driver.conf");
            System.exit(1);
        }else {
            ScaleDebugTopologyDriver driver = new ScaleDebugTopologyDriver();
            driver.configure(args[0]);
            driver.submit(500);
        }
    }

}
