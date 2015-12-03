package gr.katsip.synefo.storm.topology;

/**
 * Created by katsip on 12/3/2015.
 */
public class QueryFiveSubmitter {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Arguments: topology-driver.conf");
            System.exit(1);
        }else {
            QueryFiveDriver driver = new QueryFiveDriver();
            driver.configure(args[0]);
            driver.submit();
        }
    }
}
