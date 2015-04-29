import gr.katsp.synefo.server.time.TimeServer;


public class TimeServerTest {

	public static void main(String[] args) throws InterruptedException {
		(new Thread(new TimeServer(5556))).start();
		Thread.sleep(100000);
	}

}
