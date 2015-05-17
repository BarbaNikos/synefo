package gr.katsip.synefo.server.time;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class TimeServer implements Runnable {

	private ServerSocket server;

	public TimeServer(int port) {
		try {
			server = new ServerSocket(port);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void run() {
		System.out.println("synefo-Time-server initiated...");
		while(true) {
			try {
				Socket client = server.accept();
				System.out.println("synefo-time-server: accepted connection from: " + client.getInetAddress().getHostAddress());
				Long time = new Long(System.currentTimeMillis());
				byte[] buffer = ByteBuffer.allocate(8).putLong(time).array();
				OutputStream output = client.getOutputStream();
				output.write(buffer);
				output.flush();
				output.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
