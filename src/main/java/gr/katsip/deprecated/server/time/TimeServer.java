package gr.katsip.deprecated.server.time;

import java.io.BufferedReader;
import java.io.IOException;
//import java.io.InputStream;
import java.io.InputStreamReader;
//import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
//import java.nio.ByteBuffer;

/**
 * @deprecated
 */
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
//				Long time = new Long(System.currentTimeMillis());
//				byte[] buffer = ByteBuffer.allocate(8).putLong(time).array();
//				OutputStream output = client.getOutputStream();
				PrintWriter out = new PrintWriter(client.getOutputStream(), true);
//				InputStream input = client.getInputStream();
				BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
//				output.write(buffer);
//				output.flush();
				out.println(Long.toString(System.currentTimeMillis()));
				out.flush();
//				buffer = new byte[8];
//				int i = input.read(buffer, 0, buffer.length);
				String response = in.readLine();
				System.out.println("synefo-time-server: response: " + response);
				in.close();
				out.close();
				client.close();
//				if(i == 8) {
//					System.out.println("synefo-time-server: received response size: " + i);
//					input.close();
//					output.close();
//					client.close();
//				}else {
//					System.out.println("synefo-time-server: received response with smaller size: " + i);
//					try {
//						Thread.sleep(10);
//					} catch (InterruptedException e) {
//						e.printStackTrace();
//					}
//					input.close();
//					output.close();
//					client.close();
//				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
