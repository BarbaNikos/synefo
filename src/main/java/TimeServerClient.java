import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Date;


public class TimeServerClient {

	public static void main(String[] args) throws UnknownHostException, IOException {
		for(int i = 0; i < 100000; i++) {
			Socket client = new Socket("127.0.0.1", 5556);
			OutputStream output = client.getOutputStream();
			InputStream input = client.getInputStream();
			byte[] buffer = new byte[8];
			if(input.read(buffer) == 8) {
				ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
				Date d = new Date(byteBuffer.getLong());
				System.out.println(d);
			}else {
				System.out.println("ERROR");
			}
			output.close();
			//		in.close();
			input.close();
			client.close();
		}
	}

}
