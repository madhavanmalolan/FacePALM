import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.restlet.Server;
import org.restlet.data.Protocol;

public class Main {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("Error Invalid Arguments!!");
			System.out
					.println("Usage : java WorkerNode hdfs://hdfs_uri PORT_NUMBER http://master_server:master_port");
			return;
		}
		String HDFS_URI = args[0];
		int PORT = Integer.parseInt(args[1]);
		String Master_URL = args[2];
		MyHandler handler = new MyHandler(HDFS_URI);
		new Server(Protocol.HTTP, PORT, handler).start();
		HttpClient httpclient = new DefaultHttpClient();
		HttpPost http_req= new HttpPost(Master_URL + "/register");
		http_req.setEntity(new StringEntity("{\"port\" : "+ PORT +" }"));
		httpclient.execute(http_req);
		System.out.println("Successfully Registered...");
	}

}
