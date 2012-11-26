import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.Restlet;
import org.restlet.Server;
import org.restlet.data.MediaType;
import org.restlet.data.Protocol;
import org.restlet.data.Reference;

public class Main {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Error Invalid Arguments!!");
			System.out
					.println("Usage : java ClusterServer hdfs://hdfs_uri PORT_NUMBER ");
			return;
		}
		String HDFS_URI = args[0];
		int PORT = Integer.parseInt(args[1]);
		MyHandler handler = new MyHandler(HDFS_URI);
		new Server(Protocol.HTTP, PORT, handler).start();
	}

	private static class MyHandler extends Restlet {
		URI hdfs_uri;
		String dictionary_uri;
		String frequency_uri;
		String centroid_uri;
		HashMap<String, Integer> dictionary;
		HashMap<Integer, String> rev_dictionary;
		HashMap<Integer, Long> frequency;
		List<Cluster> centroids_list;
		static final int MAX_CONCURRENT_REQUESTS = 20;
		Semaphore sem = new Semaphore(MAX_CONCURRENT_REQUESTS);

		public MyHandler(String hdfs_uri) {
			try {
				this.hdfs_uri = new URI(hdfs_uri);
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
		}

		public void handle(Request request, Response response) {
			Reference ref = request.getResourceRef();
			String path = ref.getPath();
			String ip = request.getClientInfo().getAddress();
			if (path.compareTo("/query") == 0) {
				int cid = Integer.parseInt(request.getEntityAsText());
				boolean done = false;
				JSONArray ar = new JSONArray();
				for (Cluster c : centroids_list) {
					if (c.getId() == cid) {
						done = true;
						RandomAccessSparseVector v = (RandomAccessSparseVector) c
								.getCenter();
						for (Vector.Element e : v) {
							if (e.get() > 0.0001) {
								JSONObject ob = new JSONObject();
								try {
									ob.put("word",
											rev_dictionary.get(e.index()));
									ob.put("score", e.get());
								} catch (JSONException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}
								ar.put(ob);
							}
						}
						JSONObject ob = new JSONObject();
						try {
							ob.put("id", cid);
							ob.put("words", ar);

						} catch (JSONException e1) {
							e1.printStackTrace();
						}
						response.setEntity(ob.toString(),
								MediaType.APPLICATION_JSON);
						break;
					}
				}
				if (!done) {
					response.setEntity("No Such Clusterid",
							MediaType.APPLICATION_ALL);
				}

			} else if (path.compareTo("/update") == 0) {
				update(request, response);

			} else {
				response.setEntity("Error no such link", MediaType.TEXT_PLAIN);
			}

		}

		private void update(Request req, Response resp) {
			try {
				sem.acquire(MAX_CONCURRENT_REQUESTS);
				JSONObject ob = new JSONObject(req.getEntityAsText());
				System.out.println("Got Request : " + ob.toString());
				dictionary_uri = ob.getString("dictionary_file");
				frequency_uri = ob.getString("frequency_file");
				centroid_uri = ob.getString("centroid_file");
				load_dictionary();
				System.out.println(dictionary.size()
						+ " Elements loaded to Dictionary");
				load_frequency();
				System.out.println(frequency.size()
						+ " Elements loaded to Frequency File");
				load_centroids();
				System.out.println(centroids_list.size() + " Centroids loaded");
				String s = "";
				for (Cluster c : centroids_list) {
					s += c.getId() + " ";
				}
				System.out.println(s);
				resp.setEntity(s, MediaType.TEXT_PLAIN);

			} catch (Exception e) {
				resp.setEntity("Error! : " + e.toString(), MediaType.TEXT_PLAIN);
			} finally {
				sem.release(MAX_CONCURRENT_REQUESTS);
			}
		}

		private void load_dictionary() throws IOException,
				InstantiationException, IllegalAccessException {
			dictionary = new HashMap<String, Integer>();
			rev_dictionary = new HashMap<Integer, String>();
			Configuration config = new Configuration();
			FileSystem fs = FileSystem.get(hdfs_uri, config);
			Path path = new Path(dictionary_uri);
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path,
					config);
			Text key = (Text) reader.getKeyClass().newInstance();
			IntWritable value = (IntWritable) reader.getValueClass()
					.newInstance();
			while (reader.next(key, value)) {
				dictionary.put(key.toString(), value.get());
				rev_dictionary.put(value.get(), key.toString());
			}
			reader.close();
		}

		private void load_frequency() throws IOException,
				InstantiationException, IllegalAccessException {
			frequency = new HashMap<Integer, Long>();
			Configuration config = new Configuration();
			FileSystem fs = FileSystem.get(hdfs_uri, config);
			Path path = new Path(frequency_uri);
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path,
					config);
			IntWritable key = (IntWritable) reader.getKeyClass().newInstance();
			LongWritable value = (LongWritable) reader.getValueClass()
					.newInstance();
			while (reader.next(key, value)) {
				frequency.put(key.get(), value.get());
			}
			reader.close();
		}

		private void load_centroids() throws IOException,
				InstantiationException, IllegalAccessException {
			centroids_list = new ArrayList<Cluster>();
			Configuration config = new Configuration();
			FileSystem fs = FileSystem.get(hdfs_uri, config);
			Path path = new Path(centroid_uri);
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path,
					config);
			Text key = (Text) reader.getKeyClass().newInstance();
			ClusterWritable value = (ClusterWritable) reader.getValueClass()
					.newInstance();
			HashMap<Integer, String> m = new HashMap<Integer, String>();
			for (String s : dictionary.keySet()) {
				m.put(dictionary.get(s), s);
			}
			while (reader.next(key, value)) {
				Cluster c = value.getValue();
				long id = c.getId();
				centroids_list.add(c);
				System.out.println("Centroid " + id);
				Vector v = c.getCenter();
				for (Vector.Element e : v) {
					if (e.get() > 0.0001)
						System.out.println(m.get(e.index()) + " - " + e.get());
				}

			}
			reader.close();
		}
	}

}
