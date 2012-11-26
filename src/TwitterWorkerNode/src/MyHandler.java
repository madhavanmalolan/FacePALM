import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.cassandra.thrift.Cassandra.system_add_column_family_args;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.vectorizer.TFIDF;
import org.json.JSONArray;
import org.json.JSONObject;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.Restlet;
import org.restlet.data.MediaType;
import org.restlet.data.Reference;

public class MyHandler extends Restlet {
	URI hdfs_uri;
	String dictionary_uri;
	String frequency_uri;
	String centroid_uri;
	HashSet<Long> centroids;
	HashMap<String, Integer> dictionary;
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

	@Override
	public void handle(Request request, Response response) {
		Reference ref = request.getResourceRef();
		String path = ref.getPath();
		String ip = request.getClientInfo().getAddress();
		System.out.println("Request " + path + " from " + ip);
		if (path.compareTo("/test") == 0) {
			String s = request.getEntityAsText();
			if (s == null)
				s = "NoInp";
			response.setEntity(s, MediaType.TEXT_PLAIN);
		} else if (path.compareTo("/cluster_tweet") == 0) {
			classify(request, response);
		} else if (path.compareTo("/update_clusters") == 0) {
			update(request, response);
		} else {
			response.setEntity("Error, Not Found", MediaType.TEXT_PLAIN);
		}
	}

	RandomAccessSparseVector convert_to_vector(String tweet) {
		String[] tokens = tweet.split("\\s|\\p{Punct}");
		RandomAccessSparseVector vec = new RandomAccessSparseVector(dictionary.size());
		TFIDF tfidf = new TFIDF();
		HashMap<Integer, Integer> m = new HashMap<Integer, Integer>();
		for (String token : tokens) {
			if (token == null) {
				System.out.println("HOLY SHIT");
			}
			if (dictionary.containsKey(token)) {
				int id = dictionary.get(token);
				if (m.containsKey(id)) {
					m.put(id, m.get(id) + 1);
				} else {
					m.put(id, 1);
				}
			}
		}
		for (int key : m.keySet()) {
			vec.set(key, tfidf.calculate(m.get(key), frequency.get(key)
					.intValue(), tokens.length, 2000000));
		}
		return vec;
	}

	private void classify(Request req, Response resp) {
		try {
			sem.acquire();
			JSONObject ob = new JSONObject(req.getEntityAsText());
			long id = ob.getLong("id");
			String tweet = ob.getString("tweet");
			CosineDistanceMeasure dist_measure = new CosineDistanceMeasure();
			RandomAccessSparseVector vec = convert_to_vector(tweet);
			double min_score = Double.POSITIVE_INFINITY;
			int min_id = 0;
			for (Cluster c : centroids_list) {
				double score = dist_measure.distance(vec, c.getCenter());
				if (score < min_score) {
					min_score = score;
					min_id = c.getId();
				}
			}
			JSONObject ret = new JSONObject();
			ret.put("cluster_id", Integer.toString(min_id));
			ret.put("score", Double.toString(min_score));
			resp.setEntity(ret.toString(), MediaType.APPLICATION_JSON);
			JSONArray top_words;
			class word {
				String w;
				double score;
			};
			List<word> l = new ArrayList<word>();
			class wordComparator implements Comparator<word> {
				public int compare(word a, word b) {
					if (a.score < b.score)
						return -1;
					else if(a.score == b.score)
						return 0;
					else
						return 1;
				}
			}
			System.out.println("Replying : " + ret.toString());
		} catch (Exception e) {
			resp.setEntity("Error! : " + e.toString(), MediaType.TEXT_PLAIN);
			e.printStackTrace();
		} finally {
			sem.release();
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
			String[] centroid_list = ob.getString("centroids").split(",");
			centroids = new HashSet<Long>();
			for (int i = 0; i < centroid_list.length; i++) {
				centroids.add(Long.parseLong(centroid_list[i]));
			}
			load_dictionary();
			System.out.println(dictionary.size()
					+ " Elements loaded to Dictionary");
			load_frequency();
			System.out.println(frequency.size()
					+ " Elements loaded to Frequency File");
			load_centroids();
			System.out.println(centroids_list.size() + " Centroids loaded");
			resp.setEntity("Cluster Loaded", MediaType.TEXT_PLAIN);

		} catch (Exception e) {
			resp.setEntity("Error! : " + e.toString(), MediaType.TEXT_PLAIN);
		} finally {
			sem.release(MAX_CONCURRENT_REQUESTS);
		}
	}

	private void load_dictionary() throws IOException, InstantiationException,
			IllegalAccessException {
		dictionary = new HashMap<String, Integer>();
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(hdfs_uri, config);
		Path path = new Path(dictionary_uri);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, config);
		Text key = (Text) reader.getKeyClass().newInstance();
		IntWritable value = (IntWritable) reader.getValueClass().newInstance();
		while (reader.next(key, value)) {
			dictionary.put(key.toString(), value.get());
		}
		reader.close();
	}

	private void load_frequency() throws IOException, InstantiationException,
			IllegalAccessException {
		frequency = new HashMap<Integer, Long>();
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(hdfs_uri, config);
		Path path = new Path(frequency_uri);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, config);
		IntWritable key = (IntWritable) reader.getKeyClass().newInstance();
		LongWritable value = (LongWritable) reader.getValueClass()
				.newInstance();
		while (reader.next(key, value)) {
			frequency.put(key.get(), value.get());
		}
		reader.close();
	}

	private void load_centroids() throws IOException, InstantiationException,
			IllegalAccessException {
		centroids_list = new ArrayList<Cluster>();
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(hdfs_uri, config);
		Path path = new Path(centroid_uri);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, config);
		Text key = (Text) reader.getKeyClass().newInstance();
		ClusterWritable value = (ClusterWritable) reader.getValueClass()
				.newInstance();
		HashMap<Integer, String> m = new HashMap<Integer, String>();
		for(String s : dictionary.keySet()) {
			m.put(dictionary.get(s), s);
		}
		while (reader.next(key, value)) {
			Cluster c = value.getValue();
			long id = c.getId();
			if (centroids.contains(id)) {
				centroids_list.add(c);
				/*
				System.out.println("Centroid " + id);
				Vector v = c.getCenter();
				for(Vector.Element e : v) {
					if(e.get() > 0.0001)
					System.out.println(m.get(e.index()) + " - " + e.get());
				}
				*/
			}
		}
		reader.close();
	}
}
