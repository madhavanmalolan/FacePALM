
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.iterator.ClusterWritable;

public class GetClusters {

	
	public static class CentroidMapper extends Mapper<IntWritable, ClusterWritable, LongWritable, Text> {
		public void map(IntWritable key, ClusterWritable value, Context context ){

			
			Cluster c = value.getValue();

		    
		  		    
			Text textOfVector = new Text("");
		    LongWritable l = new LongWritable(c.getId());
		    try{
		        context.write(l, textOfVector);
		    }
		    catch (Exception e){
		    	System.out.println("Woopsie!");
		    }
		    
		}
	}
	
	
	public static void main(String[] args) throws IOException,
			InstantiationException, IllegalAccessException {
		Configuration config = new Configuration();
		Job job = new Job(config, "Sequence to JSON");
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		
		job.setJarByClass(GetClusters.class);
		job.setMapperClass(CentroidMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		try {
			System.exit(job.waitForCompletion(true)?0:1);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		

}
