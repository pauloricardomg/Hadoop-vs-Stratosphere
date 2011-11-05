package se.kth.emdc.examples.olap;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class PartitionTableByKey {	

	public static class PartitionTableByKeyMapper extends Mapper<Object, Text, Text, Text> {
		Integer keyPosition;
		String delimiter;
		
		protected void setup(Context context)
		{
			keyPosition  = Integer.parseInt(context.getConfiguration().get("KEY_POSITION"));
			delimiter = context.getConfiguration().get("DELIMITER");
		}
		
		protected void map(Object key, Text value, Context context) throws IOException ,InterruptedException {
			String tuple = value.toString();
			String[] items = tuple.split(delimiter);
			context.write(new Text(items[keyPosition]), value);
		};

	}

	public static class PartitionTableByKeyReducer extends Reducer<Text,Text,Text,Text> {
				
		public void reduce(Text key, Iterable<Text> tuples, Context context) throws IOException, InterruptedException {
			for (Text tuple : tuples) {
				context.write(key, tuple);
			}			
		}
	}	

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 4) {
			System.err.println("Usage: " + PartitionTableByKey.class.getName() + "<inputFilePath> <outputFolderPath> <keyPosition> <delimiter> ");
			
			System.err.println("<inputFilePath>: the relational table file in HDFS");
			System.err.println("<outputFolderPath>: output folder in HDFS, must be empty before running the job");
			System.err.println("<keyPosition>: position of the key");
			System.err.println("<delimiter>: the delimiter that separates the columns in the table");
			
			System.exit(2);
		}
		
		Job job = new Job(conf, "Partition data in a table by a certain key");
		job.setJarByClass(PartitionTableByKey.class);
		job.setMapperClass(PartitionTableByKeyMapper.class);
		job.setReducerClass(PartitionTableByKeyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		job.getConfiguration().set("KEY_POSITION", otherArgs[2]);
		job.getConfiguration().set("DELIMITER", otherArgs[2]);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
