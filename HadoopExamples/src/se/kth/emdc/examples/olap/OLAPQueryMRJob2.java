package se.kth.emdc.examples.olap;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class OLAPQueryMRJob2 {
	
		enum Environment {DATE};

		public static class OLAPJoinMapper extends Mapper<Object, Text, Text, Text> {
			/*
			 * format of tuples from step1 : URL|Content|Rank|AverageDuration|\n
			 * example a tuple from step1: url_1|words words words|86|50
			 * format of tuples in visits: IP|URL|DATE|1|2|3|4|5|6|\n
			 * example a tuple in visits:  133.33.250.203|url_910|2005-4-16|0.12|Mozilla Firefox 3.1|de|de|Nothing special|124|
			*/
			Integer step1_KeyPosition = 0;
			Integer visits_KeyPosition = 1;
			String Delimiter = "\\|";
			
			String date=null;
			
			protected void setup(Context context)
			{
				Configuration conf = context.getConfiguration();
				date = conf.get(Environment.DATE.toString());
			}
			
			protected void map(Object key, Text value, Context context) throws IOException ,InterruptedException {
				String[] items = value.toString().split(Delimiter);
				if(items.length == 4) // it's a step1-tuple
				{
					context.write(new Text(items[step1_KeyPosition]), value);
				}
				else if(items.length == 9) // it's a visits-tuple
				{
					if(items[2].equals(date)) // check whether the date is specified date
					{
						context.write(new Text(items[visits_KeyPosition]), value);
					}
				}
			};

		}

		public static class OLAPJoinReducer extends Reducer<Text,Text,NullWritable,Text> {
			String Delimiter = "\\|";
			
			protected void setup(Context context)
			{
			}
			
			public void reduce(Text key, Iterable<Text> tuples, Context context) throws IOException, InterruptedException {
				boolean step1_tuple_found = false;
				boolean visits_tuple_found = false;
				LinkedList<Text> outputTuples = new LinkedList<Text>();
				for(Text tuple : tuples)
				{
					if(tuple.toString().split(Delimiter).length == 4)
					{
						step1_tuple_found = true;
						outputTuples.add(tuple);
					}
					if(tuple.toString().split(Delimiter).length == 9)
						visits_tuple_found = true;
				}
				
				if(step1_tuple_found && (!visits_tuple_found))
				{
					for(Text tuple: outputTuples)
					{
						context.write(NullWritable.get(), tuple);
					}
				}
			}
		}

		

		public static void main(String[] args) throws Exception {

			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			
			if (otherArgs.length != 4) {
				System.err.println("Usage: " + OLAPQueryMRJob2.class.getName() + 
								   "<step1_files> <visits_file> <date> <outputFolderPath>");
				
				System.err.println("<step1_files>: a list of space separated paths to all the files generated from step 1 in HDFS,\n" +
						"			must be a string quoted");
				System.err.println("<visits_file>: the path to the \"visits\" file in HDFS");
				System.err.println("<date>: the date to be excluded, must be in the format of yyyy-(m)m-dd");
				System.err.println("<outputFolderPath>: output folder in HDFS, must be empty before running the job");
				System.exit(2);
			}
			
			Job job = new Job(conf, "OLAP Query Job2");
			job.setJarByClass(OLAPQueryMRJob2.class);
			job.setMapperClass(OLAPJoinMapper.class);
			job.setReducerClass(OLAPJoinReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			String[] inputPaths = otherArgs[0].split("\\s+");
			for(String path: inputPaths)
			{
				FileInputFormat.addInputPath(job, new Path(path));
			}
			
			FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
			job.getConfiguration().set(Environment.DATE.toString(), otherArgs[2]);
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}

