package se.kth.emdc.examples.olap;

import java.io.IOException;

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


public class OLAPQueryMRJob1 {

		public static class OLAPJoinMapper extends Mapper<Object, Text, Text, Text> {
			/*
			 * format of tuples in docs : URL | Content|\n
			 * example a tuple in docs: url_1|words words words|
			 * format of tuples in ranks: Rank | URL | Average Duration |\n
			 * example a tuple in ranks:  86|url_1|50
			*/
			Integer docs_KeyPosition = 0;
			Integer ranks_KeyPosition = 1;
			String Delimiter = "\\|";
			
			String[] keywords={ " editors ", " oscillations ", " convection " };
			Integer rank=50;
			
			protected void setup(Context context)
			{
			
			}
			
			protected void map(Object key, Text value, Context context) throws IOException ,InterruptedException {
				String[] items = value.toString().split(Delimiter);
				if(items.length == 2) // it's a docs-tuple
				{
					boolean ContainAllKeywords=true;
					for(String keyword: keywords)
					{
						if(!items[1].contains(keyword)) // check whether content contains keyword
						{
							ContainAllKeywords=false;
							
						}
					}
					if(ContainAllKeywords)
					{
						context.write(new Text(items[docs_KeyPosition]), value);
					}
				}
				else if(items.length == 3) // it's a ranks-tuple
				{
					if(Integer.parseInt(items[0]) > rank) // check whether the rank is bigger enough
					{
						context.write(new Text(items[ranks_KeyPosition]), value);
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
				boolean rank_tuple_found = false;
				boolean doc_tuple_found = false;
				
				Text rank_tuple = null;
				for(Text tuple : tuples)
				{
					if(tuple.toString().split(Delimiter).length == 3)
					{
						rank_tuple_found = true;
						rank_tuple = new Text(tuple);
					}
					else if(tuple.toString().split(Delimiter).length == 2)
					{
						doc_tuple_found = true;
					}
				}
				
				if(rank_tuple_found && doc_tuple_found)
				{					
					//emit the rank tuple
					context.write(NullWritable.get(), rank_tuple);
				}
			}
		}

		

		public static void main(String[] args) throws Exception {

			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			
			if (otherArgs.length != 3) {
				System.err.println("Usage: " + OLAPQueryMRJob1.class.getName() + 
								   "<docs_file> <ranks_file> <outputFolderPath>");
				
				System.err.println("<docs_file>: the path to the \"docs\" file in HDFS");
				System.err.println("<ranks_file>: the path to the \"ranks\" file in HDFS");
				System.err.println("<outputFolderPath>: output folder in HDFS, must be empty before running the job");
				System.exit(2);
			}
			
			Job job = new Job(conf, "OLAP Query Job1");
			job.setJarByClass(OLAPQueryMRJob1.class);
			job.setMapperClass(OLAPJoinMapper.class);
			job.setReducerClass(OLAPJoinReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}
