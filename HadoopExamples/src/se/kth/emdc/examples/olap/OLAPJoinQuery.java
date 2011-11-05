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
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;


public class OLAPJoinQuery {	

	public static class OLAPJoinMapper extends Mapper<Object, Text, Text, Text> {
		
		
		protected void setup(Context context)
		{
			
		}
		
		protected void map(Object key, Text value, Context context) throws IOException ,InterruptedException {



			//context.write(new Text(closestCenter.toString()), new Text(point.toString()));
		};

	}

	public static class OLAPJoinReducer extends Reducer<Text,Text,Text,Text> {
				
		public void reduce(Text key, Iterable<Text> points, Context context) throws IOException, InterruptedException {
			
		}
	}

	

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 9) {
			System.err.println("Usage: " + OLAPJoinQuery.class.getName() + 
							   "<table1_Prefix> <table1_NumberOfPartions> <table1_KeyPosition> <table1_delimiter> <table2_Prefix> <table2_NumberOfPartitions> <table2_KeyPosition> <table2_delimiter> <outputFolderPath>");
			
			System.err.println("<table?_Prefix>: the prefix of the name of the relational table file in HDFS");
			System.err.println("<table?_NumberOfPartions>: number of partitions of the table");
			System.err.println("<table?_keyPosition>: position of the key");
			System.err.println("<table?_delimiter>: the delimiter that separates the columns in the table");
			System.err.println("<outputFolderPath>: output folder in HDFS, must be empty before running the job");
			System.exit(2);
		}
		
		Job job = new Job(conf, "OLAP Join Query");
		job.setJarByClass(OLAPJoinQuery.class);
		job.setMapperClass(OLAPJoinMapper.class);
		job.setReducerClass(OLAPJoinReducer.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		Integer table1_partitions = Integer.parseInt(otherArgs[1]);
		for(int i=0; i < table1_partitions; ++i)
		{
			FileInputFormat.addInputPath(job, new Path(otherArgs[0] + "_" + i));
		}
		
		job.getConfiguration().set("TABLE1_KEY_POSITION", otherArgs[0]);
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}