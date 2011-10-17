package se.kth.emdc.examples.kmeans;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class KmeansHadoopMR {

	
	public static List<Point> centers = null;

	public static class NearestCenterMapper extends Mapper<Object, Text, Text, Text> {
		public static String CENTERS_FILENAME = null;
		
		public static List<Point> getCenters() throws Exception{

			BufferedReader pointReader = new BufferedReader(new FileReader(CENTERS_FILENAME));

			LinkedList<Point> centersList = new LinkedList<Point>();

			String line;
			while((line = pointReader.readLine()) != null){
				centersList.add(new Point(line.split("\\s+")));
			}

			return centersList;
		}
		
		protected void setup(Mapper.Context context)
		{
			CENTERS_FILENAME  = context.getConfiguration().get("CENTERS_FILENAME");
		}
		
		protected void map(Object key, Text value, Context context) throws IOException ,InterruptedException {

			if(centers == null){
				try {
					centers = getCenters();
				} catch (Exception e) {
					System.err.println("Could not read centers file.");
					e.printStackTrace();
					return;
				}
			}

			Point point;
			try {
				point = new Point(value.toString().split("\\s+"));
			} catch (Exception e) {
				throw new IOException(e);
			}

			double minDist = Double.MAX_VALUE;
			Point closestCenter = null;

			for (Point center : centers) {
				double dist = point.sumOfSquares(center);
				if(dist < minDist){
					minDist = dist;
					closestCenter = center;
				}
			}

			context.write(new Text(closestCenter.toString()), new Text(point.toString()));
		};

	}

	public static class NearestCenterReducer extends Reducer<Text,Text,Text,Text> {
				
		public void reduce(Text key, Iterable<Text> points, Context context) throws IOException, InterruptedException {
			
			Point globalCentroid = new Point(0,0);
			long x=0, y=0;
			long length=0;
			
			for (Text point : points) {
				Point p;
				try {
					p = new Point(point.toString().split("\\s+"));
				} catch (Exception e) {
					throw new IOException(e);
				}
				x += p.getX();
				y += p.getY();
				++length;
			}
			
			globalCentroid.setX(x/length);
			globalCentroid.setY(y/length);
			
			context.write(key, new Text(globalCentroid.toString()));
		}
	}

	

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 3) {
			System.err.println("Usage: " + KmeansHadoopMR.class.getName() + " <pointsFile> <centerFile> <output>");
			System.err.println("<pointsFile>: points file in HDFS");
			System.err.println("<centerFile>: center file in the local file system");
			System.err.println("<output>: output folder in HDFS, must be empty before running the job");
			System.exit(2);
		}
		
		Job job = new Job(conf, "Kmeans Clustering Algorithm");
		job.setJarByClass(KmeansHadoopMR.class);
		job.setMapperClass(NearestCenterMapper.class);
		job.setCombinerClass(NearestCenterReducer.class);
		job.setReducerClass(NearestCenterReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		job.getConfiguration().set("CENTERS_FILENAME", otherArgs[1]);
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}