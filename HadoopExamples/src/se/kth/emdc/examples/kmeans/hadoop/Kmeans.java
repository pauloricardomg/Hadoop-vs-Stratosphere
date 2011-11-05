package se.kth.emdc.examples.kmeans.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import se.kth.emdc.examples.kmeans.BasePoint;

public class Kmeans {
	
	public static final String CENTERS_FILENAME = "CENTERS_FILENAME";
	
	public static class NearestCenterMapper extends Mapper<Object, Text, IntWritable, CoordinatesSum> {

		private List<BasePoint> centers = null;
		
		protected void setup(Context context) {
			String centersFileName = context.getConfiguration().get(CENTERS_FILENAME);
			
			try {
				centers  = BasePoint.getPoints(centersFileName);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			CoordinatesSum onePoint;
			try {
				onePoint = new CoordinatesSum(value.toString().split("\\s+"));
			} catch (Exception e) {
				throw new IOException(e);
			}

			double minDist = Double.MAX_VALUE;
			Integer closestCenterId = -1;

			for (int i = 0; i < centers.size(); i++) {
				BasePoint center = centers.get(i);
				double dist = onePoint.euclidianDistanceTo(center);
				if(dist < minDist){
					minDist = dist;
					closestCenterId = i;
				}
			}

			context.write(new IntWritable(closestCenterId), onePoint);
		};

	}

	public static class SumCoordinatesCombiner extends Reducer<IntWritable, CoordinatesSum, IntWritable, CoordinatesSum> {
		
		public void reduce(IntWritable key, Iterable<CoordinatesSum> points, Context context) throws IOException, InterruptedException {
			
			Long[] coordSums = null;
			long length=0;
			
			for (CoordinatesSum onePoint : points) {
				Long[] pointCoords = onePoint.getCoords();
				if(coordSums == null){
					coordSums = new Long[pointCoords.length];
					for (int i=0; i<pointCoords.length; i++) {
						coordSums[i] = pointCoords[i];
					}
				} else {
					for (int i = 0; i < coordSums.length; i++) {
						coordSums[i] += pointCoords[i];
					}					
				}
				length += onePoint.getCount();
			}
			
			context.write(key, new CoordinatesSum(coordSums, length));
		}
	}
	
	public static class RecomputeCenterReducer extends Reducer<IntWritable, CoordinatesSum, NullWritable, WritablePoint> {
				
		public void reduce(IntWritable key, Iterable<CoordinatesSum> points, Context context) throws IOException, InterruptedException {	
			Long[] coordSums = null;
			long length=0;
			
			for (CoordinatesSum summedPoints : points) {
				Long[] pointCoords = summedPoints.getCoords();
				if(coordSums == null){
					coordSums = new Long[pointCoords.length];
					for (int i=0; i<pointCoords.length; i++) {
						coordSums[i] = pointCoords[i];
					}
				} else {
					for (int i = 0; i < coordSums.length; i++) {
						coordSums[i] += pointCoords[i];
					}
				}
				length += summedPoints.getCount();
			}
			
			Long[] centCoords = new Long[coordSums.length];
			for (int i = 0; i < centCoords.length; i++) {
				centCoords[i] = coordSums[i]/length;
			}
			
			WritablePoint globalCentroid = new WritablePoint(centCoords);
			
			context.write(NullWritable.get(), globalCentroid);
		}
	}
		
	public static class WritablePoint extends BasePoint implements Writable{

		public WritablePoint() {
			super();
		}
		
		public WritablePoint(String[] coords){
			super(coords);
		}
		
		public WritablePoint(Long... coords){
			super(coords);
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			int length = in.readInt();
			coords = new Long[length];
			for (int i = 0; i < length; i++) {
				this.coords[i] = in.readLong();
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(coords.length);
			for (int i = 0; i < coords.length; i++) {
				out.writeLong(coords[i]);
			}
		}
	}

	public static class CoordinatesSum extends WritablePoint implements Writable{

		private Long count;
		
		public CoordinatesSum() {
			super();
		}
		
		public CoordinatesSum(String[] points){
			super(points);
			this.count = 1L;
		}
		
		public CoordinatesSum(Long[] points, Long count){
			super(points);
			this.count = count;
		}
		
		public Long getCount() {
			return count;
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			super.readFields(in);
			count = in.readLong();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			super.write(out);
			out.writeLong(count);
		}		
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 3) {
			System.err.println("Usage: " + Kmeans.class.getName() + " <pointsFile> <centerFile> <output>");
			System.err.println("<pointsFile>: points file in HDFS");
			System.err.println("<centerFile>: center file in the local file system");
			System.err.println("<output>: output folder in HDFS, must be empty before running the job");
			System.exit(2);
		}
		
		Job job = new Job(conf, "Kmeans Clustering Algorithm");
		job.setJarByClass(Kmeans.class);
		job.setMapperClass(NearestCenterMapper.class);
		job.setCombinerClass(SumCoordinatesCombiner.class);
		job.setReducerClass(RecomputeCenterReducer.class);
		
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(CoordinatesSum.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(WritablePoint.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		job.getConfiguration().set(CENTERS_FILENAME, otherArgs[1]);
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}