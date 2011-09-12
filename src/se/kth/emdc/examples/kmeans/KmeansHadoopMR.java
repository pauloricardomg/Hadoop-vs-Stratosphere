package se.kth.emdc.examples.kmeans;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class KmeansHadoopMR {

	public static final String CENTERS_FILENAME = "/home/paulo/blabla";
	public static List<Point> centers = null;
	
	public static class NearestCenterMapper extends Mapper<Object, Text, Text, Text> {
		
		protected void map(Object key, Text value, Context context) throws IOException ,InterruptedException {
			
			if(centers == null){
				try {
					centers = getCenters();
				} catch (Exception e) {
					System.err.println("Could not read centers file. Empty centers list.");
					e.printStackTrace();
					centers = new LinkedList<Point>();
				}
			}
			
			Point point = new Point(value.toString().split(" +"));
			
			int minDist = Integer.MAX_VALUE;
			Point closestCenter = null;
			
			for (Point center : centers) {
				int dist = point.distanceTo(center);
				if(dist < minDist){
					minDist = dist;
					closestCenter = center;
				}
			}
			
			context.write(new Text(closestCenter.toString()), new Text(point.toString()));
		};
		
	}
	
	
	public static List<Point> getCenters() throws Exception{
		
		BufferedReader pointReader = new BufferedReader(new FileReader(CENTERS_FILENAME));
		
		LinkedList<Point> centersList = new LinkedList<Point>();
		
		String line;
		while((line = pointReader.readLine()) != null){
			centersList.add(new Point(line.split(" +")));
		}
		
		return centersList;
	}
}
