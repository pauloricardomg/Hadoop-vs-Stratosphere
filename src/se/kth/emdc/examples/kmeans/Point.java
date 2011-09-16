package se.kth.emdc.examples.kmeans;

import java.util.Arrays;

public class Point {

	private long x;
	private long y;
	
	public Point(String[] points) throws Exception{
		if(points.length != 2){
			throw new Exception("Cannot initialize points object. Given points: " + Arrays.toString(points));
		}
		
		this.x = new Long(points[0]);
		this.y = new Long(points[1]);
	}
	
	public Point(long x, long y) {
		super();
		this.x = x;
		this.y = y;
	}
	
	public long getX() {
		return x;
	}
	
	public void setX(long x) {
		this.x = x;
	}
	
	public long getY() {
		return y;
	}
	
	public void setY(long y) {
		this.y = y;
	}
	
	public long distanceTo(Point otherPoint){
		return (long)(Math.pow(this.x - otherPoint.x, 2) + Math.pow(this.y - otherPoint.y, 2));
	}

	@Override
	public String toString() {
		return x + "\t" + y;
	}
	
	
}
