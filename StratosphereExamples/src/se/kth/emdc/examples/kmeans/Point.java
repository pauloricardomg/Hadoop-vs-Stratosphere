package se.kth.emdc.examples.kmeans;


public class Point {

	private long x;
	private long y;
	
	public Point(String[] points){
		if(points.length != 2){
			System.err.println("Bad Initialization!");
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
	
	public double sumOfSquares(Point otherPoint){
		return (Math.pow(this.x - otherPoint.x, 2) + Math.pow(this.y - otherPoint.y, 2));
	}

	@Override
	public String toString() {
		return x + "\t" + y;
	}
	
	
}