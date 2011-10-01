package se.kth.emdc.examples.kmeans;

public class Point {

	private int x;
	private int y;
	
	public Point(String[] points){
		if(points.length == 2){
			this.x = new Integer(points[0]);
			this.y = new Integer(points[1]);
		}
		
		this.x = -1;
		this.y = -1;
	}
	
	public Point(int x, int y) {
		super();
		this.x = x;
		this.y = y;
	}
	
	public int getX() {
		return x;
	}
	
	public void setX(int x) {
		this.x = x;
	}
	
	public int getY() {
		return y;
	}
	
	public void setY(int y) {
		this.y = y;
	}
	
	public int distanceTo(Point otherPoint){
		return (int)(Math.pow(this.x - otherPoint.x, 2) + Math.pow(this.y - otherPoint.y, 2));
	}

	@Override
	public String toString() {
		return x + "\t" + y;
	}
	
	
}
