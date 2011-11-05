package se.kth.emdc.examples.kmeans;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class BasePoint {

	/**
	 * Retrieves points from a local file system
	 * @param pointsFileName
	 * @return the list of points
	 * @throws Exception if there is a problem while reading the file
	 */
	public static List<BasePoint> getPoints(String pointsFileName) throws Exception{

		BufferedReader pointReader = new BufferedReader(new FileReader(pointsFileName));

		LinkedList<BasePoint> pointsList = new LinkedList<BasePoint>();

		String line;
		while((line = pointReader.readLine()) != null){
			pointsList.add(new BasePoint(line.split("\\s+")));
		}

		return pointsList;
	}
	
	protected Long coords[];
	
	public BasePoint(){
		coords = null;
	}
	
	public BasePoint(String[] coords){
		this.coords = new Long[coords.length];
		
		for (int i = 0; i < coords.length; i++) {
			this.coords[i] = new Long(coords[i]);
		}
	}
	
	public BasePoint(Long... coords){
		this.coords = new Long[coords.length];
		
		for (int i = 0; i < coords.length; i++) {
			this.coords[i] = coords[i];
		}
	}
	
	public double euclidianDistanceTo(BasePoint otherPoint){

		if (otherPoint.coords.length != this.coords.length) {
			return -1.0;
		}

		double quadSum = 0.0;
		for (int i = 0; i < this.coords.length; i++) {
			quadSum += Math.pow((this.coords[i] - otherPoint.coords[i]), 2);
		}
		
		return Math.sqrt(quadSum);
	}

	public Long[] getCoords() {
		return coords;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(coords);
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BasePoint other = (BasePoint) obj;
		if (!Arrays.equals(coords, other.coords))
			return false;
		return true;
	}


	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer();
		for (Long coord : coords) {
			buf.append(coord);
			buf.append("\t");
		}
		return buf.toString().trim();
	}
}


