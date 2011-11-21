package se.kth.emdc.examples.kmeans.stratosphere;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import se.kth.emdc.examples.kmeans.BasePoint;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactNull;

public class PactPoint extends BasePoint implements Key, Value{

	public PactPoint() {
		super();
	}
	
	public PactPoint(String[] coords){
		super(coords);
	}
	
	public PactPoint(Long... coords){
		super(coords);
	}
	
	@Override
	public void read(DataInput in) throws IOException {
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
	
	/**
	 * Compares this coordinate vector to another key.
	 * 
	 * @return -1 if the other key is not of type CoordVector. If the other
	 *         key is also a CoordVector but its length differs from this
	 *         coordinates vector, -1 is return if this coordinate vector is
	 *         smaller and 1 if it is larger. If both coordinate vectors
	 *         have the same length, the coordinates of both are compared.
	 *         If a coordinate of this coordinate vector is smaller than the
	 *         corresponding coordinate of the other vector -1 is returned
	 *         and 1 otherwise. If all coordinates are identical 0 is
	 *         returned.
	 */
	@Override
	public int compareTo(Key o) {

		// check if other key is also of type CoordVector
		if (!(o instanceof PactPoint)) {
			return -1;
		}
		// cast to CoordVector
		PactPoint oP = (PactPoint) o;

		// check if both coordinate vectors have identical lengths
		if (oP.coords.length > this.coords.length) {
			return -1;
		} else if (oP.coords.length < this.coords.length) {
			return 1;
		}

		// compare all coordinates
		for (int i = 0; i < this.coords.length; i++) {
			if (oP.coords[i] > this.coords[i]) {
				return -1;
			} else if (oP.coords[i] < this.coords[i]) {
				return 1;
			}
		}
		return 0;
	}
	
	public static class LineOutFormat extends TextOutputFormat<PactNull, PactPoint> {

		/**
		 * {@inheritDoc}
		 */

		@Override
		public byte[] writeLine(KeyValuePair<PactNull, PactPoint> pair) {
			// TODO Auto-generated method stub
			//String key = pair.getKey().toString();
			String value = pair.getValue().toString() + "\n";
			return value.getBytes();
		}

	}
	
	/**
	 * Converts a input string (a line) into a KeyValuePair with the string
	 * being the key and the value being a zero Integer.
	 */
	public static class LineInFormat extends TextInputFormat<PactPoint, PactNull> {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean readLine(KeyValuePair<PactPoint, PactNull> pair, byte[] line) {
			String lineStr = new String(line);
			
			CoordinatesSum onePoint = null;
			try {
				onePoint = new CoordinatesSum(lineStr.toString().split("\\s+"));
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			pair.setKey(onePoint);
			pair.setValue(new PactNull());
			return true;
		}

	}
}
