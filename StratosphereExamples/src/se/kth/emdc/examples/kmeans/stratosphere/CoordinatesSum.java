package se.kth.emdc.examples.kmeans.stratosphere;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactNull;

public class CoordinatesSum extends PactPoint implements Value{

	private Long count;
	
	public CoordinatesSum() {
		super();
	}
	
	public CoordinatesSum(String[] points){
		super(points);
		this.count = 1L;
	}
	
	public CoordinatesSum(Long[] points){
		this(points, 1L);
	}
	
	public CoordinatesSum(Long[] points, Long count){
		super(points);
		this.count = count;
	}
	
	public Long getCount() {
		return count;
	}
	
	@Override
	public void read(DataInput in) throws IOException {
		super.read(in);
		count = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeLong(count);
	}		
	
	@Override
	public int compareTo(Key o) {
		if (!(o instanceof CoordinatesSum)) {
			return -1;
		}

		CoordinatesSum oP = (CoordinatesSum) o;
		
		int superCompareTo = super.compareTo(o);
				
		return superCompareTo == 0? count.compareTo(oP.count) : superCompareTo;
	};
	
	/**
	 * Converts a input string (a line) into a KeyValuePair with the string
	 * being the key and the value being a zero Integer.
	 */
	public static class LineInFormat extends TextInputFormat<PactNull, CoordinatesSum> {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean readLine(KeyValuePair<PactNull, CoordinatesSum> pair, byte[] line) {
			String lineStr = new String(line);
			
			CoordinatesSum onePoint = null;
			try {
				onePoint = new CoordinatesSum(lineStr.toString().split("\\s+"));
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			pair.setKey(new PactNull());
			pair.setValue(onePoint);
			return true;
		}

	}
}
