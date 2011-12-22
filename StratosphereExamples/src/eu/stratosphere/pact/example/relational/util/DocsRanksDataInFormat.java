package eu.stratosphere.pact.example.relational.util;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactString;

public class DocsRanksDataInFormat extends TextInputFormat<PactString, Tuple> {

	private final int maxColumns = 20;
	private final int delimiter = '|';
	private Tuple emptyTuple = new Tuple();

	@Override
	public boolean readLine(KeyValuePair<PactString, Tuple> pair, byte[] line) {
		int readPos = 0;

		// allocate the offsets array
		short[] offsets = new short[maxColumns];

		int col = 1; // the column we are in
		int countInWrapBuffer = 0; // the number of characters in the wrapping buffer

		int startPos = readPos;

		while (readPos < line.length) {
			if (line[readPos++] == delimiter) {
				offsets[col++] = (short) (countInWrapBuffer + readPos - startPos);
			}
		}

		Tuple value = new Tuple(line, offsets, col - 1);
		PactString key = null;

		/*
		 * format of tuples in docs : URL | Content|\n
		 * example a tuple in docs: url_1|words words words|
		 * format of tuples in ranks: Rank | URL | Average Duration |\n
		 * example a tuple in ranks:  86|url_1|50
		 */
		
		if(value.getNumberOfColumns() == 2) //Docs
		{
			key = new PactString(value.getStringValueAt(0));
			pair.setKey(key);
			pair.setValue(emptyTuple);
		}
		else if(value.getNumberOfColumns() == 3)//Ranks
		{		
			key = new PactString(value.getStringValueAt(1));
			pair.setKey(key);
			pair.setValue(value);
		}
		return true;
	}

}
