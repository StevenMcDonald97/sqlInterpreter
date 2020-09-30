package main;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.io.FileOutputStream;
import java.util.List;

public class binaryTupleWriter implements TupleWriter {

	ByteBuffer buf;
	
	/**Create an instance of binaryTupleWriter, writing to buffer b
	 * 
	 * @param b the buffer to write binary representation of tuples to
	 */
	public binaryTupleWriter(ByteBuffer b){
		try {
			buf = b;
			
		} catch (Exception e) {
			System.err.println("Exception occurred creating binary writer");
			e.printStackTrace();
		}
	}
	
	@Override
//	input is a buffer of ints already created 
	public void writeNextTuple(List<Integer> tuple_body, int tuple_ind) {

	    try{
	    	int i = 0;
	    	for (int entry : tuple_body){
//				write elements of tuple into the buffer (offset by 8 to leave space for 2 ints of metadata)
	    		buf.putInt((i+2)*4+tuple_ind, entry);
	    		i++; 
	    	}
		} catch (Exception e){
			System.err.println("Exception occurred writing to output buffer");
				e.printStackTrace();
			}
	}
	
	
}
