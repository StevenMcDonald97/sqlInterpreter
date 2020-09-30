package main;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author Steven
 *TupleReader is an interface for extracting tuples of data from data files
 */
public interface TupleReader {
	
	/** Abstract method for creating a tuple from a datafile
	 * @return the next tuple 
	 */
	abstract tuple readNextTuple();
	
	/** Abstract method for closing a reader on a datafile
	 */
	abstract void close();
	
	/** Abstract method for resetting a reader on a datafile
	 */	
	abstract void reset();
	
	/** getSpecificTuple returns a specific tuple on a specific page of the data file
	 * Important: This will move the pointer up to this tuple, and subsequent calls to readNextTuple
	 * will take the tuples after this
	 * @param pageNum is which page the tuple is on
	 * @tuplePos is which tuple on the page it is
	 * @return the tuple at the specified position
	 */
	abstract tuple getSpecificTuple(int pageNum, int tuplePos);
	
	/** getSpecificTuple moves the pointer in the file reader to a specific tuple on a specific page 
	 * of the data file
	 * Important: This will move the pointer up to this tuple, and subsequent calls to readNextTuple
	 * will take the tuples after this
	 * @param pageNum is which page the tuple is on
	 * @tuplePos is which tuple on the page it is
	 */
	abstract void setFileReaderPosition(int pageNum, int tuplePos);
}
