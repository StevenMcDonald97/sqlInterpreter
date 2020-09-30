package main;
/**
 * @author Steven
 *TupleWriter is an interface to write a tuple body to an output file
 */
import java.util.List;

public interface TupleWriter {
	/** Abstract method for writing a tuple to a datafile
	 * @param[tuple_body List<Integer>] is the list of integers in the tuple
	 * @param[tuples_size int] is the size of a tuple body
	 */
	abstract void writeNextTuple(List<Integer> tuple_body, int tuples_size);
	
//	/** Abstract method for closing a writer on a datafile
//	 */
//	abstract void close();
	
}
