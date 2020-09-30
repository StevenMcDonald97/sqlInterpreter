package main;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.io.FileInputStream;
import java.util.List;
import java.util.ArrayList;

/** binaryTupleReader is a class which implements TupleReader to read in 
 * tuples from a binary format input channel. It represents the data for a given page of binary data
 * @author Steven
 */

public class binaryTupleReader implements TupleReader{
//	track input file and channel for reader
	FileInputStream fin;
	FileChannel fcIn;
	String fpath;
//	create buffer to store bytes
	public ByteBuffer buf;
//	track metadata of tuples
	int tuple_size;
	int tuple_number;
	int tuples_seen;
//	store name of table being read
	String table;
//	store list of of column names for table
	List<String> schema;
//	flag to check if end of file is reached
	Boolean finished = false;
	
//	needs to have fields tracking tuple size and number for page
	
	
	public binaryTupleReader(String filePath, String table, ArrayList<String> s){
		try{
//			open file stream and instantiate bytebuffer
			this.fin = new FileInputStream(filePath);
			this.fcIn = fin.getChannel();
			this.fpath=filePath;
			this.buf = ByteBuffer.allocate( 4096 );
			this.table=table;
			this.schema=s;
			getNextPage();
						
		} catch (Exception e) {
			System.err.println("Exception occurred opening filechannel");
			e.printStackTrace();
		}
	};
	
	@Override
	/**readNextTuple() creates and returns the next tuple from the data in the binary reader
	 * @return [tuple] the tuple representing an entry in the database
	 */
	public tuple readNextTuple() {
		// TODO Auto-generated method stub
		
//		check if you have read all tuples on the page
		if (tuples_seen==tuple_number){
			getNextPage();
		}

//		check if there is still data in the buffer
		if (finished){
			return new tuple();
		}
		
		List<Integer> current_tuple = new ArrayList<Integer>();
		int i = 0;
		int position=0;
//		extract however many ints are needed to make a tuple


		while (i<tuple_size){
//			the regular getInt() method is finicky, so here we take the int at the next position
//			a position is the number of byte already seen in the file, so we want 
//			the number of tuples seen times the bytes in a tuple plus the first 8 bytes representing
//			tuple size and number of tuples 
			position=(tuple_size*tuples_seen+i)*4+8;
			current_tuple.add(buf.getInt(position));

			i++;
		}		


//		increment number of tuples taken from current page
		tuples_seen++;
		
		return (new tuple(current_tuple, schema, table));
	}

	
	@Override
	public void close() {
		// TODO Auto-generated method stub
		try{
			fin.close();
		} catch (Exception e){
			System.err.println("Exception occurred closing filechannel");
			e.printStackTrace();
		}
	}
	
	/**getNextPage() loads another page of binary data from memory into the buffer
	 * 
	 */
	public void getNextPage(){
		tuples_seen=0;

		try{
		    this.buf.clear();

		    int r = fcIn.read(buf);

		     if (r!=-1) {
//			     read in the size and number of tuples
			    tuple_size = buf.getInt(0);
			    tuple_number = buf.getInt(4);
//				System.out.println(tuple_size);
//				System.out.println(tuple_number);

			 } else {
				 finished = true;
				 close();
			 }

		     
		} catch (Exception e){
			System.err.println("Exception occurred reading byte tuple");
			e.printStackTrace();
		}
		
	}
	/** getSpecificTuple moves the pointer in the file reader to a specific tuple on a specific page 
	 * of the data file
	 * Important: This will move the pointer up to this tuple, and subsequent calls to readNextTuple
	 * will take the tuples after this
	 * @param pageNum is which page the tuple is on
	 * @tuplePos is which tuple on the page it is
	 */
	public void setFileReaderPosition(int pageNum, int tuplePos) {
//		tuples_seen=0;
    	Long page_position = Long.valueOf(pageNum*4096);

    	
		try{
//			load in the specified page
	    	fcIn.position(page_position);
	    	buf.clear();
	    	fcIn.read(buf);

		    int r = fcIn.read(buf);

		     if (r!=-1) {
//			     read in the size and number of tuples
			    tuple_size = buf.getInt(0);
			    tuple_number = buf.getInt(4);
			    
//			    move pointer in tuples up to desired tuple 
			    tuples_seen=tuplePos;
			    
			 } else {
				 System.out.println("tupleReader could not read specified page");
//				 finished = true;
//				 close();
			 }

		     
		} catch (Exception e){
			System.err.println("Exception occurred changing position in file reader");
			e.printStackTrace();
		}
	}
	
	/** getSpecificTuple returns a specific tuple on a specific page of the data file
	 * Important: This will move the pointer up to this tuple, and subsequent calls to readNextTuple
	 * will take the tuples after this
	 * @param pageNum is which page the tuple is on
	 * @tuplePos is which tuple on the page it is
	 * @return the tuple at the specified position
	 */
	public tuple getSpecificTuple(int pageNum, int tuplePos) {
//		tuples_seen=0;
		setFileReaderPosition(pageNum, tuplePos);
		return readNextTuple();
//    	Long page_position = Long.valueOf(pageNum*4096);
//
//    	
//		try{
////			load in the specified page
//	    	fcIn.position(page_position);
//	    	buf.clear();
//	    	fcIn.read(buf);
//
//		    int r = fcIn.read(buf);
//
//		     if (r!=-1) {
////			     read in the size and number of tuples
//			    tuple_size = buf.getInt(0);
//			    tuple_number = buf.getInt(4);
//			    
////			    move pointer in tuples up to desired tuple 
//			    tuples_seen=tuplePos;
//			    
//				List<Integer> current_tuple = new ArrayList<Integer>();
//				int i = 0;
//				int position;
////				extract however many ints are needed to make a tuple
//				while (i<tuple_size){
//					position=(tuple_size*tuples_seen+i)*4+8;
////					System.out.println(position);
//					current_tuple.add(buf.getInt(position));
//					i++;
//				}	
//				
//				tuples_seen++;
//				
//				return (new tuple(current_tuple, schema, table));
//
//			 } else {
//				 System.out.println("tupleReader could not read specified page");
////				 finished = true;
////				 close();
//			 }
//
//		     
//		} catch (Exception e){
//			System.err.println("Exception occurred reading byte tuple");
//			e.printStackTrace();
//		}
//		return new tuple();
	}

	/** Reset the current fileinput stream and channel and reload first page
	 * 
	 */
	public void reset() {

		try {
			// TODO Auto-generated method stub
			fin.close();
			fcIn.close();
			this.fin = new FileInputStream(fpath);
			this.fcIn = fin.getChannel();
			finished = false;
			getNextPage();

		} catch (Exception e) {
			System.err.println("Exception occurred opening filechannel");
			e.printStackTrace();
		}
	}

}
