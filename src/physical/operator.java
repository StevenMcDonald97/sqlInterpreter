package physical;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

import main.Interpreter;
import main.TupleWriter;
import main.binaryTupleWriter;
import main.tuple;

public abstract class operator {

	private String outputdir = Interpreter.getOutputDir();
	
	/**	getNextTuple() is a method to return the next tuple in the operator's output
	 */
	public abstract tuple getNextTuple();
		
	/**	reset() is a method to return to reset the operator's state and return to the first tuple in the operator's output
	 */
	public abstract void reset();
	
	/**	reset(int i) is a method to return to reset the operator's state to the ith tuple in the operator's output
	 */
	public abstract void reset(int i);
	
	/**	getSchema() returns the schema an oeprator uses
	 */
	public abstract List<String> getSchema();
	
	/**	dump() repeatedly calls getNextTuple and write the tuples to an output stream until an empty tuple is returned 
	 */
	public void dump(int queryNumber){
		tuple t;
//		while ((t=this.getNextTuple()).getBody() != null){
//			System.out.println(t.getBody());
//
//		}
		
//		UNCOMMENT THE BELOW CODE AND COMMENT OUT THE ABOVE CODE TO SWITCH TO WRTIE TO OUTPUT DIRECTORY

//		outputdir = Interpreter.getOutputDir();
		outputdir = "./output";

//		!!! change to dump to a specified printstream
		try{
			FileWriter fstream = new FileWriter(outputdir+"/query"+queryNumber);
			BufferedWriter out = new BufferedWriter(fstream);

			while ((t=this.getNextTuple()).getBody() != null){

				int len = t.getBody().size();
				for (int i=0; i<len-1; i++ ){
					out.write(t.getBody().get(i)+",");	 
				}
				
				out.write(t.getBody().get(len-1)+"");	 
				out.write("\n");
			}

			out.close();

		}catch (Exception e){
			System.err.println("Exception occurred writing to file");
			e.printStackTrace();		
		}

		
	}
	

//	/**	dump() repeatedly calls getNextTuple and writes the tuples to an 
//	 * a ByteBuffer which is then written to a file
//	 */
//	public void dump(int queryNumber){
//		try {
//			tuple t;
//			FileOutputStream fout = new FileOutputStream( outputdir+"/query"+queryNumber );
//			FileChannel fcOut = fout.getChannel();
//			ByteBuffer outBuf = ByteBuffer.allocate( 4096 );
//			TupleWriter writer = new binaryTupleWriter(outBuf);
//
//			List<Integer> body;
////		    variables for buffer meta-data
//			int tuple_size=0;
////			tuples seen is how many tuples have been read into the current buffer
//			int tuples_seen=0;
//			int buffer_capacity=0;
//
//			while ((t=this.getNextTuple()).getBody() != null){
//				try {
//	//				outBuf.clear();
//					body=t.getBody(); 
//					tuple_size = body.size();
//	//				calculates buffer capacity as integer division of buffer size by the number of bytes in a tuple body
//					buffer_capacity = 4088/(tuple_size*4);
//	
//	//				write tuple into buffer. Current position in buffer should be the number of bytes in the tuples 
//	//				already read into the buffer
//					writer.writeNextTuple(body, (tuples_seen*tuple_size)*4);
//					
//					tuples_seen++;
//					
//	//				if the max number of tuples possible has been read into the buffer, write to file and reset buffer 
//					if (tuples_seen==buffer_capacity){
//	//					write meta-data to buffer
//						outBuf.putInt(0,tuple_size);
//						outBuf.putInt(4,tuples_seen);
//	
//	//					System.out.println(outBuf.getInt(0));
//	//					System.out.println(outBuf.getInt(4));
//	//					System.out.println(outBuf.getInt(16));
//	
//	//					now write buffer to file
//	//					outBuf.flip();
//						fcOut.write( outBuf );
//						
//						tuples_seen=0;
//						outBuf.clear();
//					} 
//				} catch (Exception e){
//					System.out.println("exception in dump");
//					e.printStackTrace();
//				}
//				
//			}
//
////			after while-loop might still have data in buffer to write to file
////			fill in metadata
//			outBuf.putInt(0,tuple_size);
//			outBuf.putInt(4,tuples_seen);
//			
////			the last filled position in the buffer is the number of tuples seen time the size of the tuples 
//			int position = 4*(2+(tuples_seen)*tuple_size);
//
//
//			for (int k=position; k<4096; k++){
//				outBuf.put(k, (byte) 0);
//			}
//			
//
////			outBuf.flip();
//			fcOut.write( outBuf );
//			fout.close();
//		} catch (Exception e){
//			System.err.println("Exception occurred writing to file");
//			e.printStackTrace();
//		}
//	}
//	

		
}

