package main;

import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;

/** 
 * An instance of randomData create random table data files and a schema file for testing purposes.
 * Keeps same schema as given data (i.e. sailors, boats, reserves) 
 * 		Sailors A B C
		Boats D E F
		Reserves G H
 * Given number of tuples, and range for attributes, generate three binary data files 
 */
public class randomData {
//	direct data files to test input directory 
	private static String outputdir = "./testInput/db/data";
	
	public static void writeFile(FileOutputStream fout, int numFields, int numTuples, int maxVal){
		FileChannel fcOut = fout.getChannel();
		ByteBuffer outBuf = ByteBuffer.allocate( 4096 );
		TupleWriter writer = new binaryTupleWriter(outBuf);
		
//		number of tuples to put in buffer at a time
		int buffer_capacity = 4088/(numFields*4);
		
//		tuples seen is how many tuples have been read into the current buffer
		int tuples_seen=0;
		
//		random int generator 
		Random random = new Random();
		List<Integer> body = new ArrayList<Integer>();
		
		try{
			for (int i=0; i<numTuples; i++){
				
				for (int j=0; j<numFields;j++){
					body.add(random.nextInt(maxVal));
				}
				
				writer.writeNextTuple(body, (tuples_seen*numFields)*4);
				body.clear();
				tuples_seen++;

				if (tuples_seen==buffer_capacity){
	//				write meta-data to buffer
					
					outBuf.putInt(0, numFields);
					outBuf.putInt(4, tuples_seen);
	
	//				now write buffer to file
	//				outBuf.flip();
					fcOut.write( outBuf );
					
					tuples_seen=0;
					outBuf.clear();
				} 
				
			}
			
	
	//		after while-loop might still have data in buffer to write to file
	//		fill in metadata
			outBuf.putInt(0,numFields);
			outBuf.putInt(4,tuples_seen);

			
	//		the last filled position in the buffer is the number of tuples seen time the size of the tuples 
			int position = 4*(2+(tuples_seen)*numFields);
	
	
			for (int k=position; k<4096; k++){
				outBuf.put(k, (byte) 0);
			}
			
	//		outBuf.flip();
			fcOut.write( outBuf );
			fout.close();
		
		} catch (Exception e){
			System.err.println("Exception occurred writing outbufer to random data file");
			e.printStackTrace();
		}

	}
	
	
	
	/**	First argument passe in become the number of tuples in each table. The second is the maximum value
	 *  Command line arguments must be integers!
	 */
	public static void main(String args[]){
		try {
			int numberTuples = Integer.parseInt(args[0]);
			int maxVal = Integer.parseInt(args[1]);
			
//			each file needs to be written seperately
			FileOutputStream foutSailors = new FileOutputStream( outputdir+"/Sailors" );
			writeFile(foutSailors, 3, numberTuples, maxVal);
			
			FileOutputStream foutBoats = new FileOutputStream( outputdir+"/Boats" );
			writeFile(foutBoats, 3, numberTuples, maxVal);

			FileOutputStream foutReserves = new FileOutputStream( outputdir+"/Reserves" );
			writeFile(foutReserves, 2, numberTuples, maxVal);
			
			FileOutputStream foutExtra = new FileOutputStream( outputdir+"/Extra" );
			writeFile(foutExtra, 2, numberTuples, maxVal);

		} catch (Exception e){
			System.err.println("Exception occurred opening file stream for random data");
			e.printStackTrace();
		}
	}

}
