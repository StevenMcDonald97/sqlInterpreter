package main;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;

public class nioTest {
	
	public static void main(String[] args) {
		try {
//			databaseCatalog cat = databaseCatalog.getInstance();
//			ArrayList<String> schema=cat.getSchema("Boats");
//			binaryTupleReader testReader = new binaryTupleReader("./input/db/data/Boats", "Boats", schema);
//   		 	System.out.println("first tuple:"+testReader.getSpecificTuple(1,0).getBody());
//   		 	testReader.setFileReaderPosition(1, 0);
//   		 	System.out.println("first tuple:"+testReader.readNextTuple().getBody());

			
	//		reading 
			FileInputStream fin = new FileInputStream( "./input/db/indexes/Sailors.A" );
			FileChannel fcIn = fin.getChannel();
			ByteBuffer buffer = ByteBuffer.allocate( 4096 );
	//		fcIn.read( buffer );
	//		
	//		
	//		
	////		writing
			FileWriter fstream = new FileWriter("./output/test_index");
			BufferedWriter out = new BufferedWriter(fstream);
	//		
	//		for (int i=0; i<message.length; ++i) {
	//			buffer.put( message[i] );
	//		}
	//		buffer.flip();
	//		
	//		fc.write( buffer );
	//		
			
			while (true) {
			     buffer.clear();
			     int r = fcIn.read( buffer );
//			     System.out.println("here");

			     if (r==-1) {
				     System.out.println("here_2");

			       break;
			     }
			     
			     try{
			    	 for (int i=0; i<4096;i+=4) {
			    		 out.write(buffer.getInt(i)+" ");
			    	 }


			     }catch (Exception e){
						System.err.println("Exception occurred reading int");
						e.printStackTrace();
					}


//			     break;

			}
			
//			NOTE: for overwriting buffer, can track number of bites written to it, then overwrite? 
//			Or just write over with zeros
			
			fin.close();
			out.close();
		} catch (Exception e){
			System.err.println("Exception occurred in copying file");
			e.printStackTrace();
		}
		
	}
	
	
}
