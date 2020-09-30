package main;
import java.io.BufferedWriter;
import java.nio.ByteBuffer;
import java.util.List;

public class humanTupleWriter implements TupleWriter {
	
	BufferedWriter out;
	
	public humanTupleWriter(BufferedWriter o){
		try {
			out = o;
		} catch (Exception e) {
			System.err.println("Exception occurred creating binary writer");
			e.printStackTrace();
		}
	}
	
	
//	BufferedWriter out
	@Override
	public void writeNextTuple(List<Integer> tuple_body, int tuples_size) {
		// TODO Auto-generated method stub
		try{
			int len = tuple_body.size();
			for (int i=0; i<len-1; i++ ){
				out.write(tuple_body.get(i)+",");	 
			}
			out.write(tuple_body.get(len-1)+"");		 
			out.write("\n");
		
		} catch (Exception e) {
			System.err.println("Exception occurred writing to file");
			e.printStackTrace();		
		}
	}
}
