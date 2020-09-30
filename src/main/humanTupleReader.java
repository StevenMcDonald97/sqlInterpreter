package main;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

public class humanTupleReader implements TupleReader {

	private BufferedReader reader;
	private String table;
	private ArrayList<String> schema;
	private String dataFile;
	
	/** humanTupleReader(String table, ArrayList<String> Schema) creates an instance of ScanOperator 
	 * 	on the datafile associated with table,  
	 * 	@param [string table] is the name of the table to be scanned 
	 * 	@return the instance of ScanOperator with a reader opened on the file associated with table
	 * */
	public humanTupleReader(String dataFile, String t, ArrayList<String> s){
		this.dataFile=dataFile;
		this.table=t;
		this.schema =s;
		try {
			this.reader = new BufferedReader(new FileReader(dataFile));
		} catch (Exception e){
			System.out.println("Exception opening bufferedreader");
			e.printStackTrace();
		}

	}
	
	
	@Override
	public tuple readNextTuple() {
		// TODO Auto-generated method stub
		try {
			String nextLine = reader.readLine();
			tuple t;
			if (nextLine != null){
				String[] string_body = nextLine.split(",");
				ArrayList<Integer> body = new ArrayList<Integer>();
				
				for (String col : string_body){
					body.add(Integer.parseInt(col));
				}
				t = new tuple(body, schema, table);
				return t;
			} else {
				return new tuple();
			}
			
			
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
			
			return new tuple();
		}
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		try {
			reader.close();
		}catch(Exception e){
			System.out.println("error occurred closing reader");
			e.printStackTrace();
		}
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		try {
			reader.close();
			this.reader = new BufferedReader(new FileReader(dataFile));
		} catch (Exception e){
			System.out.println("Exception reseting bufferedreader");
			e.printStackTrace();
		}

	}
	
	/** setFileReaderPosition returns a specific tuple on a specific page of the data file
	 * Important: This will move the pointer up to this tuple, and subsequent calls to readNextTuple
	 * will take the tuples after this
	 * @param pageNum is which page the tuple is on
	 * @tuplePos is which tuple on the page it is
	 */
	public tuple getSpecificTuple(int pageNum, int tuplePos) {
		System.out.println("getSpecificTuple is unimplementedfor humanTupleReader");
		return new tuple();
	}
	/** getSpecificTuple returns a specific tuple on a specific page of the data file
	 * Important: This will move the pointer up to this tuple, and subsequent calls to readNextTuple
	 * will take the tuples after this
	 * @param pageNum is which page the tuple is on
	 * @tuplePos is which tuple on the page it is
	 * @return the tuple at the specified position
	 */
	public void setFileReaderPosition(int pageNum, int tuplePos)  {
		System.out.println("setFileReaderPosition is unimplementedfor humanTupleReader");
	}
}
