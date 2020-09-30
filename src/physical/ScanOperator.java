package physical;

/** An instance of scanOperator is used to extract full rows from a database.
 * It finds the file associated with a table, opens a BufferedReader object
 * on that file, and then reads data from the BufferedReader. 
 */
import java.util.ArrayList;

import main.TupleReader;
import main.binaryTupleReader;
import main.databaseCatalog;
import main.tuple;

public class ScanOperator extends operator{
	private String table;
	private String dataFile;
	private TupleReader reader;
	private ArrayList<String> schema;

	databaseCatalog cat = databaseCatalog.getInstance();
	
	/** ScanOperator(String table) creates an instance of ScanOperator 
	 * 	on the datafile associated with table,  
	 * 	@param [string table] is the name of the table to be scanned 
	 * 	@return the instance of ScanOperator with a reader opened on the file associated with table
	 * */
	public ScanOperator(String table, ArrayList<String> Schema){
		this.table = table;
		this.dataFile=(cat.getBinaryFile(table));
		this.schema=Schema;

//		try {
//			this.reader = new humanTupleReader(this.dataFile, table, schema);
//		} catch (Exception e) {
//			System.err.println("Exception occurred opening file reader");
//			e.printStackTrace();
//		}
		
		try {
			this.reader = new binaryTupleReader(dataFile, table, schema);
		} catch (Exception e) {
			System.err.println("Exception occurred opening file reader");
			e.printStackTrace();
		}
	}
		
	/**getNextTuple() finds the next tuple in the specified query file
	 * @return a tuple with the next line from reader split into a list of string as its body, 
	 * 	or a tuple with a null body if reader is at the end of dataFile.
	 */
	public tuple getNextTuple(){
		try {
			
			tuple t = reader.readNextTuple();
			return t;
			
		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
			
			return new tuple();
		}
	}
	
	/**reset() returns the scan to the first line of the queries file. 
	 * The next call to getNextTuple() will then be the first tuple from dataFile again. 
	 * */
	public void reset(){
		try {
			reader.reset();
		} catch (Exception e){
			System.err.println("Exception occurred resetting file stream");
			e.printStackTrace();
		}
	}
	
	/** getTable() returns the name of the table the scan operator is opened on
	 *  @return table is the table associated with the scan operator
	 */
	public String getTable(){
		return table;
	}
	
	/** getSchema() returns the schema of the table the scan operator is opened on
	 *  @return schema is a list of the columns in the table the scan operator is opened on
	 */
	public ArrayList<String> getSchema(){
		return schema;
	}
	
	public void reset(int i) {};

}
