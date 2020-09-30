package main;
/** Every interpreter has one dataBase catalog which never changes, which stores information on 
 * table schemas and table files
 */

import java.util.HashMap;
import java.util.Map;
import java.util.List;

import main.Interpreter;

import java.util.ArrayList;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.Arrays;
import java.io.File;


public class databaseCatalog {
//	create an object of databaseCatalog using schemas in schema.txt

		private static String inputDir = Interpreter.getInputDir();
//		private static String inputDir = "input";
//		private static databaseCatalog instance = new databaseCatalog(inputDir+"/db/schema.txt");
//		private static String inputDir="./input";
		private static databaseCatalog instance = new databaseCatalog(inputDir+"/db/schema.txt");

//		private static databaseCatalog instance = new databaseCatalog("./input/db/schema.txt");
//	   a mapping of table names to the files they are located in (for bot human readable and byte versions)
		public Map<String, String> files = new HashMap<String, String>();
		public Map<String, String> binaryFiles = new HashMap<String, String>();
		public Map<String, List<String>> tableIndexes = new HashMap<String, List<String>>();
		HashMap<String,Boolean> indexClusters = new HashMap<String,Boolean>();

//	   a mapping of table to their schemas
		public Map<String, ArrayList<String>> schemas = new HashMap<String, ArrayList<String>>();

		private databaseCatalog(String filename){
			this.schemas = getSchemas(filename);
			this.files = getTables();
			this.binaryFiles = getBinaryTables();
			
			try {
	//			map tables to any index on them by reading which indexes are available
				BufferedReader indexReader = new BufferedReader(new FileReader(inputDir+"/db/index_info.txt"));
	//			HashMap<String,String> indexes = new HashMap<String,String>();
	
				String indexLine;
				String[] splitLine;
				
	//			for each line in the file, map tables to the indexes on the table
				while ((indexLine = indexReader.readLine()) != null) {
					splitLine = indexLine.split(" ");
	//				map table name to column index is on 
					if (tableIndexes.get(splitLine[0])==null){
						tableIndexes.put(splitLine[0], new ArrayList<String>());
	
					} 
					List<String> newVal = tableIndexes.get(splitLine[0]);
					newVal.add(splitLine[0]+"."+splitLine[1]);
					tableIndexes.put(splitLine[0], newVal);
					
	//				create list of names of existing indexes
	//				map index column to whether or not its a clustered index
					indexClusters.put(splitLine[0]+"."+splitLine[1], splitLine[2].equals("1"));
	//				if the buildIndexes flag is set, create each index specified 
	
				}
				indexReader.close();
			
			} catch (Exception e) {
				System.err.println("Exception occurred creating index mappings");
				e.printStackTrace();
			}
			
			
			
			
		};
	   
	   
	/** getSchemas(String filename) returns the schemas associated with the tables listed in filename
	 * @param filename the text file listing the table schemas, with the first column as table names and the 
	 * 	subsequent columns containing the names of the columns in each table
	 * @return a map of table names to a list of column names ordered by their appearance in filename
	 */
	   private Map<String, ArrayList<String>> getSchemas(String filename){
		   Map<String, ArrayList<String>> c = new  HashMap<String, ArrayList<String>>();

		   try {
			   BufferedReader reader = new BufferedReader(new FileReader(filename));
			   String current_line;
			   ArrayList<String> columns;
			   ArrayList<String> newColumns;

			   String tableName;
			   while ((current_line = reader.readLine())!=null){
				   columns = new ArrayList<String>(Arrays.asList(current_line.split(" ")));
				   newColumns = new ArrayList<String>();
				   tableName = columns.get(0);
				   columns.remove(0);
				   
//				   add table name to reference columns
				   for (String col : columns){
					   newColumns.add(tableName+"."+col);
				   }
				   c.put(tableName, newColumns);
				   

			   }
			   reader.close();
				   
		   } catch (Exception e){
			   System.err.println("Exception occurred during parsing");
			   e.printStackTrace();
		   }

		   return c;
	   }
	   
/**getTables() returns a mapping of table names to the names of the human-readable files they are defined in
 * @return a map of table to file names
 */
	   private Map<String, String> getTables(){
		   Map<String, String> tableFiles = new HashMap<String, String>();
		   String path = inputDir+"/db/data";
//		   String path = inputDir + "/db/data";

		   File folder = new File(path);
		   File[] tableList = folder.listFiles();
   		   String tableName;
   		   for (File table : tableList){
   			   tableName=table.getName();
   			   if ((tableName.contains("_humanreadable"))){
   				   tableFiles.put(tableName.replace("_humanreadable", ""), table.toString());
   			   }

   		   }
		   
		   return tableFiles;
	   }
	   
	   /**getBinaryTables() returns a mapping of table names to the names of the binary files they are defined in
	    * @return a map of table to file names
	    */
	   	  private Map<String, String> getBinaryTables(){
	   		   Map<String, String> tableFiles = new HashMap<String, String>();
			   String path = inputDir+"/db/data";
//	   		   String path = inputDir + "/db/data";
			   
	   		   File folder = new File(path);
	   		   File[] tableList = folder.listFiles();
	   		   String tableName;
	   		   for (File table : tableList){
	   			   tableName=table.getName();
	   			   if (!(tableName.contains("_humanreadable"))){
	   				   tableFiles.put(tableName, table.toString());
	   			   }
	   		   }
	   		   
	   		   return tableFiles;
	   	   }     

/**	getFile(String table) find the name of the human readable file where table is defined
 *	 @param table is the table being matched to a filename 
 * 		@return the name of the file table is defined in
 */
	   public String getFile(String table){

		   return files.get(table);
	   }
	   
/**		getBinaryFile(String table) find the name of the binary file where table is defined
*	 	 @param table is the table being matched to a filename 
* 		 @return the name of the file table is defined in
*/
	   public String getBinaryFile(String table){
		   return binaryFiles.get(table);
	   }
	   
/**	   getSchema(String table) finds the schema associated with a table as a list of column ids
 * 		@param table is the table being considered
 * 		@return list of column names
 */
	   public ArrayList<String> getSchema(String table){
		   return schemas.get(table);
	   }	   
	   
/**	  getLabelColumn(String table, String id)  finds the column position of a specific attribute
 * 		@param table is the name of the table the column is in
 * 		@param id is the name of the column
 * 		@return the integer index of the column in the table 
 */
	   public int getLabelColumn(String table, String id){
		   ArrayList<String> schema = schemas.get(table);
		   return schema.indexOf(id);
	   }
	   
/** getIdnexes(String table) returns a list of indexes that exist for a given table
 * @param table is the name of the table indexes are being found for
 * @return the list of index names (as strings)
 */
	   public List<String> getIndexes(String table){
		   return tableIndexes.get(table);
	   }
/** getCluster(String index) returns whether an index is clustered or not
* @param index is the name of the index
* @return true if index is clustered, false other wise
*/
   public Boolean getCluster(String index){
	   if(indexClusters.get(index)==null) System.err.println("unkown index looked up");
	   return indexClusters.get(index);
   }   
/** getInstance() returns the only instance of databaseCatalog which exists
 * @return the only instance of databaseCatalog for the current program
 */
	   public static databaseCatalog getInstance(){
	      return instance;
	   }

}