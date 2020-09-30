package main;
import java.io.FileReader;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.util.Comparator;

import logical.OperatorTree;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.PlainSelect;

import java.io.BufferedReader;
import java.io.File;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

/** An instance of interpreter reads in an input directory location and an output directory location,
 *  then for each query in the queries file in the input directory it will create a queryPlan object,
 *  parse the queryPlan, and write the result of the query to a file in the output directory
 *  inputDir must contain a queries.sql file and a db directory
 */
public class Interpreter {
	static String configFile;
//	input directory path
	static String inputDir;
//	output directory path
	static String outputDir;
//	temp output directory path
	static String tempDir;
	
	
//	path to queries file
//	static String queryFilePath;
//	reader to read in queries file
	static FileReader reader;
	
	private static String queryFilePath;
	
	/**getInputDir() returns the input directory associated with the interpreter, where queries and tables are
	 * @return the input directory associated with the interpreter 
	 */
	public static String getInputDir(){
		return inputDir;
	}
	
	/**getInputDir() returns the input directory associated with the interpreter where output files are written
	 * @return the output directory associated with the interpreter 
	 */
	public static String getOutputDir(){
		return outputDir;
	}
	
	/**getTemoDir() returns the temp output directory associated with the interpreter where output files are written
	 * @return the output directory associated with the interpreter 
	 */
	public static String getTempDir(){
		return tempDir;
	}
	
	/**deleteDirectory(File directoryToBeDeleted) removes all contents of a folder
	 * taken from: https://www.baeldung.com/java-delete-directory
	 * @param directoryToBeDeleted is the directory being emptied
	 */
	public static void deleteDirectory(File directoryToBeDeleted) {
	    File[] allContents = directoryToBeDeleted.listFiles();
	    if (allContents != null) {
	        for (File file : allContents) {
	            deleteDirectory(file);
	        }
	    }
	    directoryToBeDeleted.delete();
	}
	
	public static void main(String args[]){  		
		//take query from file, extract Table name from it, and 		
		try {
			
			configFile = args[0];
			
//			open reader on configuration file
			BufferedReader configFileReader = new BufferedReader(new FileReader(configFile));
			
//			extract directory location s
			inputDir = configFileReader.readLine();
			outputDir = configFileReader.readLine();
			tempDir = configFileReader.readLine();
			
			
			queryFilePath = inputDir+"/queries.sql";
			//queryFilePath = "./queries.sql";
			reader = new FileReader(queryFilePath);
			CCJSqlParser parser = new CCJSqlParser(reader);

			Statement statement;
			Select select;
//			PlainSelect body;
			
			GetStats stats = new GetStats();
			stats.writeStats();

			
//===========================================================================================
//===========================================================================================
			
//			map tables to any index on them by reading which indexes are available
			BufferedReader indexReader = new BufferedReader(new FileReader(inputDir+"/db/index_info.txt"));
//			HashMap<String,String> indexes = new HashMap<String,String>();
			HashMap<String,Boolean> indexClusters = new HashMap<String,Boolean>();
			List<String> indexNames = new ArrayList<String>();

			String indexLine;
			String[] splitLine;
			String index;
//			int leafn;
			HashMap<String, Integer> indexLeaves = new HashMap<String, Integer>();
//			for each line in the file, map tables to the index on the table
			while ((indexLine = indexReader.readLine()) != null) {
				splitLine = indexLine.split(" ");
//				map table name to column index is on 
//				indexes.put(splitLine[0], splitLine[0]+"."+splitLine[1]);
//				create list of names of existing indexes
				index=splitLine[0]+"."+splitLine[1];
				indexNames.add(index);
//				map index column to whether or not its a clustered index
				indexClusters.put(index, splitLine[2].equals("1"));
//				if the buildIndexes flag is set, create each index specified 
				BPlusTree indexTree = new BPlusTree(splitLine[0], index, Integer.parseInt(splitLine[3]), Integer.parseInt(splitLine[2]));
//				inputDir+"/db/data/"+splitLine[0],
				indexLeaves.put(index, indexTree.leafNum);

			}
			indexReader.close();
			

//===========================================================================================	
//===========================================================================================

			long time1=0;
			long time2=0;

			int querynumber =1;
			
//			only evaluate queries if configFile says to
				while ((statement = parser.Statement()) != null) {
					System.out.println(statement.toString());

					try {
						select = (Select) statement;
//						create a catalog for storing alias-table mappings for this query
						aliasCatalog aCat = new aliasCatalog(indexLeaves, indexClusters);
						
						
						LogicalTreeMaker tree = new LogicalTreeMaker((PlainSelect) select.getSelectBody(), aCat);
						OperatorTree op = tree.getRoot();

						long ptime1=System.currentTimeMillis();
						PhysicalPlanBuilder pp = new PhysicalPlanBuilder(op, aCat);
						op.accept(pp);
						long ptime2=System.currentTimeMillis();
						System.out.println("time to build plan: " + (ptime2-ptime1));
						
//						write logical plan our
						tree.writeLogicalPlan(querynumber);
						pp.writePhysicalPlan(querynumber);
					
						time1=System.currentTimeMillis();
						physical.operator planOp = pp.getOp();
						planOp.dump(querynumber);
						time2=System.currentTimeMillis();
						System.out.println("time to dump: " + (time2-time1));
						

						planOp.reset();					
						
					} catch (Exception e) {
						System.err.println("Exception occurred while processing query");
						e.printStackTrace();
					}
					querynumber++;
					
					File dir = new File(tempDir);
					deleteDirectory(dir);
	
				}
			
			configFileReader.close();

		} catch (Exception e) {
			System.err.println("Exception occurred during parsing");
			e.printStackTrace();
		}
		
	}  
	
	
}
