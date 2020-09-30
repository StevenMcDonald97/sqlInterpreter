package main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/**A class for reading all relation data files and writing a stats.txt file containing statistics
 * on the data
 * @author stevenmcdonald
 *
 */
public class GetStats {
	
	private static String inputDir = Interpreter.getInputDir();
	databaseCatalog cat = databaseCatalog.getInstance();
//	map attributes to min/max bounds
	private static HashMap<String, Integer[]> attrBounds = new HashMap<String, Integer[]>();
	private static HashMap<String, Integer[]> originalBounds = new HashMap<String, Integer[]>();

	private static HashMap<String, Integer> num_tuples = new HashMap<String, Integer>();
	
	public GetStats(){}
	
	/**writeStats() writes statistics for each relation in the database to a stats.txt file
	 *  following the form: relation name, number of tuples, and then attribute names followed
	 *  by the minimum and maximum values of the tuples
	 */
	public void writeStats() {
		   String path = inputDir+"/db/data";
   		   File folder = new File(path);
   		   File[] tableList = folder.listFiles();
   		   String tableName;
   		   
   		   List<String> fileStats = new ArrayList<String>();
   		   List<Integer> thisLine;
//			keep track of info on data
   		   ArrayList<String> schema;

   		   int lineCount;
   		   
//   		   for file
   		   try {
//   			process every data relation in the input folder
	   		   for (File table : tableList){
	   	   		   List<Integer> min= new ArrayList<Integer>();
	   	   		   List<Integer> max= new ArrayList<Integer>();
	   			   tableName=table.getName();
	   			   if (tableName.charAt(0) != '.' && !(tableName.contains("_humanreadable"))){
		   			   schema=cat.getSchema(tableName);

		   			   binaryTupleReader fileReader = new binaryTupleReader(table.getAbsolutePath(), tableName, schema);
		   			   List<Integer> tuple = fileReader.readNextTuple().getBody();
		   			   for (Integer colVal : tuple) {
		   				   min.add(colVal);
		   				   max.add(colVal);
		   			   }
		   			   lineCount=1; 

	//	   			   loop through every line in the file
		   			   while ((thisLine=fileReader.readNextTuple().getBody())!=null) {
	//		   			   for each line, check if the max or min value for a column needs to be updated
			   			   for (int i=0;i<thisLine.size();i++) {
			   				   int current_val=thisLine.get(i);
			   				   if (current_val<min.get(i)) {
			   					   min.add(i, current_val);
			   				   } else if (current_val>max.get(i)) {
			   					   max.add(i, current_val);
			   				   } 
			   			   }  
		   				   lineCount++;
		   			   }
		   			   
	//	   			   create string representing data for this relation
		   			   String tableStats = tableName+" "+lineCount;

		   			   
	//	   			   add info for each column to string
		   			   for (int j=0;j<schema.size();j++) {
		   				   num_tuples.put(tableName, lineCount); //UPDATE NUMBER OF TUPLES
		   				   Integer[] bounds = {min.get(j), (max.get(j))};
		   				   attrBounds.put(schema.get(j), bounds);
		   				   Integer[] oldBounds = {min.get(j), (max.get(j))};
		   				   originalBounds.put(""+schema.get(j), oldBounds);
		   				   tableStats = tableStats+" "+schema.get(j).replace(tableName+".", " ")+","+min.get(j)+","+max.get(j);
		   			   }
		   			   
		   			   fileStats.add(tableStats);
	   			   }
	   		   }
//   			   System.out.println(fileStats);
   			   BufferedWriter statsWriter = new BufferedWriter(new FileWriter(inputDir+"/db/stats.txt"));
   			   try {
   				   //create a temporary file
   				   for (String tStats : fileStats) {
   					   statsWriter.write(tStats+"\n");
					}
   			   } catch (Exception e) {
   				   System.err.println("Exception occurred writing out stats file");
   				   e.printStackTrace();
   			   } 
   			   statsWriter.close();
   			   
   		   } catch (Exception e) {
   			System.err.println("Exception occurred scanning data files");
   			e.printStackTrace();
   		}
	}
	
	/**
	 * getBounds(String attribute) returns the minimum and maximum values associated with an attribute
	 * @param attribute is the attribute bounds are being found for
	 * @return an array of the minimum/maximum value that attribute takes
	 */
	public Integer[] getBounds(String attribute) {
		Integer[] bnds = attrBounds.get(attribute).clone();
		return bnds;
	}
	
	/**
	 * getOriginalBounds(String attribute) returns the original minimum and maximum values associated with an attribute
	 * @param attribute is the attribute bounds are being found for
	 * @return an array of the minimum/maximum value that attribute takes
	 */
	public Integer[] getOriginalBounds(String attribute) {
		return originalBounds.get(attribute);
	}
	
	/**
	 * getAllBounds() returns the attrBounds map
	 * @return map of attributes to array containing its min/max bounds
	 */
	public HashMap<String, Integer[]> getAllBounds() {
		return attrBounds;
	}
	
	public int getNumTuples(String table){
		return num_tuples.get(table);
	}
	

}
