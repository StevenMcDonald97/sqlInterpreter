package main;

import java.util.ArrayList;
import java.io.File;

public class indexScanTest {

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
				databaseCatalog cat = databaseCatalog.getInstance();
				ArrayList<String> schema=cat.getSchema("Boats");
				physical.IndexScan inOp= new physical.IndexScan("Boats", "input/expected_indexes/Boats.E",schema, 1, 5, 2, false);
				
				System.out.println(inOp.getNextTuple().getBody());
				System.out.println(inOp.getNextTuple().getBody());
				System.out.println(inOp.getNextTuple().getBody());
				
				inOp.reset();
				System.out.println(inOp.getNextTuple().getBody());
				System.out.println(inOp.getNextTuple().getBody());
				System.out.println(inOp.getNextTuple().getBody());
				


			} catch (Exception e) {
				System.err.println("Exception occurred during testing indexScan");
				e.printStackTrace();
			}
			
		}  
		
}
