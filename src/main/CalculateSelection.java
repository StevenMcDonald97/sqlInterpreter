package main;

import java.util.Arrays;
import java.util.List;

public class CalculateSelection {
	databaseCatalog cat = databaseCatalog.getInstance();
	int total_tuples;
	int bytes_int = 4;
	int page_bytes = 4096;
	List<String> schema;
//	String attribute;
	GetStats gs;

	/**
	 * Instantiate CalculateSelection
	 * @param attribute that is being selected
	 * @param table name
	 * @param schema of tuple
	 */
	public CalculateSelection(String table, List<String> schema){
//		System.out.println(attribute);
//		System.out.println(table);
//		System.out.println(schema);
		//System.out.println(this.attribute);
		this.schema = schema;
		gs = new GetStats(); 
		total_tuples = gs.getNumTuples(table);
		
		
	}
	
	/**
	 * For the scan, you can estimate the cost by referencing the stats file for
	 * how many tuples the base table has, multiplying by the size of one tuple, 
	 * and dividing that by the page size which is still 4096 bytes
	 * @return scan cost
	 */
	public int calcScan(){
		int scan=0;
		scan =(int) Math.ceil( (total_tuples*bytes_int*schema.size()) / page_bytes );
		return scan;
		
	}
	
	/**
	 * For index scan, compute range of values selected divided by total range of 
	 * values of table
	 * @return reduction factor
	 */
	public double reductFactor(String attribute, int lower, int upper){
		//System.out.println(lower); System.out.println(upper);
		Integer[] bounds = gs.getOriginalBounds(attribute);
		double lower_total=bounds[0]; 
		double upper_total= bounds[1];
		double out=0; 
		double selectrange=0;

		if (lower==-1&&upper==-1){
			out = 1; return out;
		}
		else if (upper==-1){
			selectrange=upper_total-lower+1;
		}
		else if (lower==-1){
			selectrange = upper-lower_total+1;
		}
		else{
			selectrange = upper-lower+1;
			//System.out.println(selectrange);
		}
		 
		double total_range = upper_total-lower_total+1;

		out = selectrange / total_range;
		return out;
	}
	
	/**
	 * Now that you have the reduction factor, you can compute the cost of the index scan as follows. Assume the
cost of root-to-leaf traversal is 3. If the index is clustered, you can go directly to the rst relevant data
page and then need to access a fraction of all the data pages as determined by the reduction factor. 
	 * @param reductfactor is the reduction factor
	 * @return
	 */
	public int indexScanCostClustered(double reductfactor){
		int num_pages=(int)Math.ceil(total_tuples*bytes_int*schema.size() / page_bytes);
		
		int clusteredcost=(int) Math.ceil(3 + num_pages*reductfactor);
		
		return clusteredcost;
	}
	
	/**
	 * If the index is unclustered, you need to scan a fraction of the leaves (determined by the reduction factor) and
then in the worst case one page I/O for every tuple that matches the selection
	 * @param reductfactor
	 * @return
	 */
	public int indexScanCostUnclustered(double reductfactor, int leaves){
//		int leaves=(int)Math.ceil(total_tuples*bytes_int*schema.size() / page_bytes); //*******************
		int unclusteredcost=(int) Math.ceil(3 + total_tuples*reductfactor + leaves * reductfactor);
		
		return unclusteredcost;
	}
}