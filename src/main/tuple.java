package main;
/** An instance of tuple stores a list of strings representing the fields from a database
 *  that have been extracted by an operator
 */

import java.util.List;
//import java.util.ArrayList;

public class tuple {
	private List<Integer> body;
	private List<String> schema;
	private String lastTable;

	
	/** tuple(List<String> b) creates an instance of tuple with b as its body, 
	 * 	s representing its schema, and t as the last table which contributed to the tuple
	 * 	@param [b] is the list of integers to be stored in the tuple's body
	 * 	@param [s] is the list of column names representing the schema for the tuple
	 * 	@param [t] is the name of the last table which contributed to the tuple 
	 * 				(the table the tuple was drawn from if there is no join)
	 */
	public tuple(List<Integer> b, List<String> s, String t){
		this.body=b;
		this.schema=s;
		this.lastTable=t;
	}


	/** tuple() creates an instance of tuple with a null body
	 */	
	public tuple(){
		this.body=null;
	}
	
	/** getBody() returns body of tuple
	 * @return List<Integer> body representing tuple
	 */	
	public List<Integer> getBody(){
		return body;
	}
	
	/** getSchema() returns schema of tuple
	 * @return List<String> schema representing column names of tuple
	 */	
	public List<String> getSchema(){
		return schema;
	}
	/** getLastTable() returns the last table the tuple was built from
	 * @return String name of last table part of tuple was taken from
	 */	
	public String getLastTable(){
		return lastTable;
	}
	/** displayBody() prints the tuple's body to stdout
	 */	
	public void displayBody(){
		System.out.println(body);
	}
}
