package main;
/**An aliasCatalog tracks mapping of aliases to tables and vice versa
 *  It also is used to retain some index information
 */

import java.util.HashMap;
import java.util.Map;

import main.Interpreter;

import java.util.ArrayList;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.Arrays;
import java.io.File;

public class aliasCatalog {
	//	map table names to their corresponding aliases
	private static HashMap<String,String> tableAliases;
	private static HashMap<String,String> aliasTables;
	public HashMap<String, Integer> indexLeafNums;
	public HashMap<String, Boolean> indexClusters;
	public boolean hasAliases=false;
	
	
	/** Create instance of aliasCatalog with empty maps but with mappings of indexes how many leaves they has
	 */
	public aliasCatalog(HashMap<String, Integer>  leafNums, HashMap<String, Boolean> ic) {
		this.tableAliases = new HashMap<String,String>();
		this.aliasTables = new HashMap<String,String>();
		this.indexLeafNums=leafNums;
		this.indexClusters=ic;
	}
		
	/**getAliasTable() returns the aliasTables hash map
	 * @return the mapping of aliases to table names
	 */ 
	public HashMap<String,String> getAliasTable() {
		return aliasTables;
	}
		  
	/**getAliasTable() returns the aliasTables hash map
	 * @return the mapping of table names to aliases
	 */ 
	public HashMap<String,String> getTableAliases() {
		return tableAliases;
	} 
	
	/**
	 * Add a mapping of a table to alias to tableAliases
	 * @param t table name
	 * @param a alias name
	 */
	public void addToTableAliases(String t, String a) {
		tableAliases.put(t, a);
		hasAliases=true;

	}
	/**
	 * Add a mapping of an alias to a able to aliasTable
	 * @param a alias name
	 * @param t table name
	 */
	public void addToAliaseTables(String a, String t) {
		aliasTables.put(a, t);
		hasAliases=true;
	}
			  
		  
	
}
