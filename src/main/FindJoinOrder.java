package main;
import java.util.List;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.LinkedList;

import java.util.HashMap;
import java.util.Arrays;
import net.sf.jsqlparser.expression.Expression;

/**findJoinOrder is a class for calculating the best order to perform joins in an expression  
 * 
 * @author stevenmcdonald
 *
 */
public class FindJoinOrder {
	private HashMap<String, List<String>> tableStats = new HashMap<String, List<String>>();
//	store the cost of join each set of relations
	private HashMap<List<String>, Integer> setCosts = new HashMap<List<String>, Integer>();
//	an additional setSizes map is helpful, because the cost of two relation joins is 0 but 
//	we may need to know their size
	private HashMap<List<String>, Integer> setSizes = new HashMap<List<String>, Integer>();

//	store the ideal order for each set
//	only needs to store last table added? can recurse to get full order 
	private HashMap<List<String>, String> setOrders = new HashMap<List<String>, String>();
	
//	store v-values for each attribute in each table
//	HashMap<String, Integer> vValues;
//	we may need to know the v values of every attribute for every subset, which may not agree with eachother 
	private HashMap<List<String>, HashMap<String, Integer>> allVValues = new HashMap<List<String>, HashMap<String, Integer>>();
	physical.FindAttrMinMax findVals;
	
//	create instance of seperate conditions for parsing expressions
	private physical.SeparateConditions sepCon;
	
	private databaseCatalog cat = databaseCatalog.getInstance();
//	private GetStats stats = new GetStats();

//	store tables here or have functions in the seperate conditions class to 
//	HashMap<String, List<String>> tableJoins;
//	HashMap<List<String>, List<Expression>> joinExpressions;
	private Boolean aliases;
	private aliasCatalog aliasCat;
	
//	SHOULD JOIN CONDITION PARSING ALL HAPPEN HERE? 
	
	
	
//	int cost;
	
	public FindJoinOrder(List<String> tables, Expression joinExp, Boolean aliases, aliasCatalog aliasCat){
		this.findVals= new physical.FindAttrMinMax(aliases, aliasCat);
//		set lower and upper bounds on each attribute according to joinExp
		joinExp.accept(findVals);
//		take initial v-values from FindAttrMinMax class
//		with no joins v values are their initival values 
		this.allVValues.put(new ArrayList<String>(), findVals.getVValues());
		this.aliases=aliases;
		this.aliasCat=aliasCat;
		
		try {
//			put stats info in mapping to be accessed later
			BufferedReader statsReader = new BufferedReader(new FileReader(Interpreter.getInputDir()+"/db/stats.txt"));
			String line;
			ArrayList<String> splitLine;
			while ((line=statsReader.readLine())!=null) {
				splitLine = new ArrayList<String>(Arrays.asList(line.split(" ")));
				String temp = splitLine.get(0);
				splitLine.remove(0);
				this.tableStats.put(temp, splitLine);
			}
			
			statsReader.close();
			
//			create a separateConditions instance to create mapping of tables to expressions
			this.sepCon = new physical.SeparateConditions();
			if (joinExp != null){
				joinExp.accept(sepCon);
			}
////			selectConditions maps a table name to the expression associated with it
//			this.selectConditions = sepCon.getSelectConditions();
//
////			joinConditions maps a pair of table names to a join expression 
//			joinConditions = sepCon.getJoinConditions();
			
		} catch (Exception e) {
			System.err.println("Exception occurred reading stats file");
			e.printStackTrace();
		}
		
	}
	
	
	/** findBestPlan(List<String> tables) calculates the ideal join order for tables
	 * @param tables is a list of tables being joined
	 * @return the ideal join order of the tables
	 */
	// iterating over subsets is adapted from: https://www.geeksforgeeks.org/finding-all-subsets-of-a-given-set-in-java/ 
	public List<String> findBestPlan(List<String> tables){
		List<String> tempTables;
		ArrayList<String> newTables = new ArrayList<String>(tables);

		java.util.Collections.sort(newTables);

//		shift bit by one every iteration
        for (int i = 1; i < (1<<tables.size()); i++) 
        { 
//			create a new empty list 
        	tempTables = new ArrayList<String>();
//			add table names to tempTables if the corresponding outer loops bits and inner loops bit are both 1
        	for (int j = 0; j < newTables.size(); j++) 
        		if ((i & (1 << j)) > 0) {
        			tempTables.add(newTables.get(j));
                }
//          run cost calculations on the subset represented by tempTable 
        	calculateCosts(tempTables);
        } 
        
        return getOrder(newTables);
	}
	
	/** getOrder(List<String> tables) assumes that the tables setOrders is filled out and recurses back to retrieve the join order 
	 * @param tables
	 * @return
	 */
	private List<String> getOrder(List<String> tables){
		if (tables.size()==1) {
			return tables;
		}
		
		List<String> order = new ArrayList<String>();
		String lastTable = setOrders.get(tables);
		order.add(lastTable);
		
		ArrayList<String> newTables = new ArrayList<String>(tables);
		newTables.remove(lastTable);
		order.addAll(getOrder(newTables));
		
		return order;
	}
	
	
	/**calculateCost(List<String> tables) returns the ideal join cost for tables, and stores
	 * ideal join order, cost, and size in global maps setOrders, setCosts, and setSizes 
	 * @param tables is the list of which tables need to be joined
	 * @return the optimal cost of joining the given subset of tables 
	 */
	private int calculateCosts(List<String> tables){
//		put tables in alphabetical order for calculation costs
		java.util.Collections.sort(tables);
		
//		if this cost is already calculated, return
		if (setOrders.containsKey(tables)) {
			return setCosts.get(tables);
		}
		
//		if one relation set cost to 0, don't change tables
		if (tables.size()==1) {
			setCosts.put(tables,0);
			allVValues.put(tables, findVals.getVValues());
			return 0;
		}
		
//		if two relations, put smaller first and set cost to 0
		if (tables.size()==2) {
//			put smaller relation first
			setCosts.put(tables, 0);
			Integer size0 = Integer.parseInt(tableStats.get(tables.get(0)).get(0));
			Integer size1 = Integer.parseInt(tableStats.get(tables.get(1)).get(0));
			
			/// !!! DO WE NEED TO CALCULATE V_VALUE SIZE FOR BOTH TABLES, JOIN?
			
			setSizes.put(tables, size0+size1);
			
			Expression tempJoinExp;
//			int netJoinVal;
//			int intermediateJoinVal=0;
//			initialize FindJoinVValues class with old v values for this subset of tables

			FindJoinVValues joinVals = new FindJoinVValues(findVals.getVValues(), aliases, aliasCat);
//			for each table that the new table can join with, update v-values
			
			List<String> tableKey = new ArrayList<String>();
			if (aliases) {
				for (String t : tables) {
					tableKey.add(aliasCat.getTableAliases().get(t));
				}
			}else {
				tableKey=tables;
			}
			
			
			tempJoinExp = sepCon.getJoinConditions().get(tableKey);
			
//				take v values for subset of tables
			
			if (tempJoinExp != null) {
//					process joinExp to update v values associated with t and joinTable 
//					return the v value of this part of the join
//					when creating join v-values, every v value in an equijoin should be replaced with lower of two values
				tempJoinExp.accept(joinVals);
				
			}
			

			allVValues.put(tables, joinVals.getVVals());
			
			
			
//			figure out which table should be the inner relation
			if (Integer.parseInt(tableStats.get(tables.get(0)).get(0)) <= Integer.parseInt(tableStats.get(tables.get(1)).get(0))) {
				setOrders.put(tables, tables.get(1));
				return 0;
			} else {
				setOrders.put(tables, tables.get(0));
				return 0;
			}
		}
		
		
		List<String> tempTables = new ArrayList<String>(tables);
		int optCost = Integer.MAX_VALUE;
		int oldCost;
		int oldSize=0;
		int newCost=0;
		int tableSize=0;

//		find which table is cheapest to join last
		for (String t: tables) {
			tempTables.remove(t); 
//			size of joining all previous tables 
			oldCost = setCosts.get(tempTables);
			oldSize = setSizes.get(tempTables);
			
//			new cost = old cost + size of new join
//			Expression selectCondition = sepCon.getSelectConditions().get(t);

			
//			calculate cost of pairing t with every table in temp tables
			Expression tempJoinExp;
//			initialize FindJoinVValues class with old v values for this subset of tables

			FindJoinVValues joinVals = new FindJoinVValues(allVValues.get(tempTables), aliases, aliasCat);



//			for each table that the new table can join with, update v-values
			for (String joinTable : tempTables) {

				List<String> joinKey = new ArrayList<String>();
				if (aliases) {
					joinKey.add(aliasCat.getTableAliases().get(t));
					joinKey.add(aliasCat.getTableAliases().get(joinTable));
				}else {

					joinKey.add(t);
					joinKey.add(joinTable);
				}

				java.util.Collections.sort(joinKey); 
								
				tempJoinExp = sepCon.getJoinConditions().get(joinKey);
//				take v values for subset of tables

				if (tempJoinExp != null) {
//					process joinExp to update v values associated with t and joinTable 
//					return the v value of this part of the join
//					when creating join v-values, every v value in an equijoin should be replaced with lower of two values
					tempJoinExp.accept(joinVals);
				}
				
			}
						
//			IF V VALUE IS 0, ROUND UP TO 1

//			put t back to find the cost of joining the next table last
			tempTables.add(t);
			java.util.Collections.sort(tempTables);
//			store vValues for this join
			allVValues.put(tempTables, joinVals.getVVals());
//			retrieve the joinValue calculated for this join
			Integer joinVal = joinVals.joinValue;
			tableSize = Integer.parseInt(tableStats.get(t).get(0));
//			calculate join cost as size of relation 1 * size of relation 2 divided by join v-values multiplied together
			newCost = oldCost + ((oldSize * tableSize)/ joinVal);

			if (newCost<optCost) {
				optCost=newCost;
//				update ideal join cost for tables
				setCosts.put(tables, newCost);
				setSizes.put(tables, newCost);
//				update which table is joined last
				setOrders.put(tables, t);
			}

		}
			
		
		return setCosts.get(tables);
	}
	
	
//	/**
//	 * getOneTableVal(String attribute) returns the previously calculated v value for one attribute 
//	 * @param attribute is the attribute the v value is returned for 
//	 * @return the current v value of attribute 
//	 */
//	public int getOneTableVal(String attribute) {
//		return vValues.get(attribute);
//	}
	
//	/**
//	 * selectVVal(String table, Expression selectExp) calculates the v-values 
//	 * for the attributes in a given table, using the formula max-min+1 (where max/min are
//	 * the upper and lower bounds that attribute can take)
//	 * @param table the table v-values are found for
//	 * @param selectExp the selection  expression (if any) on the table 
//	 * @return List of v values for the table?
//	 */
//	public int selectVVal(String table, Expression selectExp) {
//		List<String> schema = cat.getSchema(table);
//		
//		for (String attr: schema) {
////			if attribute is not in a selection 
//			if (!selectExp.toString().contains(attr)) {
//				Integer[] bounds =  stats.getBounds(attr);
//				vValues.put(attr, (bounds[1]-bounds[0]+1));
//				
//			} else {
////				if it is in the selection condition need to update min/max! 
//			}
//		}
//		
//
//
//	}
	
//	/**
//	 * updateJoinVValues(String table, String joinTable, Expression joinExp) calculates and/or updates
//	 * the v-values for the tables in an equijoin.
//	 * 
//	 * @param table new table being joined
//	 * @param joinTable the table being joined to table (calculated on at a time)
//	 * @param joinExp is the join condition for the two tables
//	 * @return
//	 */
//	public int updateJoinVValues(String table, String joinTable, Expression joinExp) {
//		
//	}
}
