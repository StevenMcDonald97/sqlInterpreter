package physical;
import java.util.List;

import main.databaseCatalog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;

/** An instance of QueryPlanBuilder takes in a plainSelect object and constructs an operator tree
 * based off the body of the plainSelect. It always creates a scan operator to look at the table
 * in the SELECT, and then generates optional selection and projection operators. 
 */

public class QueryPlanBuilder {
	private operator parent;
	private HashMap<String, BinaryExpression> selectConditions;
	private HashMap<List<String>, BinaryExpression> joinConditions;
	private Boolean aliases = false;
//	map table names to their corresponding aliases
	private HashMap<String,String> tableAliases = new HashMap<String,String>();
//	instance of the catalog storing database schema and file information
	databaseCatalog cat = databaseCatalog.getInstance();
	
	/** QueryPlanBuilder(PlainSelect query) instantiates an instance of QueryPlanBuilder, which is a tree
	 *  of operators built from the PlainSelect query
	 */
	public QueryPlanBuilder(PlainSelect query){
//		get the where expression
		Expression exp = query.getWhere();

//		create a separateConditions instance to create mapping of tables to expressions
		SeparateConditions sepCon = new SeparateConditions();
		
//		if a where clause is specified
		if (exp != null){
			exp.accept(sepCon);
		}

//		selectConditions maps a table name to the expression associated with it
		selectConditions = sepCon.getSelectConditions();

//		joinConditions maps a pair of table names to a join expression 
		joinConditions = sepCon.getJoinConditions();
		
//		retrieve the table names in the query (firstTable is parsed by JSQLparser separately so its retrieved first)
		Table firstTable = (Table) query.getFromItem();
		String tableName = firstTable.getName();
		String tableAlias = firstTable.getAlias();
		
		if(tableAlias!=null){
			tableAliases.put(tableName, tableAlias);
			aliases = true;
		}
		
		List<Join> allTables = query.getJoins();
		
		List<SelectItem> items = query.getSelectItems();
		
		List<String> order = query.getOrderByElements();

//		create an operator for the first table
		Expression exp1;
		
//		nexTable keeps track of next table to visit, prevTable keeps track of the previous table visited
		String nextTable;
		String prevTable;
		if (aliases){
			exp1 = selectConditions.get(tableAlias);
			prevTable = tableAlias;

		} else {
			exp1 = selectConditions.get(tableName);
			prevTable = tableName;
		}

		//*****************************************************************
		//************* CREATE FIRST SCAN/SELECT ****************
		//*****************************************************************
		
		
		operator firstOp = buildTableOp(tableName, exp1);

//		set parent to the current operator
		parent = firstOp;


		//*****************************************************************
		//************* CREATE JOINS ****************
		//*****************************************************************
		
		
//		iterate through all the tables after the first (if they exist) and create the tree of join operators
		if (allTables != null){

			for (Join nextJoin : allTables){

				Table currentTable=(Table) nextJoin.getRightItem();
				
				if(aliases){
					tableAliases.put(currentTable.getName(), currentTable.getAlias());
					nextTable = currentTable.getAlias();
				}	else {
					nextTable = currentTable.getName();
				}
				
							
//				create an operator for each table allTables
				Expression expNext = selectConditions.get(nextTable.toString());

				operator nextOp = buildTableOp(currentTable.getName(), expNext);

				Expression joinExpression = joinConditions.get(Arrays.asList(prevTable, nextTable.toString()));
//				check both orders of table names in case it was inserted into joinConditions in reverse order
				if (joinExpression == null){
					joinExpression = joinConditions.get(Arrays.asList(nextTable.toString(), prevTable.toString()));
				}

//				create a join operator with the previous operators as children
				joinOperator joinOp = new joinOperator(parent, nextOp, joinExpression);

//				prevTable needs to be either name or alias of current table
				prevTable = nextTable;
				
//				set parent to the current operator
				parent = joinOp;	
			}
		}
		
		//*****************************************************************
		//************* CREATE PROJECTION ****************
		//*****************************************************************
				
		if (items.get(0).toString() != "*"){
//			get the schema for the current table
			ArrayList<String> schema = (ArrayList<String>) parent.getNextTuple().getSchema();
			parent.reset();
			System.err.println("using outdated queryplan builder, projection code has changed");
//			parent = new projectionOperator(parent, items, schema);

		}
		
		//*****************************************************************
		//************* CREATE SORT ****************
		//*****************************************************************
				
		if (order != null){
			SortOperator sort= new SortOperator(parent, order);
			parent = sort;
		}
		
		//*****************************************************************
		//************* CREATE DISTINCT ****************
		//*****************************************************************
		
		DuplicateEliminationOperator dist = new DuplicateEliminationOperator(parent);
		parent = dist;
	}
	
	
	/**operator buildTableOp(String table, List<SelectItem> items, Expression ex) creates a one branch expression tree
	 * for only the current table
	 * @param table is the table associated with the operators
	 *  @param items is the select items (if any) specifying the projection
	 *  @param ex is the select expression (if any) for this table
	 *  @return an operator performing all scan, select, and project operations on table
	 */
	public operator buildTableOp(String table, Expression ex){
		
		operator last;
//		variables for tracking the current column in items
		String alias;
		ArrayList<String> schema;
		ArrayList<String> newSchema = new ArrayList<String>();

//		find schema for table
		schema = cat.getSchema(table);
		
//		if table is an alias, replace it with the table name
		if (aliases){
			alias=tableAliases.get(table);

//			replace select item names with 
			if(aliases){

				for (int i=0;i<schema.size();i++){
					newSchema.add(schema.get(i).replace(table, alias));
				}
			} 
			schema = newSchema;

		}


//		always create a scan operator
		ScanOperator scan = new ScanOperator(table, schema);
		last = scan;

		
//		if there is a selection expression associated with this table create a select operator
		if (ex != null){
			SelectOperator select = new SelectOperator(scan, ex);
			last = select;

		}

		return last;
	}
	
	
	/** getPlan() return the parent operator which contains the entire query plan as children
	 * @return the operator containing the logic of the current query plan
	 */
	public operator getPlan(){
		return parent;
	}
	
}
