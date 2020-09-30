package main;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import logical.*;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Union;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import java.util.HashMap;
import net.sf.jsqlparser.expression.BinaryExpression;


/**
 * class that constructs the relational algebra tree
 *
 */
public class LogicalTreeMaker implements SelectVisitor {
	private OperatorTree root;
	private HashMap<String, BinaryExpression> selectConditions;
	private HashMap<List<String>, BinaryExpression> joinConditions;
	private Boolean aliases = false;
	private UnionFind uf;
//	map table names to their corresponding aliases
//	private HashMap<String,String> tableAliases = new HashMap<String,String>();
//	private HashMap<String,String> aliasTables = new HashMap<String,String>();

//	instance of the catalog storing database schema and file information
	databaseCatalog cat = databaseCatalog.getInstance();
	public aliasCatalog aliasCat;
	private String logicalPlan;
	
	
  /**
   * Build a tree based on selectBody (select statement)
   * @param plainSelect is select statement used to build the tree
   */
  public LogicalTreeMaker(PlainSelect selectBody, aliasCatalog aCat) {
	  this.aliasCat=aCat;
	  selectBody.accept(this);
  }
  
  
  /**
   * @return the root of the current operator tree
   */
  public OperatorTree getRoot() {
	  return root;
  }
  

  
  /**
   * Builds a tree from the plainselect.
   * @param plainSelect is the select statement
   */
  @Override
  public void visit(PlainSelect plainSelect) {
	  
//		get the where expression
		Expression exp = plainSelect.getWhere();
		logicalPlan="";
		
		//******************************************************************************
		//Use UnionFindVisitor to get UnionFind structure
		UnionFindVisitor visitor = new UnionFindVisitor();
		if (exp != null) {
			exp.accept(visitor);
		}
		uf = visitor.uf;
		
		
		String joinLine="---Join[";
		for (Expression ep : uf.unused) {
			joinLine=joinLine+" "+ep.toString();
		}
		joinLine=joinLine+"]\n";
		

		
		HashMap<Set<String>, List<Integer>> collection = uf.collection;
		for (Set<String> keys: collection.keySet()){
			String unionFindLine = "[";
//			System.out.println("Attributes");
			String attributesList = "[";
			
			for (String k:keys){
//				System.out.print(k+" ");
				attributesList=attributesList+" "+k;
			}
			attributesList=attributesList+"]";
			unionFindLine = unionFindLine+attributesList;
			unionFindLine = unionFindLine+", equals "+collection.get(keys).get(2)+", min "+collection.get(keys).get(0)+", max "+collection.get(keys).get(1)+"]\n";
			
//			System.out.println("Bounds");
//			for (int bounds: collection.get(keys)){
//				
////				System.out.print(bounds+" ");
//			}
//			System.out.println();
//			System.out.println(unionFindLine.replace("-1", "null"));
			joinLine=joinLine+(unionFindLine.replace("-1", "null"));

		}


//		logicalPlan
		
//		---Join[R.H <> B.D]
//				[[S.B, R.G], equals null, min null, max null]
//				[[S.A, B.D], equals null, min null, max null]
//				[[R.H], equals null, min null, max 99]
//		
		
		
//		create a separateConditions instance to create mapping of tables to expressions
		physical.SeparateConditions sepCon = new physical.SeparateConditions();
		
//		if a where clause is specified
		if (exp != null){
			exp.accept(sepCon);
		}

//		selectConditions maps a table name to the expression associated with it
		selectConditions = sepCon.getSelectConditions();


//		joinConditions maps a pair of table names to a join expression 
		joinConditions = sepCon.getJoinConditions();
		
//		retrieve the table names in the query (firstTable is parsed by JSQLparser separately so its retrieved first)
		Table firstTable = (Table) plainSelect.getFromItem();
		String firstTableName = firstTable.getName();
		String firstTableAlias = firstTable.getAlias();

		if(firstTableAlias!=null){
			aliasCat.addToTableAliases(firstTableName, firstTableAlias);
			aliasCat.addToAliaseTables(firstTableAlias, firstTableName);
			aliases = true;
		}
		
		List<Join> allTables = plainSelect.getJoins();
		
		List<SelectItem> items = plainSelect.getSelectItems();
		
		List<OrderByElement> order = plainSelect.getOrderByElements();
		
	
		
//		convert to list of strings
		List<String> stringOrder = new ArrayList<String>();
		if (order != null) {
			for (OrderByElement o : order){
				stringOrder.add(o.toString());
			}
		}

//		create an operator for the first table
		Expression exp1;
		
//		nextTable keeps track of next table to visit, prevTable keeps track of the previous table visited
		String nextTable;
		String prevTable;
		if (aliases){
			exp1 = selectConditions.get(firstTableAlias);
			prevTable = firstTableAlias;

		} else {
			exp1 = selectConditions.get(firstTableName);
			prevTable = firstTableName;
		}
		
//		keep track of current operator being constructed
		OperatorTree current;

		//*****************************************************************
		//************* CREATE FIRST tableop ****************
		//*****************************************************************
		OperatorTree firstOp = buildTableOp(firstTableName, exp1, uf);

//		set parent to the current operator
		root = firstOp;
		current = firstOp;


		//*****************************************************************
		//************* CREATE JOINS ****************
		//*****************************************************************

		if (allTables != null){
			logicalPlan = joinLine+logicalPlan;

//			create list to calculate join order
			List<String> tables = new ArrayList<String>();
			tables.add(firstTable.getName());


//			add all tables names to a list
			for (Join nextJoin : allTables){
				Table currentTable=(Table) nextJoin.getRightItem();
				tables.add(currentTable.getName());

//				store table aliases
				if(aliases){
					String tableName = currentTable.getName();
					String tableAlias = currentTable.getAlias();
					aliasCat.addToTableAliases(tableName, tableAlias);
					aliasCat.addToAliaseTables(tableAlias, tableName);		
				}
			}
			
//			create the ideal join order

			FindJoinOrder getJoinOrder = new FindJoinOrder(tables, exp, aliases, aliasCat);

			List<String> joinOrder = getJoinOrder.findBestPlan(tables);

//			create a list to store the join children
			List<OperatorTree> children = new ArrayList<OperatorTree>();
//			instantiate children list to be null
			for(int i=0; i<tables.size(); i++) {
				children.add(null);
			}
			
			int firstIndex = joinOrder.indexOf(firstTable.getName());
			children.set(firstIndex, firstOp);


//			add children operators at their corresponding place in the children list, determined from the 
//			ideal join order
			for (Join nextJoin : allTables){

				Table currentTable=(Table) nextJoin.getRightItem();
				if(aliases){
					aliasCat.addToTableAliases(currentTable.getName(), currentTable.getAlias());
					aliasCat.addToAliaseTables(currentTable.getAlias(), currentTable.getName());
					nextTable = currentTable.getAlias();
				}	else {
					nextTable = currentTable.getName();
				}
							
//				create an operator for each table in allTables
				Expression expNext = selectConditions.get(nextTable.toString());

				OperatorTree nextOp = buildTableOp(currentTable.getName(), expNext, uf);
				int secondIndex = joinOrder.indexOf(currentTable.getName());

				children.set(secondIndex, nextOp);
			}

//			create a logical join op with a list of children and a list of tables name in ideal join order
			logical.JoinOperator joinOp = new logical.JoinOperator(null, exp, children, tables, joinOrder, joinConditions);
			current.setRoot(joinOp);
			
//			assign parent operator (join) for children 
			for (OperatorTree child: children){
				child.setRoot(joinOp);
			}
			
			root = joinOp;	
			current = root;
	
		}
		
		//*****************************************************************
		//************* CREATE PROJECTION ****************
		//*****************************************************************
				
		if (items.get(0).toString() != "*"){
//			get the schema for the current table
			logical.ProjectOperator project = new logical.ProjectOperator(null,current,items);
			current.setRoot(project);
			root = project;
			current=root;
			String projectLine="--Project[";
			for (SelectItem s : items) {
				projectLine=projectLine+" "+s.toString();
			}
			projectLine = projectLine+"]\n";
			logicalPlan = projectLine+logicalPlan;

		}
		
		//*****************************************************************
		//************* CREATE SORT ****************
		//*****************************************************************
				
		if (order != null){
			
			List<String> currentSchema = current.getSchema();
			
			for (String field : currentSchema){
				if (!stringOrder.contains(field)){
					stringOrder.add(field);
				}
			}
			
			logical.SortOperator sort= new logical.SortOperator(null, stringOrder);
			sort.setLeft(current);
			sort.setSchema(current.getSchema());
			current.setRoot(sort);
			root=sort;
			current = root;
			
			String sortLine = "-Sort[";
			for (OrderByElement o : order) {
				sortLine = sortLine+" "+o.toString();
			}
			sortLine = sortLine+"]\n";

			logicalPlan = sortLine+logicalPlan;

			
		} else {
			logicalPlan = "Sort[] \n"+logicalPlan;

		}
		
		//*****************************************************************
		//************* CREATE DISTINCT ****************
		//*****************************************************************
//		always add a distinct operator
		if (plainSelect.getDistinct() != null){
			logical.DistinctOperator dist = new logical.DistinctOperator(null);
			dist.setLeft(current);
			dist.setSchema(current.getSchema());
			current.setRoot(dist);
			root=dist;
			current = root;
			logicalPlan = "DupElim\n"+logicalPlan;
		}
//		System.out.println(logicalPlan);
	  
  }
  

  /**operator buildTableOp(String table, List<SelectItem> items, Expression ex) creates a one branch logical 
   * expression tree for only the current table
	 * @param table is the table associated with the operators
	 *  @param ex is the select expression (if any) for this table
	 *  @return a  logicalOperator with a table op and optional select op, with the select as the root of table
	 */
	public OperatorTree buildTableOp(String table, Expression ex, UnionFind uf){
		String tableLine = "";
		OperatorTree last;
//		variables for tracking the current column in items
		String alias="";
		ArrayList<String> schema;
		ArrayList<String> newSchema = new ArrayList<String>();

//		find schema for table
		schema = cat.getSchema(table);
		
//		if table is an alias, replace it with the table name
		if (aliases){
			alias=aliasCat.getTableAliases().get(table);
//			alias=tableAliases.get(table);

//			replace select item names with 
			if(aliases){

				for (int i=0;i<schema.size();i++){
					newSchema.add(schema.get(i).replace(table, alias));
				}
			} 
			schema = newSchema;

		}
		tableLine = "----Leaf["+table+"]\n";
		
//				----Select[R.H <= 99]
//						-----Leaf[Reserves]
//		always create a table operator with no root yet
		TableOperator t = new TableOperator(null, table, schema);
		last = t;
		
		
		
//		//********************************* UNION FIND ************************************
//		loop through table attributes and find all unionfinds on this table
//		for any expression in unusable which only includes the current table, add to expression
		
		List<Expression> unused = uf.unused;
		Expression selectExp=ex;
		String oldSelect="";
		if (aliases) {
			Expression tempExp = selectConditions.get(alias);
			if (tempExp!=null) oldSelect=tempExp.toString();
		}else {
			Expression tempExp = selectConditions.get(table);
			if (tempExp!=null) oldSelect=tempExp.toString();		
		}

//		add any unused expression that shows up in the original select 
		for (Expression e: unused) {
			if (oldSelect.contains(e.toString())) {
//				if selectExp is uninitialized, then set it to current expression
				if (selectExp == null) {
					selectExp=e;
				} else {
					selectExp = new AndExpression(selectExp, e);
				}
			}
		}
		
		String schemaName = table;
		if (aliases) {
			schemaName = aliasCat.getTableAliases().get(table);
		}
//	============CREATE NEW EXPRESSION FROM UNIONFIND FOR GIVEN TABLE========================== 
		Table tab = new Table(null, null);
		tab.setName(schemaName);
//		tab.setAlias(schemaName);
		List<Integer> ufBounds;	
		
		for(String attr : schema) {
			ufBounds= uf.findBounds(attr);

//			create a column expression for this attribute
			String[] attribute = attr.split("\\.");

			Column attrCol = new Column(tab, attribute[1]);
//			for each bound if it exists create a condition
//			first bound is minimum
			if (ufBounds.get(1) != -1) {
				LongValue bound = new LongValue(""+ufBounds.get(1));
				MinorThanEquals met = new MinorThanEquals();
				met.setLeftExpression(attrCol);
				met.setRightExpression(bound);
				if (selectExp == null) {
					selectExp=met;
				} else {
					selectExp = new AndExpression(selectExp, met);
				}
			} 
//			next bound is maximum
			if (ufBounds.get(0) != -1) {
				LongValue bound = new LongValue(""+ufBounds.get(0));
				GreaterThanEquals get = new GreaterThanEquals();
				get.setLeftExpression(attrCol);
				get.setRightExpression(bound);
				if (selectExp == null) {
					selectExp=get;
				} else {
					selectExp = new AndExpression(selectExp, get);
				}
			} 
//			last bound is equality
			if (ufBounds.get(2) != -1) {
				LongValue bound = new LongValue(""+ufBounds.get(2));
				EqualsTo equals = new EqualsTo();
				equals.setLeftExpression(attrCol);
				equals.setRightExpression(bound);
				if (selectExp == null) {
					selectExp=equals;
				} else {
					selectExp = new AndExpression(selectExp, equals);
				}
			} 
		}


//	==================END UNION FIND PROCESSING========================== 

		
//		//iterate through the UnionFind and create UnionSelectOperators
//		for (Set<String> atts: uf.collection.keySet()){
//			for (String temp_att: atts){
//				List<Integer> bounds = uf.collection.get(atts);
//				logical.UnionSelectOperator ufselect= new logical.UnionSelectOperator(null, temp_att, bounds.get(0), bounds.get(1), bounds.get(2));
//				out.add(ufselect);
//			}
//		}
//		//add unusable comparison expressions
//		if (uf.unused!=null)
//		for (Expression e: uf.unused){
//			logical.SelectOperator select = new logical.SelectOperator(null, e);
//			out.add(select);
//		}
		
		
		
//		if there is a selection expression associated with this table create a select operator with the 
//		tableOp as its child
		if (selectExp != null){
			logical.SelectOperator select = new logical.SelectOperator(null, selectExp);
			select.setSchema(last.getSchema());
			select.setLeft(t);
			select.table=t.getTable();
//			t's parent is the select operator
			t.setRoot(select);
			last = select;
			tableLine = "----Select["+selectExp.toString()+"]\n-"+tableLine;

		}
		logicalPlan=logicalPlan+tableLine;
		
		return last;
	}
	

	@Override
	public void visit(Union arg0) {
		// TODO Auto-generated method stub
		
	}
	
	/**
	 * writeLogicalPlan(Integer queryNum) creates and writes a text file of the logical plan constructed from a query
	 * @param queryNum is the number of the query the plan is written for
	 */
	public void writeLogicalPlan(Integer queryNum) {
		try {
			BufferedWriter statsWriter = new BufferedWriter(new FileWriter(Interpreter.getOutputDir()+"/query"+queryNum+"_logicalPlan"));
			statsWriter.write(logicalPlan);
			statsWriter.close();
		} catch (Exception e) {
			System.err.println("Exception occurred writing out logical plan file");
			e.printStackTrace();
		} 
	}
 

}