package main;
//import java.io.IOException;
import java.util.ArrayList;
//import java.util.HashMap;
import java.util.List;
import java.util.Arrays;

import logical.DistinctOperator;
import logical.JoinOperator;
import logical.OperatorTree;
import logical.ProjectOperator;
import logical.SelectOperator;
import logical.SortOperator;
import logical.TableOperator;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectItem;
import physical.sortMergeJoin;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;


public class PhysicalPlanBuilder implements LogicalVisitor {
//	private HashMap<String, BinaryExpression> selectConditions;
//	private HashMap<List<String>, BinaryExpression> joinConditions;
//	private Boolean aliases = false;
//	map table names to their corresponding aliases
//	private HashMap<String,String> tableAliases = new HashMap<String,String>();
//	instance of the catalog storing database schema and file information
	databaseCatalog cat = databaseCatalog.getInstance();
//	private OperatorTree t;
	private physical.operator op;
//	store info for reading configurationFile
	private Integer joinPageNum=5;
	private Integer sortPageNum=20;
	private aliasCatalog aliasCat;

//	variables for tacking info to print physical plan
	private String physicalPlan;
	private String leftLeaf;
	private String rightLeaf;
	private String currentLine;
	private String distinctLine;
	private String projectionLine;
	private String joinLine;
	private String tableLine;
	private String sortLine;
	private boolean seenJoin=false;
	private boolean seenSelect=false;
	private boolean aliases;

	
	
//	List<String> indexNames;
//	HashMap<String, Boolean> indexesClustered;
	
//	tuple tup;
	
	/**
	 * Initializes PhysicalPlanBuilder
	 * @param st is the statement that will be parsed into a tree
	 */
	public PhysicalPlanBuilder(OperatorTree t, aliasCatalog aCat){
//		this.t=t; 
		this.aliasCat = aCat;
//		this.physicalPlan="";
		this.distinctLine = "";
		this.projectionLine="";
		this.joinLine="";
		this.tableLine="";
		this.sortLine="";
		this.aliases=aliasCat.hasAliases;
	}

	/**
	 * 
	 * @return physical.operator
	 */
	public physical.operator getOp(){
		return op;
	}

	@Override
	public void visit(DistinctOperator d) {
		OperatorTree left = d.getLeft();
//		t=left;
		left.accept(this);
		op = new physical.DuplicateEliminationOperator(op);
//		tup=op.getNextTuple();
//		physicalPlan = "DupElim\n"+physicalPlan;
		distinctLine = "DupElim\n";
	}

	@Override
	public void visit(JoinOperator j) {	
		long ptime1=System.currentTimeMillis();

//		take the children operators of the join
		List<OperatorTree> children = j.getChildren();
		seenJoin=true;
		OperatorTree firstOp = children.get(0);
		firstOp.accept(this);

		physical.operator op1 = op;
		leftLeaf = currentLine;
//		track which tables are already joined
		List<String> tablesSeen = new ArrayList<String>();
		tablesSeen.add(firstOp.getTable());
		OperatorTree secondOp;
		physical.operator op2;	
//		store the schema of the tables in the ideal join order
		ArrayList<String> newSchema = new ArrayList<String>();
		
		String firstTable = firstOp.getTable();
		String firstAlias = aliasCat.getTableAliases().get(firstTable);
		aliases = firstAlias!=null;
		if(aliases){
			for (String schemaAttr : cat.getSchema(firstTable)){
				newSchema.add(schemaAttr.replace(firstTable, firstAlias));
			}
			firstTable = firstAlias;
		} 	else {
			newSchema.addAll(cat.getSchema(firstTable));
		}

//		for every child operator of the tree, join with previous operator(s) 

		for (int i=1; i<children.size();i++) {

			seenSelect=false;
			secondOp = children.get(i);
			secondOp.accept(this);
			op2 = op;	
			rightLeaf = currentLine;

//			System.out.println(secondOp.getTable());
			
			String newTable = secondOp.getTable();
			String tableAlias = aliasCat.getTableAliases().get(newTable);
			

//			replace select item names with 
			if(tableAlias!=null){
				for (String schemaAttr : cat.getSchema(newTable)){
					newSchema.add(schemaAttr.replace(newTable, tableAlias));
				}
				newTable = tableAlias;
			} 	else {
				newSchema.addAll(cat.getSchema(newTable));
			}


//			create join expression for current table by finding every join expression between it
//			and a table which is in the existing join
			BinaryExpression joinExp=null;
			for (String t : tablesSeen) {
//				if(aliases){
//					tableAliases.put(currentTable.getName(), currentTable.getAlias());
//					nextTable = currentTable.getAlias();
//				}	else {
//					nextTable = currentTable.getName();
//				} 
				String tTemp= t;
				if (aliases) {
					tTemp=aliasCat.getTableAliases().get(t);
				}
				
				List<String> joinKey = new ArrayList<String>();
				joinKey.add(newTable);
				joinKey.add(tTemp);
				java.util.Collections.sort(joinKey);

//				if no condition has been found yet, set to condition between current pair of tables
				if (joinExp==null) {
					joinExp = j.joinConditions.get(joinKey);
				} else {
					BinaryExpression newEx = j.joinConditions.get(joinKey);
					if (newEx != null) {
						joinExp = new AndExpression(joinExp, newEx);
					}
				}	

			}

			checkForEquijoin joinType = new checkForEquijoin();
			if (joinExp!=null) {
				joinExp.accept(joinType);
			}

			if ((joinExp!=null) && joinType.checkJoin()) {

				sortMergeJoin smj = new physical.sortMergeJoin(op1, op2, joinExp, sortPageNum);
				op = smj;
//				physicalPlan="---SMJ["+joinExp.toString()+"]\n"+physicalPlan;
				joinLine=joinLine+"---SMJ["+joinExp.toString()+"]\n";

				String sort1 = smj.leftSortLine;
				String sort2 = smj.rightSortLine;
////				only create line for left table if it has not been made before
////				if (i==1) physicalPlan=physicalPlan+sort1+leftLeaf;
////				physicalPlan=physicalPlan+sort2+rightLeaf;
				if (i==1) tableLine=tableLine+"---"+sort1+leftLeaf;
				tableLine=tableLine+"---"+sort2+rightLeaf;

			} else {

				op = new physical.BNLjoin(op1, op2, joinExp, joinPageNum);
//				physicalPlan="---BNLJ["+joinExp+"]\n"+physicalPlan;
				joinLine=joinLine+"---BNLJ["+joinExp+"]\n";
				if (i==1) tableLine=tableLine+leftLeaf;
				tableLine=tableLine+rightLeaf;
			}

//			if (joinNum==1){
//				op = new physical.BNLjoin(op1, op2, joinExp, joinPageNum);
//			} else if (joinNum==2){
//				op = new physical.sortMergeJoin(op1, op2, joinExp, sortNum, sortPageNum);
//			} else {
//				op = new physical.joinOperator(op1, op2, joinExp);
//			}
			
//			update the outer operator and which tables have already been joined together
			tablesSeen.add(secondOp.getTable());
			op1=op;

			op1.reset();
			
		}
		
//		create original table schema
		ArrayList<String> oldSchema = new ArrayList<String>();

		for (String oldTable : j.getOriginalOrder()) {
			String oldAlias = aliasCat.getTableAliases().get(oldTable);
			if(oldAlias!=null){
				for (String schemaAttr : cat.getSchema(oldTable)){
					oldSchema.add(schemaAttr.replace(oldTable, oldAlias));
				}
			} 	else {
				oldSchema.addAll(cat.getSchema(oldTable));
			}
		}
		

//		project the original schema onto the reorganized table
		op = new physical.projectionOperator(op, oldSchema, newSchema);
		long ptime2=System.currentTimeMillis();
		System.out.println("	build join: " + (ptime2-ptime1));
		
	}

	@Override
	public void visit(ProjectOperator p) {

		OperatorTree left = p.getLeft();
		left.accept(this);
		physical.operator o = op;

		ArrayList<String> schema = (ArrayList<String>) op.getSchema();

		ArrayList<String> attributes = new ArrayList<String>();
		for (SelectItem i : p.selectitems) {
			attributes.add(i.toString());
		}
		op = new physical.projectionOperator(o, attributes, schema);
		
		projectionLine="--Project[";
		for (String s : attributes) {
			projectionLine=projectionLine+" "+s;
		}
		projectionLine = projectionLine+"]\n";
//		physicalPlan = projectLine+physicalPlan;

	}

	@Override
	public void visit(SelectOperator se) {
//		reset current line for tracking plan description
		seenSelect=true;
		currentLine="";
		OperatorTree r = se.getLeft();
		r.accept(this);
		long ptime1=System.currentTimeMillis();

		Expression exp = se.e;
		String table = r.table.toString();
		
		String index = null;
		String tableAlias = aliasCat.getTableAliases().get(table);
		String indexAlias = null;

		//**************************CALCULATE SELECTION COSTS********************************
		CalculateSelection cs = new CalculateSelection(table, se.getSchema());
		
		int scan_cost = cs.calcScan();
		int opt_cost = scan_cost;
//		System.out.println("scan: "+scan_cost);
		physical.FindAttrMinMax finder = new physical.FindAttrMinMax(true, aliasCat);
		se.e.accept(finder);
		
		List<String> indexNames = cat.getIndexes(table);
		if (indexNames==null) indexNames= new ArrayList<String>();
		
//		loop through all existing indexes to find best one to use
		for (String name : indexNames) {
			String tempAlias =  !aliases ? name : name.replace(table, tableAlias);
//			System.out.println(indexAlias);
			if ( exp.toString().contains(name) || exp.toString().contains(tempAlias)) {
				Integer[] bounds = (finder.getBounds(name));
//				System.out.println("clustered index: "+clustered_cost);
				
				int leaves = aliasCat.indexLeafNums.get(name);
				int cost;
				int lowerBound = (bounds[0]>bounds[1]) ? bounds[1] : bounds[0];
				int upperBound = (bounds[0]<bounds[1]) ? bounds[1] : bounds[0];
				if (aliasCat.indexClusters.get(name)) {
					cost = cs.indexScanCostClustered(cs.reductFactor(name, lowerBound, upperBound));
				} else {
					cost=cs.indexScanCostUnclustered(cs.reductFactor(name, lowerBound, upperBound), leaves);
				};
				
//				update best cost if current index is better than old
				if (cost<=opt_cost) {
					index=name;
					indexAlias=name;
					if (tableAlias!=null) {
						indexAlias= name.replace(table, tableAlias);
					} 
					opt_cost=cost;
				}


			}
		}

//		LOOP THROUGH AND FIND BEST INDEX!
		
		if (index==null || scan_cost<opt_cost){
			physical.ScanOperator o = (physical.ScanOperator) op;
			op = new physical.SelectOperator(o, se.e);
			currentLine="----Select["+se.e+"]\n"+currentLine;
			if(!seenJoin) tableLine=tableLine+currentLine;
		}
		else if (index!=null){
			Boolean clustered = aliasCat.indexClusters.get(index);
			physical.indexCondition inCondition = new physical.indexCondition(indexAlias);
			
			exp.accept(inCondition);
			Integer low = inCondition.getLowerbound();
			Integer  high=inCondition.getUpperbound();
			
			int indexPosition = se.getSchema().indexOf(indexAlias);
//			create an index scan operator on index column
			physical.IndexScan inScan = new physical.IndexScan(table, Interpreter.getInputDir()+"/db/indexes/"+index, se.getSchema(), low, high, indexPosition, clustered);
//			create select operator with remaining conditions
			Expression selectExp=inCondition.getOtherConditions();
			
			String lowVal= low==Integer.MIN_VALUE ? "null" : low+"";
			String highVal= low==Integer.MAX_VALUE ? "null" : high+"";

			currentLine="-----IndexScan["+index.split("\\.")[0]+","+index.split("\\.")[1]+","+lowVal+","+highVal+"]\n";
//			currentLine.replace("", "null");

//			check if a select operator also needs to be made
			if (selectExp!=null) {
				op = new physical.SelectOperator(inScan, selectExp);
				currentLine="----Select["+selectExp+"]\n"+currentLine;
			} else {
				op = inScan;
			}
			
			if(!seenJoin) tableLine=tableLine+currentLine;

		}
		long ptime2=System.currentTimeMillis();
		System.out.println("	build select: " + (ptime2-ptime1));
	}

	@Override
	public void visit(SortOperator so) {

		sortLine="InternalSort["+so.ol+"]\n";
		OperatorTree r = so.getLeft();
		r.accept(this);
		physical.operator o = op;
//		if (sortNum==0) {
//			op=new physical.SortOperator(o, so.ol);
//		} else {
		long ptime1=System.currentTimeMillis();

		try {
//			op=new physical.SortOperator(o, so.ol);
			op=new physical.ExternalSortOperator(o, so.ol, sortPageNum, Interpreter.getTempDir());

		} catch (Exception e) {
			System.err.println("Exception occurred while processing query");
			e.printStackTrace();
		}
//		}
		
		
//		physicalPlan = sortLine+physicalPlan;
//		currentLine=sortLine;
		long ptime2=System.currentTimeMillis();
		System.out.println("	build sort: " + (ptime2-ptime1));

		
		
	}

	@Override
	public void visit(TableOperator t) {
		ArrayList<String> schema = t.getSchema();
		physicalPlan="-----TableScan["+t.table.toString()+"]\n"+physicalPlan;
		currentLine="-----TableScan["+t.table.toString()+"]\n";
		if(!seenJoin && !seenSelect) tableLine=tableLine+currentLine;
		op=new physical.ScanOperator(t.table.toString(), schema);	
	}
	
	/**
	 * writePhysicalPlan(Integer queryNum) creates and writes a text file of the logical plan constructed from a query
	 * @param queryNum is the number of the query the plan is written for
	 */
	public void writePhysicalPlan(Integer queryNum) {
//		System.out.println(distinctLine+sortLine+projectionLine+joinLine+tableLine);
		physicalPlan = distinctLine+sortLine+projectionLine+joinLine+tableLine;
		try {
			BufferedWriter statsWriter = new BufferedWriter(new FileWriter(Interpreter.getOutputDir()+"/query"+queryNum+"_physicalPlan"));
			statsWriter.write(physicalPlan);
			statsWriter.close();
		} catch (Exception e) {
			System.err.println("Exception occurred writing out physical plan file");
			e.printStackTrace();
		} 
	}
	
	
}
