package physical;
/** An instance of SeperateConditions separates parts of an expression into subexpressions by table name(s)
 */
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import java.util.HashMap;
import java.util.List;

import main.Interpreter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;


public class SeparateConditions implements ExpressionVisitor{
//	two possibilities: expression return a table name or null (i.e. not a column) 
//	tableName is used to track the current table associated with a column expression
	public String tableName;
//	selectConditions maps a table name to the expression associated with it
	private HashMap<String, BinaryExpression> selectConditions = new HashMap<String, BinaryExpression>();
//	joinConditions maps a pair of table names to a join expression 
	private HashMap<List<String>, BinaryExpression> joinConditions = new HashMap<List<String>, BinaryExpression>();
//	a map to map tables to other tables they join with
	private HashMap<String, List<String>> tablesJoined = new HashMap<String, List<String>>();
	
	/**SeperateConditions() instantiates an instance of the SeperateConditions class and reads in the data from the stat.txt file
	 */
	public SeparateConditions(){
	}
	
	
	/**getSelectConditions() returns the mapping of tables to select expressions 
	 * (note: all conjunctions right now are ANDs)
	 * @return map of tables to select expressions
	 */
	public HashMap<String, BinaryExpression> getSelectConditions(){
		return selectConditions;
	}
	
	/**getJoinConditions() returns the mapping of table pairs to join expressions
	 *(note: all conjunctions right now are ANDs)
	 * @return map of a pair of tables to their join expressions
	 */
	public HashMap<List<String>, BinaryExpression> getJoinConditions(){

		return joinConditions;
	}
	
	
//	functions to compare which table each side of a binary expression refers to
//	All compareTable functions check which table or tables an expression refers to 
	
	
	
	public void visit(Column arg0){
		tableName=arg0.getTable().getName();

	}
	
	public void visit(LongValue arg0){
		tableName=null;
	}
	
	public void visit(DoubleValue arg0){
		tableName=null;
	}
	
//	for And expressions don't change any mappings
	public void visit(AndExpression arg0){
		Expression left = arg0.getLeftExpression();
		left.accept(this);
		Expression right = arg0.getRightExpression(); 
		right.accept(this);
	}
	
	
	@Override
	public void visit(EqualsTo arg0) {
		// TODO Auto-generated method stub
		compareTable(arg0);
	}

	@Override
	public void visit(GreaterThan arg0) {
		// TODO Auto-generated method stub
		compareTable(arg0);
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		// TODO Auto-generated method stub
		compareTable(arg0);
	}

	@Override
	public void visit(MinorThan arg0) {
		// TODO Auto-generated method stub
		compareTable(arg0);
	}

	@Override
	public void visit(MinorThanEquals arg0) {
		// TODO Auto-generated method stub
		compareTable(arg0);
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		// TODO Auto-generated method stub
		compareTable(arg0);
	}
	
	/**compareTable(BinaryExpression arg0) looks at the two sides of arg0, and if they correspond to the same
	 * table adds that expression to the expression mapped to that table in selectConditions, or adds 
	 * a new mapping. If they disagree then the same thing is done by adding a list of the two tables name to 
	 * joinConditiosn mapped to the expression.
	 * @param arg0 the expression being evaluated
	 */
	public void compareTable(BinaryExpression arg0){
//		make sure arg0 contains a binaryExpression 
		if (arg0 != null){
			Expression left = arg0.getLeftExpression();
			left.accept(this);
			String table1 = tableName;
			Expression right = arg0.getRightExpression(); 
			right.accept(this);
			String table2 = tableName;
			
			//	if the two side of the binary expression arg0 refer to the same table, create a select expression
			//	note: in the case where table1 and table2 are null, its fine to allow that expression to be appended 
			//	to any table's expression because that must hold for all conditions regardless
			if ((table1==null || table2==null || table1.equals(table2))){

				String tTemp;
				//	find name of table as tTemp
				if (table1 != null){
					tTemp = table1;
				} else{
					tTemp=table2;
				}
				

				//	if tTemp is not in selectConditions yet add it 
				if (!selectConditions.containsKey(tTemp)){
					selectConditions.put(tTemp, arg0);

				} else {
					//	create a new expression from the existing expression associated with tTemp and arg0
					BinaryExpression oldEx = selectConditions.get(tTemp);
					AndExpression newEx = new AndExpression(oldEx, arg0);
					selectConditions.put(tTemp, newEx);
				}
	
				
				//	if there are different tables on each side, then create a join expression
			} else if(!table1.equals(table2)){

				//	need to check both orders of table pairs for the key
				List<String> joinKey = Arrays.asList(table1, table2);
//				List<String> flipJoinKey = Arrays.asList(table2, table1);
//				List<String> key = Arrays.asList();
				java.util.Collections.sort(joinKey);
				
				//	figure out which order of pairs the existing key (if it exists) is in
				//	or add a new key to joinConditions
//				if (joinConditions.containsKey(joinKey)){
//					key = joinKey;
//				} else if (joinConditions.containsKey(flipJoinKey)){
//					key = flipJoinKey;
//				} else {
//					joinConditions.put(joinKey, arg0);
//					List<String> newTableList = new ArrayList<String>();
//
//					tablesJoined.put(table1, newTableList);
//				}
				if (!joinConditions.containsKey(joinKey)){
					joinConditions.put(joinKey, arg0);
					List<String> newTableList = new ArrayList<String>();
					tablesJoined.put(table1, newTableList);
					tablesJoined.put(table2, newTableList);

				} else {
					//	if key is in joinConditions, then create a new expression from the existing 
					//	expression associated with key and arg0
					BinaryExpression oldEx = joinConditions.get(joinKey);
					AndExpression newEx = new AndExpression(oldEx, arg0);
					joinConditions.put(joinKey, newEx);
				}
//				add tables to eacho9ther join lists
				List<String> oldTable1 = tablesJoined.get(table1);
				oldTable1.add(table2);
				tablesJoined.put(table1, oldTable1);
				List<String> oldTable2 = tablesJoined.get(table2);
				oldTable1.add(table1);
				tablesJoined.put(table1, oldTable2);
				
	

			}
		}
	}
	
	//*****************************************************************
	//************* BELOW NOT IMPLEMENTED IN THIS PART ****************
	//*****************************************************************
	
	@Override
	public void visit(NullValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Function arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(InverseExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(JdbcParameter arg0) {
		// TODO Auto-generated method stub
		
	}

	
	@Override
	public void visit(DateValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimeValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(TimestampValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Parenthesis arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(StringValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Addition arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Division arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Multiplication arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Subtraction arg0) {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void visit(Between arg0) {
		// TODO Auto-generated method stub
		
	}
	
	
	@Override
	public void visit(InExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(IsNullExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LikeExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(CaseExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(WhenClause arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(ExistsExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Concat arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Matches arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseAnd arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseOr arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(BitwiseXor arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(OrExpression arg0) {
		// TODO Auto-generated method stub
		Expression left = arg0.getLeftExpression();
		left.accept(this);
		Expression right = arg0.getRightExpression(); 
		right.accept(this);
	}

}
