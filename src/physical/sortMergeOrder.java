package physical;
/** An instance of sortMergeOrder find the order of attributes by which to sort the left 
 * and right children of a sortMergeJoin operator based on their join expression
 * We only have to consider ands, equals, and columns because we assume its an equijoin 
 */

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
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
import java.util.List;
import java.util.ArrayList;

//import java.util.Arrays;

public class sortMergeOrder implements ExpressionVisitor{
	String attribute;
	List<String> leftTables;
	List<String> rightTables;
	List<String> leftOrder = new ArrayList<String>();
	List<String> rightOrder = new ArrayList<String>();

	/**sortMergeOrder() instantiates an instance of the sortMergeOrder class with empty values
	 */
	public sortMergeOrder(List<String> left, List<String> right){
		this.leftTables=left;
		this.rightTables=right;
	}
	
	
	/**getLeftOrder() returns the sort-order list for the left table
	 * @return list of string names of columns in order to sort table by
	 */
	public List<String> getLeftOrder(){
		return leftOrder;
	}
	
	/**getrightOrder() returns the sort-order list for the Right table
	 * @return list of string names of columns in order to sort table by
	 */
	public List<String> getRightOrder(){
		return rightOrder;
	}
	
	@Override
	public void visit(AndExpression arg0) {
		// TODO Auto-generated method stub
		Expression left = arg0.getLeftExpression();
		left.accept(this);
		Expression right = arg0.getRightExpression(); 
		right.accept(this);
		
	}

	@Override
	public void visit(EqualsTo arg0) {
		// TODO Auto-generated method stub
		Expression left = arg0.getLeftExpression();
		left.accept(this);

		Expression right = arg0.getRightExpression(); 
		right.accept(this);


	}
	
	@Override
	/** when visiting a column, set the global field attribute to
	 * the whole column name and add to the appropriate sort order list
	 */
	public void visit(Column arg0) {
		// TODO Auto-generated method stub
		String tempName = arg0.getTable().getName();
//		if the names of tables being joined are not yet set, set them
//		make sure the table names are distinct
		
//		figure out if the attribute is for the left or right table
		if (leftTables.contains(tempName)) {

//			get column name
			leftOrder.add(arg0.getWholeColumnName());
			
		} else if (rightTables.contains(tempName)){
			rightOrder.add(arg0.getWholeColumnName());
		}
		
		attribute=arg0.getWholeColumnName();
		
		
	}

//	note: the sort merge join should only use equijoins, so the following 
//	comparators do not need to be considered 
	@Override
	public void visit(GreaterThan arg0) {
		// TODO Auto-generated method stub

		
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		// TODO Auto-generated method stub

		
	}
	
	@Override
	public void visit(MinorThan arg0) {
		// TODO Auto-generated method stub

		
	}

	@Override
	public void visit(MinorThanEquals arg0) {
		// TODO Auto-generated method stub

		
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		// TODO Auto-generated method stub

		
	}
	
	
	
	//****************************************************************
	//************ BELOW NOT IMPLEMENTED IN THIS PART ****************
	//****************************************************************	
	
	
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
	public void visit(DoubleValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(LongValue arg0) {
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
	public void visit(OrExpression arg0) {
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

}
