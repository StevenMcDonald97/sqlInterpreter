package physical;
/**SelectCondition implements the expressionVisitor class from JSQLParser to resolve
 * and expression down to its most simplified value
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
import java.util.ArrayList;
import java.util.List;

public class SelectionCondition implements ExpressionVisitor {
	private boolean condition;
	private double value;
	private List<Integer> tuple_body;

	ArrayList<String> schema;
	
	public SelectionCondition(ArrayList<Integer> tupleBody, ArrayList<String> s){
//		tup = t;
		condition = true;
		tuple_body= tupleBody;
		schema = s;
	}


	/**
	 * returns true or false determined after evaluating a tuple under a condition
	 * (just for access from a different class)
	 * @return boolean condition
	 */
	public boolean returnCondition(){
		return condition;
	}

	@Override
	public void visit(LongValue arg0) {
		// TODO Auto-generated method stub***********************
		value = arg0.toDouble();
		
	}
	




	@Override
	public void visit(AndExpression arg0) {
		// TODO Auto-generated method stub*********************
		Expression left = arg0.getLeftExpression();
		left.accept(this);
		boolean leftex = condition;
		Expression right = arg0.getRightExpression(); 
		right.accept(this);
		boolean rightex = condition;
		if (leftex && rightex){
			condition = true;
		}
		else{
			condition = false;
		}
	}
	
	@Override
	public void visit(OrExpression arg0) {

		// TODO Auto-generated method stub
		Expression left = arg0.getLeftExpression();
		left.accept(this);
		boolean leftex = condition;
		Expression right = arg0.getRightExpression(); 
		right.accept(this);
		boolean rightex = condition;
		if (leftex || rightex){
			condition = true;
		}
		else{
			condition = false;
		}
	}




	@Override
	public void visit(EqualsTo arg0) {

//		resolve column name in left operator to an integer
//		!!! what about if a non-column name is used?
		Expression left = arg0.getLeftExpression();
		left.accept(this);
		Double value_1 = value;
		Expression right = arg0.getRightExpression(); 
		right.accept(this);
		Double value_2 = value;

		if (value_1.equals(value_2)){
			condition = true;
		}
		else{
			condition = false;
		}
				
	}

	@Override
	public void visit(GreaterThan arg0) {

		// TODO Auto-generated method stub*****************************
		Expression left = arg0.getLeftExpression();
		left.accept(this);
		Double value_1 = value;
		Expression right = arg0.getRightExpression(); 
		right.accept(this);
		Double value_2 = value;

		if (value_1>value_2){
			condition = true;
		}
		else{
			condition = false;
		}

	}

	@Override
	public void visit(GreaterThanEquals arg0) {

		// TODO Auto-generated method stub*************************
		Expression left = arg0.getLeftExpression();
		left.accept(this);
		Double value_1 = value;
		Expression right = arg0.getRightExpression(); 
		right.accept(this);
		Double value_2 = value;

		if (value_1>=value_2){
			condition = true;
		}
		else{
			condition = false;
		}
	}


	@Override
	public void visit(MinorThan arg0) {

		// TODO Auto-generated method stub****************************
		Expression left = arg0.getLeftExpression();
		left.accept(this);
		Double value_1 = value;
		Expression right = arg0.getRightExpression(); 
		right.accept(this);
		Double value_2 = value;

		if (value_1<value_2){
			condition = true;
		}
		else{
			condition = false;
		}
	}

	@Override
	public void visit(MinorThanEquals arg0) {

		// TODO Auto-generated method stub***************************
		Expression left = arg0.getLeftExpression();
		left.accept(this);
		Double value_1 = value;
		Expression right = arg0.getRightExpression(); 
		right.accept(this);
		Double value_2 = value;

		if (value_1<=value_2){
			condition = true;
		}
		else{
			condition = false;
		}
	}

	@Override
	public void visit(NotEqualsTo arg0)  {

//		resolve column name in left operator to an integer
//		!!! what about if a non-column name is used?
		Expression left = arg0.getLeftExpression();
		left.accept(this);
		Double value_1 = value;
		Expression right = arg0.getRightExpression(); 
		right.accept(this);
		Double value_2 = value;
		
		
		if (!value_1.equals(value_2)){
			condition = true;
		}
		else{
			condition = false;
		}
				
	}

	@Override
	public void visit(Column arg0) {
		// TODO Auto-generated method stub********************8
		String column_1 = arg0.toString(); 
		Integer index_1 = schema.indexOf(column_1);
		value = (tuple_body.get(index_1));
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
	


	
}