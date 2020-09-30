package main;
/** An instance of FindVValues calculates the initial upper and lower bounds for each attribute to be sued to calculate v-values
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

import main.GetStats;
import main.Interpreter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;


public class FindJoinVValues implements ExpressionVisitor{
//	map attributes to min/max bounds
	private HashMap<String, Integer> vVals;	
	private String lastAttribute;
	private String lastTable;
	public Integer joinValue = 1;
	private aliasCatalog aliasCat;
	private Boolean aliases;
	
	/**SeperateConditions() instantiates an instance of the SeperateConditions class and reads in the data from the stat.txt file
	 */
	public FindJoinVValues(HashMap<String, Integer> initValues, Boolean a, aliasCatalog aliasC){
		this.vVals=initValues;
		this.aliasCat=aliasC;
		this.aliases=a;

	}
	
	/**
	 *  getVVals() returns the map vVals of attribute names to their vValues after visiting a join condition 
	 *  For any equijoins, set both vValues to the smaller of the two
	 *  return a map of attribute names to VValues after parsing a join condition
	 */
	public HashMap<String, Integer> getVVals(){
		return vVals;
	}
	
	/**
	 * replaceAliasName(String attr) replaces an alias name with the full table name if aliases
	 * are being used
	 * @param attr the attribute name 
	 * @return attr with the full talbe name specified 
	 */
	private String replaceAliasName(String attr) {
		if (aliases) {
			String[] aliasSplit = attr.split("\\.");
			String alias=aliasSplit[0];
			String table = aliasCat.getAliasTable().get(alias);
			return attr.replace(alias, table);
		}
		
		return attr;
	}
	
	public void visit(Column arg0){
		lastAttribute=arg0.getWholeColumnName();
		lastTable=arg0.getTable().getName();
	}
	
	public void visit(LongValue arg0){
		lastAttribute=null;
		lastTable = null;

	}
	
	public void visit(DoubleValue arg0){
		lastAttribute=null;
		lastTable=null;
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
		Expression left = arg0.getLeftExpression();
		left.accept(this);
		String attr1 = lastAttribute;
		String table1 = lastTable;
		Expression right = arg0.getRightExpression(); 
		right.accept(this);
		String attr2 = lastAttribute;
		String table2 = lastTable;

//		if this is an equijoin, adjust vValues
		if (attr1 != null && attr2!=null && table1!=table2) {
			attr1=replaceAliasName(attr1);
			attr2=replaceAliasName(attr2);
			int val1 = vVals.get(attr1);
			int val2 = vVals.get(attr2);
//			newVal is the smaller of the two old values
			int newVal = ((val1<val2) ? val1 : val2);
			vVals.put(attr1, newVal);
			vVals.put(attr2, newVal);
//			update joinvalue
//			int newJoinVal = ((val1>val2) ? val1 : val2);
			joinValue = joinValue*newVal;

		}
		


	}
	/**
	 * updateVVal(BinaryExpression arg0) updates the net v-value for the join using the v values
	 * for the attribute in the given BinaryExpression
	 * @param arg0 is the expression sued to update the join v value
	 */
	public void updateVVal(BinaryExpression arg0) {
		Expression left = arg0.getLeftExpression();
		left.accept(this);
		String attr1 = lastAttribute;
		String table1 = lastTable;
		Expression right = arg0.getRightExpression(); 
		right.accept(this);
		String attr2 = lastAttribute;
		String table2 = lastTable;
		
//		if a join condition, update v value by multiply new value with old value
		if (attr1 != null && attr2!=null && table1!=table2) {
			attr1=replaceAliasName(attr1);
			attr2=replaceAliasName(attr2);

			Integer val1 = vVals.get(attr1);
			Integer val2 = vVals.get(attr2);
//			newVal is the smaller of the two old values
			Integer newVal = ((val1>val2) ? val1 : val2);
			joinValue = joinValue*newVal;
		}
	}
	
	@Override
	public void visit(GreaterThan arg0) {
		updateVVal(arg0);

	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		updateVVal(arg0);

	}


	@Override
	public void visit(MinorThan arg0) {
		updateVVal(arg0);

	}


	@Override
	public void visit(MinorThanEquals arg0) {
		updateVVal(arg0);

	}

	@Override
	public void visit(NotEqualsTo arg0) {
		updateVVal(arg0);

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
