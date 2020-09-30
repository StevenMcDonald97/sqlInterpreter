package physical;
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


public class FindAttrMinMax implements ExpressionVisitor{
//	map attributes to min/max bounds
	private HashMap<String, Integer[]> attrBounds;

//	instance of getStats for 
	GetStats stats = new GetStats();
	main.aliasCatalog aliasCat;
	private Boolean aliases;
	
	String lastAttribute;
	Integer lastValue;
	
	/**SeperateConditions() instantiates an instance of the SeperateConditions class and reads in the data from the stat.txt file
	 */
	public FindAttrMinMax(Boolean aliases, main.aliasCatalog alsCat){
		attrBounds = new HashMap<String, Integer[]>(stats.getAllBounds());
		this.aliases=aliases;
		this.aliasCat=alsCat;
	}
	
	/**
	 * getBounds(String attribute) returns the list of bounds on an attribute
	 * @param attribute the attribute bounds are found for 
	 * @return an array containing the min and max values for that attribute
	 */
	public Integer[] getBounds(String attribute) {
		return attrBounds.get(attribute);
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
			String test = new String("S.A");
			String alias=aliasSplit[0];
			String table = aliasCat.getAliasTable().get(alias);

			return (table != null) ? attr.replace(alias, table) : attr;
		}
		
		return attr;
	}
	
//	functions to compare which table each side of a binary expression refers to
//	All compareTable functions check which table or tables an expression refers to 
	
	/**
	 * getVValues() returns a map of attributes to their initial v-values (with out join conditions considered)
	 * @return a map of fully-qualified attribute names to their starting v-values before joins are considered
	 */
	public HashMap<String, Integer> getVValues(){
		HashMap<String, Integer> vVals = new HashMap<String, Integer>();
		String attr;
		Integer min;
		Integer max;
		for (HashMap.Entry<String, Integer[]> entry : attrBounds.entrySet()) {
		    attr = entry.getKey();
		    min = entry.getValue()[0];
		    max = entry.getValue()[1];

		    vVals.put(attr, max-min+1);
		}
		return vVals;
	}
	
	public void visit(Column arg0){
		lastValue = null;
		lastAttribute=arg0.getWholeColumnName();
	}
	
	public void visit(LongValue arg0){
		lastAttribute=null;
		lastValue = (int) arg0.getValue();

	}
	
	public void visit(DoubleValue arg0){
		lastAttribute=null;
		lastValue = (int) arg0.getValue();
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
		Expression left = arg0.getLeftExpression();
		left.accept(this);
		String attr1 = lastAttribute;
		Integer val1 = lastValue;
		Expression right = arg0.getRightExpression(); 
		right.accept(this);
		String attr2 = lastAttribute;
		Integer val2 = lastValue;
		
//		change max or min depending on order of comparison
		if (attr1==null){
			attr2=replaceAliasName(attr2);
			attrBounds.get(attr2)[0]=val1;
			attrBounds.get(attr2)[1]=val1;

		} else if (attr2==null) {
			attr1=replaceAliasName(attr1);
			attrBounds.get(attr1)[0]=val2;
			attrBounds.get(attr1)[1]=val2;
		} 

	}

	@Override
	public void visit(GreaterThan arg0) {
		// TODO Auto-generated method stub
		Expression left = arg0.getLeftExpression();
		left.accept(this);
		String attr1 = lastAttribute;
		Integer val1 = lastValue;
		Expression right = arg0.getRightExpression(); 
		right.accept(this);
		String attr2 = lastAttribute;
		Integer val2 = lastValue;
		
//		change max or min depending on order of comparison
		if (attr1==null){
			attr2=replaceAliasName(attr2);
			attrBounds.get(attr2)[1]=val1-1;
		} else if (attr2==null) {
			attr1=replaceAliasName(attr1);
			attrBounds.get(attr1)[0]=val2+1;
		} 

	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		// TODO Auto-generated method stub
		Expression left = arg0.getLeftExpression();
		left.accept(this);
		String attr1 = lastAttribute;
		Integer val1 = lastValue;
		Expression right = arg0.getRightExpression(); 
		right.accept(this);
		String attr2 = lastAttribute;
		Integer val2 = lastValue;
		
//		change max or min depending on order of comparison
		if (attr1==null){
			attr2=replaceAliasName(attr2);
			attrBounds.get(attr2)[1]=val1;
		} else if (attr2==null) {
			attr1=replaceAliasName(attr1);
			attrBounds.get(attr1)[0]=val2;

		}

	}


	@Override
	public void visit(MinorThan arg0) {
		// TODO Auto-generated method stub
		Expression left = arg0.getLeftExpression();
		left.accept(this);
		String attr1 = lastAttribute;
		Integer val1 = lastValue;
		Expression right = arg0.getRightExpression(); 
		right.accept(this);
		String attr2 = lastAttribute;
		Integer val2 = lastValue;

//		change max or min depending on order of comparison
		if (attr1==null){
			attr2=replaceAliasName(attr2);
			attrBounds.get(attr2)[0]=val1+1;
		} else if (attr2==null) {
			attr1=replaceAliasName(attr1);
			attrBounds.get(attr1)[1]=val2-1;

		}

	}


	@Override
	public void visit(MinorThanEquals arg0) {
		// TODO Auto-generated method stub
		Expression left = arg0.getLeftExpression();
		left.accept(this);
		String attr1 = lastAttribute;
		Integer val1 = lastValue;
		Expression right = arg0.getRightExpression(); 
		right.accept(this);
		String attr2 = lastAttribute;
		Integer val2 = lastValue;
		
//		change max or min depending on order of comparison
		if (attr1==null){
			attr2=replaceAliasName(attr2);
			attrBounds.get(attr2)[0]=val1;
		} else if (attr2==null) {
			attr1=replaceAliasName(attr1);
			attrBounds.get(attr1)[1]=val2;

		}

	}

	@Override
	public void visit(NotEqualsTo arg0) {
		// TODO Auto-generated method stub
//		ignore, doesn't change v-value
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
