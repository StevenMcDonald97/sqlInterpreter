package physical;
/**
 * indexCondition implements the ExpressionVisitor interface to use the visitor-pattern 
 * to separate an expression into conditions on specific attributes which indexes exist for
 *  and conditions not on those attributes
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


public class indexCondition implements ExpressionVisitor {

	private String attribute;
	private String lastAttribute;
	
//	attrToBounds maps a string representation of an attribute to a 2 integer list 
//	containing the lower and upper bounds for the index on that attribute 
	private HashMap<String, Integer[]> attrToBounds = new HashMap<String, Integer[]>();

	private Integer upperBound;
	private Integer lowerBound;
	private Integer lastBound;
	
//	conditions applying to the desired attribute
//	private BinaryExpression inCondition;
//	conditions which do not apply to the desired attribute (AND-ed together) 
	private BinaryExpression otherCondition;


	/**indexConditions() instantiates an instance of the indexConditions class
	 */
	public indexCondition(String attr){
		attribute=attr;
//		inCondition=null;
		otherCondition=null;
		upperBound=Integer.MAX_VALUE;
		lowerBound=Integer.MIN_VALUE;
	}
	
	/**getAttrBounds returns attrToBounds
	 * 
	 * @return the mapping of fully-qualified attribute names to a two integer array 
	 * 	of the lower and upper bound (Integers) on that attribute
	 */
	public HashMap<String, Integer[]> getAttrBounds(){
		return attrToBounds;
	}
	
//	/** updateInCondition either sets inCondition, if it doesn't exist yet, to the current 
//	 * expression or creates a new expression by and-ing the old inCondition with the given
//	 * expression
//	 * @param e is the expression appended to inCondition
//	 */
//	public void updateInCondition(BinaryExpression e) {
//		if (inCondition==null) {
//			inCondition=e;
//		} else {
//			AndExpression newEx = new AndExpression(inCondition, e);
//			inCondition=newEx;
//		}
//	}
	
	/** updateOtherCondition either sets otherCondition, if it doesn't exist yet, to the current 
	 * expression or creates a new expression by and-ing the old otherCondition with the given
	 * expression
	 * @param e is the expression appended to otherCondition
	 */
	public void updateOtherCondition(BinaryExpression e) {
		if (otherCondition==null) {
			otherCondition=e;
		} else {
			AndExpression newEx = new AndExpression(otherCondition, e);
			otherCondition=newEx;
		}
	}
	
//	/**getInConditions() returns the conditions on the current attribute
//	 * @return expression of all conditions on attribute
//	 */
//	public Expression getInConditions(){
//		return inCondition;
//	}
	
	/**getOtherConditions() returns conditions not on current attribute
	 * @return an expression of all conditions not on current attribute
	 */
	public Expression getOtherConditions(){
		return otherCondition;
	}
	
	/**getLowerbound() returns the int representing the lower bound on the desired
	 * attribute
	 * 
	 * @return the lower bound for the index on the attribute
	 */
	public Integer getLowerbound() {
		return lowerBound;
	}
	
	/**getUpperbound() returns the int representing the upper bound on the desired
	 * attribute
	 * 
	 * @return the upper bound for the index on the attribute
	 */
	public Integer getUpperbound() {
		return upperBound;
	}
	
	/** visit checks if the current column applies to the desired attribute
	 * @param arg0 is the column expression being checked against the attribute condition
	 */
	public void visit(Column arg0){
		lastAttribute = arg0.toString();

	}
	
	public void visit(LongValue arg0){
		lastAttribute=null;
		lastBound = (int) arg0.toLong();

	}
	
	public void visit(DoubleValue arg0){
		lastAttribute=null;
		lastBound = (int) arg0.toDouble();

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
		Integer bound1 = lastBound;
		
		Expression right = arg0.getRightExpression(); 
		right.accept(this);
		String attr2 = lastAttribute;
		Integer bound2 = lastBound;

		if ((attr1==null && attr2.equals(attribute))){
			lowerBound = bound1;
			upperBound = bound1;
//			Integer[] currentBounds = {bound1, bound1};
//			attrToBounds.put(attr2, currentBounds);
			
		} else if (attr2==null && attr1.equals(attribute)) {

			lowerBound = bound2;
			upperBound = bound2;
//			Integer[] currentBounds = {bound2, bound2};
//			attrToBounds.put(attr1, currentBounds);
		} else {
			updateOtherCondition(arg0); 
		}

	}

	@Override
	public void visit(GreaterThan arg0) {
		Expression left = arg0.getLeftExpression();
		left.accept(this);
		String attr1 = lastAttribute;
		Integer bound1 = lastBound;
		
		Expression right = arg0.getRightExpression(); 
		right.accept(this);
		String attr2 = lastAttribute;
		Integer bound2 = lastBound;

		if ((attr1==null && attr2.equals(attribute))){
			upperBound = bound1+1;
//			Integer[] currentBounds = {bound1, bound1};
//			attrToBounds.put(attr2, currentBounds);
			
		} else if (attr2==null && attr1.equals(attribute)) {
			lowerBound = bound2+1;
//			Integer[] currentBounds = {bound2, bound2};
//			attrToBounds.put(attr1, currentBounds);
		} else {
			updateOtherCondition(arg0); 
		}
	}

	@Override
	public void visit(GreaterThanEquals arg0) {
		Expression left = arg0.getLeftExpression();
		left.accept(this);
		String attr1 = lastAttribute;
		Integer bound1 = lastBound;
		
		Expression right = arg0.getRightExpression(); 
		right.accept(this);
		String attr2 = lastAttribute;
		Integer bound2 = lastBound;


		if ((attr1==null && attr2.equals(attribute))){
			upperBound = bound1;
//			Integer[] currentBounds = {bound1, bound1};
//			attrToBounds.put(attr2, currentBounds);
			
		} else if (attr2==null && attr1.equals(attribute)) {
			lowerBound = bound2;
//			Integer[] currentBounds = {bound2, bound2};
//			attrToBounds.put(attr1, currentBounds);
		} else {
			updateOtherCondition(arg0); 
		}
	}

	@Override
	public void visit(MinorThan arg0) {
		Expression left = arg0.getLeftExpression();
		left.accept(this);
		String attr1 = lastAttribute;
		Integer bound1 = lastBound;
		
		Expression right = arg0.getRightExpression(); 
		right.accept(this);
		String attr2 = lastAttribute;
		Integer bound2 = lastBound;


		if ((attr1==null && attr2.equals(attribute))){
			lowerBound = bound1-1;
//			Integer[] currentBounds = {bound1, bound1};
//			attrToBounds.put(attr2, currentBounds);
			
		} else if (attr2==null && attr1.equals(attribute)) {
			upperBound = bound2-1;
//			Integer[] currentBounds = {bound2, bound2};
//			attrToBounds.put(attr1, currentBounds);
		} else {
			updateOtherCondition(arg0); 
		}
	}

	@Override
	public void visit(MinorThanEquals arg0) {
		Expression left = arg0.getLeftExpression();
		left.accept(this);
		String attr1 = lastAttribute;
		Integer bound1 = lastBound;
		
		Expression right = arg0.getRightExpression(); 
		right.accept(this);
		String attr2 = lastAttribute;
		Integer bound2 = lastBound;


		if ((attr1==null && attr2!=null)){
			lowerBound = bound1;
//			Integer[] currentBounds = {bound1, bound1};
//			attrToBounds.put(attr2, currentBounds);
			
		} else if (attr2==null && attr1!=null) {
			upperBound = bound2;
//			Integer[] currentBounds = {bound2, bound2};
//			attrToBounds.put(attr1, currentBounds);
		} else {
			updateOtherCondition(arg0); 
		}
	}

	@Override
	public void visit(NotEqualsTo arg0) {
		// Do nothing, NotEquals cannot be handled by a tree index
		updateOtherCondition(arg0); 

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
