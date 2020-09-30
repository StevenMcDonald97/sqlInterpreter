package main;

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
import java.util.*;
/**
 * visitor that walks the WHERE clause and builds a union-find
 * looks at expressions involving inequalities
 *
 */
public class UnionFindVisitor implements ExpressionVisitor {
	public UnionFind uf;
	Expression left_att;
//	Expression right_att;
	boolean equality;
	boolean greater;
	boolean minor;
	boolean greatereq;
	boolean minoreq;
	private String lastTable;
	
	public UnionFindVisitor(){
		uf = new UnionFind();
		equality=false;
		greater=false;
		minor=false;
		minoreq=false; greatereq=false;
		left_att=null;
	}
	
	/**
	 * form att1 = att2 (arg0)
	 * if dealing with equality, find corresponding elements and union them
	 */
	@Override
	public void visit(Column arg0) {
		// TODO Auto-generated method stub
		lastTable = arg0.getTable().toString();
		if (!arg0.toString().equals(left_att.toString()) && equality){
				HashMap<Set<String>, List<Integer>> el1 = uf.find(left_att.toString());
				HashMap<Set<String>, List<Integer>> el2 = uf.find(arg0.toString());

				Set<String> attributes1 = new HashSet<String>();
				for (Set<String> temp: el1.keySet()){
					attributes1=temp;
				}
				Set<String> attributes2 = new HashSet<String>();
				for (Set<String> temp: el2.keySet()){
					attributes2=temp;
				}
				
				uf.merge(attributes1, el1.get(attributes1), attributes2, el2.get(attributes2));

		}
		
	}
	
	/**
	 * unused
	 */
	@Override
	public void visit(StringValue arg0) {
		// TODO Auto-generated method stub

	}
	
	/**
	 * if dealing with equality, set constraint value for attribute to arg0
	 * if dealing with greater, set lower value
	 * if dealing with minor, set upper value
	 * Note: if -1 is passed in, that bound is not changed in UnionFind
	 */
	@Override
	public void visit(LongValue arg0) {
		// TODO Auto-generated method stub
		lastTable=null;
		if (equality) uf.setValues(left_att.toString(), (int)arg0.toLong(), (int)arg0.toLong(), (int)arg0.toLong());
		else if (greater){
			uf.setValues(left_att.toString(), (int)(arg0.toLong()+1), -1, -1);
		}
		else if (greatereq){
			uf.setValues(left_att.toString(), (int)arg0.toLong(), -1, -1);
		}
		else if (minor){
			
			uf.setValues(left_att.toString(), -1, (int)(arg0.toLong()-1), -1);
		}
		else if (minoreq){
			uf.setValues(left_att.toString(), -1, (int)arg0.toLong(), -1);
		}
		
	}
	
	/**
	 * arg0 is an EqualsTo expression
	 * if comparison is att1 = att2 find corresponding elements and union them
	 * if comparison is att1 = val find corresponding element for att1 and set values 
	 * and update numeric bound (equality constraint in this case)
	 */
	@Override
	public void visit(EqualsTo arg0) {
		// TODO Auto-generated method stub
		equality=true;
		Expression left = arg0.getLeftExpression();
		left_att= left;
		left.accept(this);
		
		Expression right = arg0.getRightExpression();
		right.accept(this);
		equality = false;
	
	}

	/**
	 * if comparison is att1 > val find corresponding element for att1 and set values 
	 * and update numeric bound (lower constraint in this case)
	 */
	@Override
	public void visit(GreaterThan arg0) {
		// TODO Auto-generated method stub
		greater=true;
		Expression left = arg0.getLeftExpression();
		left_att= left;
		left.accept(this);
		String table1=lastTable;
		
		Expression right = arg0.getRightExpression();
		right.accept(this);
		greater = false;
		String table2=lastTable;
		
		if (table1!=null && table2!=null && table1!=table2) {
			uf.unused.add(arg0);

		}
		
	}

	/**
	 * if comparison is att1 >= val find corresponding element for att1 and set values 
	 * and update numeric bound (lower constraint in this case)
	 */
	@Override
	public void visit(GreaterThanEquals arg0) {
		// TODO Auto-generated method stub
		greatereq=true;
		Expression left = arg0.getLeftExpression();
		left_att= left;
		left.accept(this);
		String table1=lastTable;
		
		Expression right = arg0.getRightExpression();
		right.accept(this);
		greatereq = false;
		String table2=lastTable;
		
		if (table1!=null && table2!=null && table1!=table2) {
			uf.unused.add(arg0);

		}
	}

	/**
	 * if comparison is att1 < val find corresponding element for att1 and set values 
	 * and update numeric bound (upper constraint in this case)
	 */
	@Override
	public void visit(MinorThan arg0) {
		// TODO Auto-generated method stub
		minor=true;
		Expression left = arg0.getLeftExpression();
		left_att= left;
		left.accept(this);
		String table1=lastTable;
		
		Expression right = arg0.getRightExpression();
		right.accept(this);
		minor = false;
		String table2=lastTable;
		
		if (table1!=null && table2!=null && table1!=table2) {
			uf.unused.add(arg0);

		}
	}

	/**
	 * if comparison is att1 <= val find corresponding element for att1 and set values 
	 * and update numeric bound (upper constraint in this case)
	 */
	@Override
	public void visit(MinorThanEquals arg0) {
		// TODO Auto-generated method stub
		minoreq=true;
		Expression left = arg0.getLeftExpression();
		left_att= left;
		left.accept(this);
		String table1=lastTable;
		
		Expression right = arg0.getRightExpression();
		right.accept(this);
		minoreq = false;
		String table2=lastTable;
		
		if (table1!=null && table2!=null && table1!=table2) {
			uf.unused.add(arg0);

		}
	}
	
	/**
	 * If it is a NotEqualsTo expression, it is an unusable comparison
	 */
	@Override
	public void visit(NotEqualsTo arg0) {
		// TODO Auto-generated method stub
		
		Expression left = arg0.getLeftExpression();
		left_att= left;

		left.accept(this);
		String table1=lastTable;

		Expression right = arg0.getRightExpression();
		right.accept(this);
		String table2=lastTable;
		
		if (table1!=null && table2!=null && table1!=table2) {
			uf.unused.add(arg0);
		}		
	}
	
	/**
	 * If it is an AndExpression, union-find the left and right expressions
	 */
	@Override
	public void visit(AndExpression arg0) {
		// TODO Auto-generated method stub
		Expression left = arg0.getLeftExpression();
		left.accept(this);
		Expression right = arg0.getRightExpression();
		right.accept(this);
	}

	
	
	@Override
	public void visit(NullValue arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void visit(Function arg0) {
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