package logical;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class JoinOperator extends OperatorTree implements logicalOperator {
	public Expression e;
	OperatorTree root;
//	OperatorTree left;
//	OperatorTree right;
	private List<String> tables;
	public List<OperatorTree> children;
	public List<String> joinOrder;
	public List<String> originalTableOrder;
	public HashMap<List<String>, BinaryExpression> joinConditions;

	/**
	 * Initialize JoinOperator
	 * @param root is the base OperatorTree
	 * @param left is the left OperatorTree
	 * @param right is the right OperatorTree
	 * @param ex is the join expression
	 */
	public JoinOperator(OperatorTree root, Expression ex, List<OperatorTree> kids, List<String> originalOrder, List<String> order, HashMap<List<String>, BinaryExpression> conditions){
		super(root);
		this.e=ex;
		this.root=root;
//		this.left=left;
//		this.right=right;
		this.children=kids;

		this.joinOrder=order;
		this.originalTableOrder = originalOrder;
		this.joinConditions=conditions;
//		this.tables=tbls;
//		the below set the left/right children inherited from the OperatorTree class
//		this.setLeft(left);
//		this.setRight(right);
//		join the two schemas from the children tables
		ArrayList<String> tempSchema=new ArrayList<String>();

		for  (OperatorTree op : children) {
			tempSchema.addAll(op.getSchema());
		}
		this.setSchema(tempSchema);
	}
	/**
	 * return the list of operator tree children of the current join 
	 * @return children
	 */
	public List<OperatorTree> getChildren(){
		return children;
	}
	/**
	 * return the order of the original tables
	 * @return the list of table names in the original from clause 
	 */
	public List<String> getOriginalOrder(){
		return originalTableOrder;
	}
	/**
	 * return the order to compute joins in 
	 * @return the list of table names to compute joins in 
	 */
	public List<String> getOrder(){
		return joinOrder;
	}
	public void accept(main.PhysicalPlanBuilder p){
		p.visit(this);
	}
	
}
