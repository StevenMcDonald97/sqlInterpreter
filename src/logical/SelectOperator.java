package logical;

import net.sf.jsqlparser.expression.Expression;

public class SelectOperator extends OperatorTree implements logicalOperator{
	public Expression e;
	public OperatorTree root;
	
	/**
	 * Initialize SelectOperator
	 * @param root is the base OperatorTree
	 * @param ex is the Select expression
	 */
	public SelectOperator(OperatorTree root, Expression ex){
		super(root);
		e=ex;
		this.root=root;

	}
	
	  public void accept(main.PhysicalPlanBuilder p){
		  p.visit(this);
	  }
	
}
