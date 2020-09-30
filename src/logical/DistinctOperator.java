package logical;

public class DistinctOperator extends OperatorTree implements logicalOperator{
	OperatorTree root;
	
	/**
	 * initialize DistinctOperator 
	 * @param root is the base OperatorTree
	 */
	public DistinctOperator(OperatorTree root) {
		super(root);
		// TODO Auto-generated constructor stub
		this.root=root;

	}
	
	  public void accept(main.PhysicalPlanBuilder p){
		  p.visit(this);
	  }

}
