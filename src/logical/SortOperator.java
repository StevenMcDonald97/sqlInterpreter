package logical;

import java.util.List;

import net.sf.jsqlparser.statement.select.OrderByElement;

public class SortOperator extends OperatorTree implements logicalOperator {
	public OperatorTree root;
	public List<String> ol;

	/**
	 * Initialize SortOperator
	 * @param root is the base of the OperatorTree
	 * @param orderByElements is the list of OrderByElements
	 */
	public SortOperator(OperatorTree root, List<String> orderByElements) {
		super(root);
		// TODO Auto-generated constructor stub
		this.ol=orderByElements;
		this.root=root;

	}
	
	  public void accept(main.PhysicalPlanBuilder p){
		  p.visit(this);
	  }

}
