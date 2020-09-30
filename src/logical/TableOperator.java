package logical;

import java.util.ArrayList;

import net.sf.jsqlparser.schema.Table;

/**
 * represents table to read from (leaf node)
 *
 */
public class TableOperator extends OperatorTree implements logicalOperator{
	public OperatorTree root;
//	public String table;


	/**
	 * Initialize TableOperator
	 * @param root is the base of the OperatorTree
	 * @param t is a Table to be read from
	 */
	public TableOperator(OperatorTree root, String t, ArrayList<String> s) {
		super(root);
		// TODO Auto-generated constructor stub
		this.table=t;
		this.root=root;
		this.setSchema(s);
	}
	
	
	
	public void accept(main.PhysicalPlanBuilder p){
		p.visit(this);
	}

}
