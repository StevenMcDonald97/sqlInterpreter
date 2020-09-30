package logical;
import java.util.*;

import net.sf.jsqlparser.statement.select.SelectItem;

public class ProjectOperator extends OperatorTree implements logicalOperator{
	public List<SelectItem> selectitems;
	OperatorTree root;
	ArrayList<String> newSchema;
	public ArrayList<String> oldSchema;

	/**
	 * Initialize ProjectOperator
	 * @param root is the root tree
	 * @param left is the left/child operator
	 * @param selItems is the select items
	 */
	public ProjectOperator(OperatorTree root, OperatorTree left, List<SelectItem> selItems){
		super(root);
		selectitems=selItems;
		this.root=root; 
		this.setLeft(left);
		oldSchema=left.getSchema();

		
//		create the schema for the projection
		this.newSchema=new ArrayList<String>();

		String selectString;
		for (SelectItem i : selItems){
			selectString = i.toString();
			newSchema.add(selectString);
		}
		
		this.setSchema(newSchema);
		
		
		
	}
	
	public void accept(main.PhysicalPlanBuilder p){
		p.visit(this);
	}

	
	
}
