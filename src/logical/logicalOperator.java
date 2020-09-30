package logical;

import main.PhysicalPlanBuilder;

public interface logicalOperator {

	  
	  /**
	   * @param root is the new root node
	   */
	 void setRoot(OperatorTree root);
	  
	  /**
	   * @param left is the new left child node
	   */
	  void setLeft(OperatorTree left);
	  
	  /**
	   * @param right is the new right child node
	   */
	  void setRight(OperatorTree right); 
	  
	  /**
	   * @return the root node
	   */
	  public OperatorTree getRoot() ;
	  
	  /**
	   * @return the left child node
	   */
	  public OperatorTree getLeft(); 
	  
	  /**
	   * @return the right child node
	   */
	  public OperatorTree getRight();
	  /**
	   * Sets parent nodes
	   * @param parent is node that will be set as a parent
	   */
	  public void setParentNodes(OperatorTree parent);
	  
	  public void accept(PhysicalPlanBuilder p);
}
