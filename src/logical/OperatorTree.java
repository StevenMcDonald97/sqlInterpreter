package logical;
import java.util.ArrayList;

import main.*;

/**
   * Tree is the structure that is used to store the relational algebra tree
   *
   */
  public abstract class OperatorTree implements logicalOperator {
	  private OperatorTree root;
	  private OperatorTree left;
	  private OperatorTree right;
	  private ArrayList<String> schema;
	  public String table;

	  /**
	   * Initializes the tree
	   * @param base is the new parent/root node
	   */
	  public OperatorTree(OperatorTree base) {
	    this.root = base;
	  }
	  
	  /**
	   * @param root is the new root node
	   */
	  public void setRoot(OperatorTree root) {
	    this.root = root;
	  }
	  
	  /**
	   * @param left is the new left child node
	   */
	  public void setLeft(OperatorTree left) {
	    this.left = left;
	  }
	  
	  /**
	   * @param right is the new right child node
	   */
	  public void setRight(OperatorTree right) {
	    this.right = right;
	  }
	  
	  /**
	   * @return the root node
	   */
	  public OperatorTree getRoot() {
	    return root;
	  }
	  
	  /**
	   * set the schema of the current operator
	   */
	  public void setSchema(ArrayList<String> s) {
	    schema=s;
	  }
	  
	  
	  /**
	   * @return the schema of the current operator
	   */
	  public ArrayList<String> getSchema() {
	    return schema;
	  }
	  
	  /**
	   * @return the left child node
	   */
	  public OperatorTree getLeft() {
	    return left;
	  }
	  
	  /**
	   * @return the right child node
	   */
	  public OperatorTree getRight() {
	    return right;
	  }
	  
	  /**
	   * Sets parent nodes
	   * @param parent is node that will be set as a parent
	   */
	  public void setParentNodes(OperatorTree parent) {
	    this.root = parent;
	    if (left != null) left.setParentNodes(this);
	    if (right != null) right.setParentNodes(this);
	  }
	  
	  /**
	   * @return the table associated with this operator
	   */
	  public String getTable() {
	    return table;
	  }
	  
  }