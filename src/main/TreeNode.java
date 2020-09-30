package main;

import java.util.List;

/**
 * abstract class for methods that Leaf nodes and Index nodes implement
 *
 */
public abstract class TreeNode {
	List<Integer> keys;


	abstract List<RecordId> getValue(int key);

	abstract void deleteValue(int key);

	abstract int getFirstLeafKey();
	
	abstract boolean isLeafNode();
	
	abstract List<TreeNode> getChildren();
	
	abstract void insertEntry(int key, List<RecordId> value);

	public String toString() {
		return keys.toString();
	}
}