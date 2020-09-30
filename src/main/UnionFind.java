package main;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.PlainSelect;

/**
 * UnionFind is the class that stores a collection of elements, where each element consists of a set of 
 * attributes and three numeric constraints (lower bound, upper bound, and equality constraint.
 */
public class UnionFind {
	public HashMap<Set<String>, List<Integer>> collection;
	public List<Expression> unused; //keep track of "nonunion-findable" expressions
	
	/**
	 * Instantiate the collection of elements
	 */
	public UnionFind(){
		collection = new HashMap<Set<String>, List<Integer>>();
		unused = new ArrayList<Expression>();
	}
	
	/**
	 * given attribute, find and return the union-find element containing that attribute;
	 * if no such element is found, create it and return it.
	 * @param att is the attribute to be found
	 * @return the corresponding union-find element
	 */
	public HashMap<Set<String>, List<Integer>> find(String att){
		//boolean found=false;
		for (Set<String> att_list: collection.keySet()){
			if (att_list.contains(att)){
				HashMap<Set<String>, List<Integer>> out = new HashMap<Set<String>, List<Integer>>();
				out.put(att_list, collection.get(att_list));
				collection.put(att_list, collection.get(att_list));
				return out;
			}
		}
		HashMap<Set<String>, List<Integer>> out = new HashMap<Set<String>, List<Integer>>();
		Set<String> newlist = new HashSet<String>(); newlist.add(att);
		List<Integer> newvalue = new ArrayList<Integer>(); newvalue.add(-1); newvalue.add(-1); newvalue.add(-1);
		out.put(newlist, newvalue);
		collection.put(newlist, newvalue);
		return out;
	}
	
	/**
	 * given attribute, find the bounds for the attribute
	 * @param att is the attribute which bounds are found for
	 * @return the corresponding list of integer bounds
	 */
	public List<Integer> findBounds(String att){
		//boolean found=false;
		for (Set<String> att_list: collection.keySet()){
			if (att_list.contains(att)){

				return collection.get(att_list);
			}
		}
		
//		System.err.println("tried to find unknown attribute in union find.");
		Integer[] bounds = {-1, -1, -1};
		List<Integer> newBounds = Arrays.asList(bounds);
		return newBounds;

	}
	
	/**
	 * given two union-find elements, modify the union-find data structure 
	 * so that these two elements get merged/"unioned"
	 * @param el1 is the first union-find element
	 * @param el2 is the second union-find element
	 */
	public void merge(Set<String> att1, List<Integer> val1, Set<String> att2, List<Integer> val2){
		
		//first union the attributes
		Set<String> new_att_list = new HashSet<String>();
		new_att_list.addAll(att1); new_att_list.addAll(att2);
		
		//then union the other values
		List<Integer> values = new ArrayList<Integer>(); values.add(Math.max(val1.get(0), val2.get(0)));values.add(-1); values.add(-1);
		//0th element is lower bound
		if (val1.get(0)!=-1 || val2.get(0)!=-1){
			if (val1.get(0)==-1){
				values.set(0, val2.get(0));
			}
			else if (val2.get(0)==-1){
				values.set(0, val1.get(0));
			}
			else{
				values.set(0,Math.max(val1.get(0), val2.get(0)));
			}
		}
		else{
			//values.add(null);
		}
		
		//1st element is upper bound
		if (val1.get(1)!=-1 || val2.get(1)!=-1){
			if (val1.get(1)==-1){
				values.set(1, val2.get(1));
			}
			else if (val2.get(1)==-1){
				values.set(1,val1.get(1));
			}
			else{
				values.set(1,Math.min(val1.get(1), val2.get(1)));
			}
		}
		else{
			//values.set(null);
		}
		
		
		//2nd element is constraint
		if (val1.get(2)!=-1 || val2.get(2)!=-1){
			if (val1.get(2)==-1){
				values.set(2,val2.get(2));
			}
			else{
				values.set(2,val1.get(2));
			}
		}
		else{
			//values.add(null);
		}
		
		collection.remove(att1); collection.remove(att2);
		collection.put(new_att_list, values);
		
		
	}
	
	/**
	 * for a given attribute, find the corresponding element and set the lower/upper bounds and equality constraint
	 * @param att attribute to look for
	 * @param lower is the lower bound
	 * @param upper is the upper bound
	 * @param constraint is the equality constraint value
	 */
	public void setValues(String att, int lower, int upper, int constraint){
		boolean done=false;
//		for (Set<String> att_list:collection.keySet()){
//			System.out.println(att_list.size());
//		}
		for (Set<String> att_list: collection.keySet()){
			if (att_list.contains(att)){
				List<Integer> val = new ArrayList<Integer>();
				val = collection.get(att_list);
				//MAKE SURE SPECIFIC BOUND IS SET ONLY IF THE PASSED IN VALUE ISN'T -1
				if (lower!=-1 && lower>val.get(0)) val.set(0, lower);
				if (upper!=-1 && upper<val.get(1)){
					val.set(1, upper);
				}
				if (constraint!=-1){
					val.set(2, constraint);
					val.set(0,  constraint); val.set(1,  constraint);
				}
				//val.add(lower); val.add(upper); val.add(constraint);
				collection.put(att_list, val);
				done=true;
				break;
			}
		}
		//if att does not exist in collection already
		if (!done){
			Set<String> newlist = new HashSet<String>(); newlist.add(att);
			List<Integer> newvalue = new ArrayList<Integer>();
			newvalue.add(lower); newvalue.add(upper); newvalue.add(constraint);
			collection.put(newlist, newvalue);
		}
	}
	
	
	

}