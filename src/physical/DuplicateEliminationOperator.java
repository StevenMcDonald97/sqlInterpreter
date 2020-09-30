package physical;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import main.tuple;

public class DuplicateEliminationOperator extends operator {
	operator op;
	tuple lastTuple;
//	LinkedList<List<Integer>> tuples = new LinkedList<List<Integer>>();
	
	public DuplicateEliminationOperator(operator c) {
		op = c;
		lastTuple = new tuple();
	}
	
	@Override
	public tuple getNextTuple() {
		tuple curr = op.getNextTuple();
//		if not tuple is taken yet, take first tuple else if body of current tuple 
//		is not the same as previous then return current tuple, else skip 
		if (lastTuple.getBody()==null){
			lastTuple=curr;
			return curr;
		} else if (!lastTuple.getBody().equals(curr.getBody())){
			lastTuple=curr;
			return curr;
		} else if (curr.getBody()!=null){
			return getNextTuple();
		} else {
			return new tuple();
		}
	}

	@Override
	public void reset() {
		op.reset();
		
	}
	public void reset(int i) {};
	/**	getSchema() returns the schema an oeprator uses
	 */
	public List<String> getSchema(){
		System.out.println("getSchema unimplemented for this operator");
		return new ArrayList<String>();
	};
}