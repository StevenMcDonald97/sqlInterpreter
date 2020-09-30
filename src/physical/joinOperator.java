package physical;
/** An instance of joinOperator takes the tuples from two child operators lc and rc, and
 *	 tests if the combination of them satisfies the expression ex. 
 *	The tuple nested loop algorithm is used to pair tuples
 */

import net.sf.jsqlparser.expression.Expression;

import java.util.*;

import main.tuple; 

public class joinOperator extends operator{
	private operator lc;
	private operator rc;
	private Expression ex;

//	The linkedlist tuples is used in the outer while loop in getNextTuple to make sure no tuples 
//	are skipped between subsequent calls to getNextTuple
	LinkedList<tuple> tuples = new LinkedList<tuple>();

	
	/** joinOperator(operator l_child, operator r_child, Expression e) creates an instance of joinOperator 
	 * 	with l_child and r_child as its children operators 
	 * 	@param [operator l_child] left child operator
	 * 	@param [operator r_child] right child operator
	 * 	@param [Expression e] is the expression to test tuples against
	 * 	@return an instance of joinOperator with l_child and r_child as children
	 * */
	public joinOperator(operator l_child, operator r_child, Expression e){
		lc = l_child;
		rc = r_child;
		ex = e;

	}
	
	@Override

	/**getNextTuple() finds the next tuple returned by each child operator, combines them, and  tests
	 * 	the combined tuple against the expression ex. This is done with the nested loop algorithm.
	 * @return a tuple with a body containing the combined tuples satisfying ex
	 */
	public tuple getNextTuple(){
		tuple outer;
		tuple current_outer;
		tuple  inner;
		tuple new_tuple = null;
		ArrayList<String> new_schema = new ArrayList<String>();
		ArrayList<Integer> new_body;

//		while there is still another tuple to consider or the list of tuples which have not been used yet is not empty
		while((outer = lc.getNextTuple()).getBody() != null || !tuples.isEmpty()) {

//			while the inner tuple is not null
			if (outer.getBody()!=null){
				tuples.addLast(outer);
			}
//			the first element in tuples is either the current outer tuple, or it is a tuple for which not
//			every inner tuple has been matched yet
			current_outer = tuples.getFirst();

			while((inner = rc.getNextTuple()).getBody() != null && current_outer.getBody() != null) {

//				new tuple combines bodies and schemas of children tuples, 
//				and sets tuple's last table to inner table
				new_body = new ArrayList<Integer>();
				new_schema = new ArrayList<String>();

	//			!!! test that this works with null
				new_body.addAll(current_outer.getBody());
				new_body.addAll(inner.getBody());
//				concatenate the column labels on each side to get the schema for the joined tuple
				new_schema.addAll(current_outer.getSchema());
				new_schema.addAll(inner.getSchema());

				new_tuple = new tuple(new_body, new_schema, inner.getLastTable());
				
				if (ex == null) {
					return (new_tuple);
	
				} else {
	//				!!! note this assumes schemas are in the form: TableName.ColumnName
					SelectionCondition joinCon = new SelectionCondition(new_body, new_schema);
					ex.accept(joinCon);

					if (joinCon.returnCondition()){
						return (new_tuple);
					}
					
				}
				
			} 
			rc.reset();

//			remove current outer tuple from consideration
			tuples.removeFirst();

		}
		
//		if no more tuples are found, return empty tuple
		return new tuple();
		
	}

	@Override
	/**reset() resets the children operators to their first tuples
	  */
	public void reset() {
		lc.reset();
		rc.reset();
	}
	
	public void reset(int i) {};
	/**	getSchema() returns the schema an oeprator uses
	 */
	public List<String> getSchema(){
		System.out.println("getSchema unimplemented for this operator");
		return new ArrayList<String>();
	};

}