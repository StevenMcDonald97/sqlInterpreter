package physical;
/** An instance of BNLjoin uses the block nested loop join algorithm to compute a join on two tables
 */

import net.sf.jsqlparser.expression.Expression;

import java.util.*;

import main.tuple; 

public class BNLjoin extends operator {
	private operator leftChild;
	private operator rightChild;
	private Expression ex;
	
//	bufferPages represents how many pages fit in the buffer, with 1 page=4096 bytes
//	private int bufferPages;
//	tuple size is the number of ints in a tuple
	private int tuplesize;
	private int pageSize = 4096;
//	bufferTupleSize is the number of tuples which can fit in a buffer
	int bufferTupleSize;
	int bufSize;
//	int count=0;

//	keep metadata to track where the current getnextTuple call is in the table and buffer
	private List<tuple> outerBuf;
//	private int tuplesSeen;
	
//	innerIndex tracks the current index inside the block in the inner loop of hte GetNextTuple
	int innerIndex;
	
//	The linkedlist tuples is used in the outer while loop in getNextTuple to make sure no tuples 
//	are skipped between subsequent calls to getNextTuple
	LinkedList<tuple> tuples = new LinkedList<tuple>();

//	store the schema for the joined tuples
	ArrayList<String> new_schema = new ArrayList<String>();

	
	/** BNLjoin(operator l_child, operator r_child, Expression e) creates an instance of joinOperator 
	 * 	with l_child and r_child as its children operators 
	 * 	@param [operator l_child] left child operator
	 * 	@param [operator r_child] right child operator
	 * 	@param [Expression e] is the expression to test tuples against
	 * 	@param [int bPages] is the size in pages of the outer buffer
	 * 	@return an instance of joinOperator with l_child and r_child as children
	 * */
	public BNLjoin(operator l_child, operator r_child, Expression e, int bPages){
		leftChild = l_child;
		rightChild = r_child;
		ex = e;
//		tuplesSeen = 0;
//		bufferPages = bPages;
//		find the size of tuples from the left child
		tuple leftTup =l_child.getNextTuple();
		tuplesize = leftTup.getBody() != null ? leftTup.getBody().size() : 1;
		l_child.reset();
		bufferTupleSize = bPages*pageSize/(4*tuplesize);
		bufSize=0;
		innerIndex = 0;
		outerBuf = new LinkedList<tuple>();
		getOuterBuffer();

	}


	@Override
	public tuple getNextTuple() {

//		if all the tuples in the current buffer have been seen, or the outer buffer is uninitialized
//		reset the inner table operator
//		if (innerIndex == bufSize || outerBuf == null ){
//			System.out.println(count);
//
//			outerBuf = getOuterBuffer();
//			tuples.clear();
//			rightChild.reset();
//		}
		
//		first load buffer
		if (outerBuf.isEmpty()){
			getOuterBuffer();
			tuples.clear();
//			reset the right child
			rightChild.reset();
		}
		
//		if after loading buffer is still empty, return empty tuple
		if (outerBuf.isEmpty()){
			return new tuple();
		}
		
//		take tuple from inner (basically now the outer from the original tuple nested join) and compare against every 
//		tuple in the buffer
		
		tuple middle;
		tuple current_middle;
		tuple new_tuple = null;
		ArrayList<Integer> new_body;
		
		
//		while there is still another tuple to consider or the list of tuples which have not been used yet is not empty
		while((middle = rightChild.getNextTuple()).getBody() != null || !tuples.isEmpty()) {
//			if there is still a non-empty tuple add it to the list of inner tuples
			if (middle.getBody()!=null){
				tuples.addLast(middle);
//				count++;
			}
//			the first element in tuples is either the current outer tuple, or it is a tuple for which not
//			every inner tuple has been matched yet
			current_middle = tuples.getFirst();


//			every time the for loop starts one after the index of the last tuple returned
			for (int i=innerIndex; i<bufSize; i++){

				tuple inner = outerBuf.get(i);
//				new tuple combines bodies and schemas of children tuples, 
//				and sets tuple's last table to inner table
				new_body = new ArrayList<Integer>();
				new_schema = new ArrayList<String>();

	//			!!! test that this works with null
				new_body.addAll(inner.getBody());
				new_body.addAll(current_middle.getBody());

//				concatenate the column labels on each side to get the schema for the joined tuple
				new_schema.addAll(inner.getSchema());
//				System.out.println(inner.getSchema());
				new_schema.addAll(current_middle.getSchema());
//				System.out.println(current_middle.getSchema());

				new_tuple = new tuple(new_body, new_schema, inner.getLastTable());

				if (ex == null) {
					innerIndex=i+1;
					return (new_tuple);
	
				} else {
	//				!!! note this assumes schemas are in the form: TableName.ColumnName
					SelectionCondition joinCon = new SelectionCondition(new_body, new_schema);

					ex.accept(joinCon);

					if (joinCon.returnCondition()){
						innerIndex=i+1;
						return (new_tuple);
					}
					
				}
				
			}
			
			innerIndex=0;
//			remove current outer tuple from consideration
			tuples.removeFirst();
			
		}
		
//		when every tuple of the right child has been visited, relaod the buffer and recurse
		getOuterBuffer();
		tuples.clear();
//		reset the right child
		rightChild.reset();
		
		return getNextTuple();
		

		
		
	}


	@Override
	public void reset() {
		// TODO Auto-generated method stub
		leftChild.reset();
		rightChild.reset();
		outerBuf = new LinkedList<tuple>();
//		tuplesSeen = 0;
		innerIndex = 0;
	}
	
	
	public void getOuterBuffer(){
//		System.out.println("updated buffer");
//		System.out.println(count);

//		count=0;
		List<tuple> outer = new ArrayList<tuple>();
		tuple t;
//		figure out how many tuples fit in the buffer
		int outerTuplesSeen = 0;

//		while there is another (non empty) tuple to take and the size of the buffer is not yet reached
		while (outerTuplesSeen != bufferTupleSize && (t=leftChild.getNextTuple()).getBody() != null){

			outer.add(t);
			outerTuplesSeen++;
		}
		
//		reset index in inner loop
		innerIndex=0;
		bufSize = outer.size();
//		System.out.println(bufSize);
		outerBuf = outer;
	}
	public void reset(int i) {};

//	public void checkBuffer(){
////		take next tuple to see if 
//		tuple middle = rightChild.getNextTuple();
//
//
////		if there are not more tuples to to take from the right child, reset and take the next block
//		if (middle.getBody()==null && tuples.isEmpty()){
//			getOuterBuffer();
////			clear tuples array (should be empty already)
//			tuples.clear();
////			reset the right child
//			rightChild.reset();
//			
////		if the tuple take is not null, then add it to tuples so that it will be checked later 
//		} else if (middle.getBody()!=null) {
//			tuples.addLast(middle);
//		}
//	}
	/**	getSchema() returns the schema an oeprator uses
	 */
	public List<String> getSchema(){
 		return new_schema;
	};
	
}
