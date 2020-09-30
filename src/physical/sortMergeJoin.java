package physical;
/** An instance of sortMergeJoin takes the tuples from two child operators lc and rc, and
 *  uses external sort to sort them in memory, and then tests if each pair of tuples 
 *  returned satisfies the expression ex. The sort merge join algorithm is used to pair 
 *  tuples.
 *  NOTE: This operator assumes an equijoin is being applied
 */

import net.sf.jsqlparser.expression.Expression;

import java.util.*;

import main.Interpreter;
import main.tuple; 

public class sortMergeJoin extends operator {
	private operator lc;
	private operator rc;
	private Expression ex;
	public String leftSortLine="";
	public String rightSortLine="";

//	store the attributes to order the tuples by for each child
	List<String> leftOrder;
	List<String> rightOrder;
	
//	store lists of which indices to look at in each tuple 
	List<Integer> leftIndices = new ArrayList<Integer>();
	List<Integer> rightIndices= new ArrayList<Integer>();
	
//	keep track of where to reset left and right operators too
	int leftInd; //Tr
	int rightInd; //Gs
	int innerRightInd; //Ts
	
	tuple Tr;
	tuple Gs;
	tuple Ts;
	
	Boolean innerReset;
	Boolean outerReset;


	
	
//	have index for resetting outer tuple

	/** sortMergeJoin(operator l_child, operator r_child, Expression e) creates an instance of joinOperator 
	 * 	with l_child and r_child as its children operators.
	 *  Note that we can assume that operators on tables appear in the same order as tables in 
	 *  the join expression, so l_child will always correspond to leftOrder
	 *  and the same applies for r_child and rightOrder
	 * 	@param [operator l_child] left child operator
	 * 	@param [operator r_child] right child operator
	 * 	@param [Expression e] is the expression to test tuples against
	 * 	@return an instance of sortMergeJoin with l_child and r_child as children
	 * */
	public sortMergeJoin(operator l_child, operator r_child, Expression e, int sortPageNum){
//		lc = l_child;
//		rc = r_child;
		ex = e;
//		!!! what if expression e is null?
//		!!! Move sorting to physical plan builder
		List<String> left_schema = l_child.getNextTuple().getSchema();

		List<String> leftTables = new ArrayList<String>();
		l_child.reset();

		if (left_schema !=null) {
			for (String attr : left_schema) {
				String[] attribute = attr.split("\\.");
				String table = attribute[0];
				if (!leftTables.contains(table)) {
					leftTables.add(table);
				}
			}
		}
		
		List<String> right_schema = r_child.getNextTuple().getSchema();
		List<String> rightTables = new ArrayList<String>();
		r_child.reset();
		if (right_schema!=null) {
			for (String attr : right_schema) {
				String table = attr.split("\\.")[0];
				if (!rightTables.contains(table)) {
					rightTables.add(table);
				}
			}	
		} 
		
		sortMergeOrder sortOrders = new sortMergeOrder(leftTables, rightTables);
		if (e!=null) {
			e.accept(sortOrders);
		}
//		System.out.println(e);

		leftOrder = sortOrders.getLeftOrder();
		int oldLeftSize=leftOrder.size();
		leftSortLine="-ExternalSort"+leftOrder+"\n";
		
//		fill in sort orders if necessary
		if (left_schema!=null) {
			for(String left_attr:left_schema) {
				if(!leftOrder.contains(left_attr)) {
					leftOrder.add(left_attr);
				}
			}
		}

		rightOrder = sortOrders.getRightOrder();
		int oldRightSize=rightOrder.size();
		rightSortLine="-ExternalSort"+rightOrder+"\n";

		if (right_schema!=null) {
			for(String right_attr:right_schema) {
				if(!rightOrder.contains(right_attr)) {
					rightOrder.add(right_attr);
				}
			}
		}


		leftInd=0;
		rightInd=0;
		innerRightInd=0;

//		lc = new SortOperator(l_child, leftOrder);
//		rc = new SortOperator(r_child, rightOrder);
		try {
			lc = new ExternalSortOperator(l_child, leftOrder, sortPageNum, Interpreter.getTempDir());
			rc = new ExternalSortOperator(r_child, rightOrder, sortPageNum, Interpreter.getTempDir());
		}catch (Exception exc){
			System.err.println("Exception occurred sorting children in sort merge join");
			exc.printStackTrace();		
		} 


//		if (sortNum==0) {
//			lc = new SortOperator(l_child, leftOrder);
//			rc = new SortOperator(r_child, rightOrder);
//
//		} else {
//			try {
//				lc = new ExternalSortOperator(l_child, leftOrder, sortPageNum, Interpreter.getTempDir());
//				rc = new ExternalSortOperator(r_child, rightOrder, sortPageNum, Interpreter.getTempDir());
//				lc = new physical.DuplicateEliminationOperator(lc);
//				rc = new physical.DuplicateEliminationOperator(rc);
//
//			}catch (Exception exc){
//				System.err.println("Exception occurred sorting children in sort merge join");
//				exc.printStackTrace();		
//			} 
//		}

		
//		initialize tuples from each operator
		Gs = rc.getNextTuple();
		Tr = lc.getNextTuple();
		
		innerReset = false;
		outerReset = false;
		
//		find indices each column in order corresponds to in schema		
		String col; 
		int index1;
		int index2;

		
//		System.out.println(oldLeftOrder);

		for (int i=0; i<oldLeftSize;i++) {
			col = leftOrder.get(i);
			index1 = Tr.getSchema().indexOf(col);			
			leftIndices.add(index1);
		}
		for (int i=0; i<oldRightSize;i++) {
			col = rightOrder.get(i);
			index2 = Gs.getSchema().indexOf(col);
			rightIndices.add(index2);
		}
//		System.out.println(e);
//		System.out.println(leftTables);
//		System.out.println(rightTables);
//
//		System.out.println(getNextTuple().getBody());
//

	}

	@Override
	public tuple getNextTuple() {
		// TODO Auto-generated method stub
//		System.out.println("new iteration");

		ArrayList<String> new_schema = new ArrayList<String>();
		ArrayList<Integer> new_body;
		tuple newTuple;

//		tuple Gs;
		
//		reset each child to its last seen index
//		lc.reset(leftInd);
//		rc.reset(rightInd);
//
//				

//		while tr body is not null and ts body is not null
		while (Gs.getBody() != null && Tr.getBody() != null){ 
//			if this is a call subsequent to a return value, then we should not
//			increment Tr or Gs again until the inner loop is satisfied
			if (!innerReset) {

//				iterate both tuples until their first join attribute lines up or one is null
				while(Tr.getBody()!=null && RlessThanS(Tr,Gs)) {
					Tr = lc.getNextTuple();
					leftInd +=1;

				}

//				double check that tuple is not empty
				if (Tr.getBody()==null) {
					break;
				}
	
				while(Gs.getBody()!=null && RgreaterThanS(Tr,Gs)) {
					Gs = rc.getNextTuple();
					rightInd +=1;

				}	
				
//				double check that tuple is not empty
				if (Gs.getBody()==null) {
//					System.out.println("null S");

					break;
				}
				
				
//				need to store index inside of S that Gs will be reset to on next pass
				innerRightInd=rightInd;
				rc.reset(rightInd);
				Ts = rc.getNextTuple();
			} 

			while(Tr.getBody() != null && RequalsS(Tr, Gs)) {
//				new tuple combines bodies and schemas of children tuples, and sets tuple's last table to inner table
				new_body = new ArrayList<Integer>();
				new_schema = new ArrayList<String>();

				if (!innerReset) {
					innerRightInd=rightInd;
					rc.reset(rightInd);
					Ts = rc.getNextTuple();

				}
//				break out if Ts returned null

				if (Ts.getBody()==null) {
					break;
				}

//					innerRightInd +=1;
//					System.out.println(S.getBody());
				
				if(RequalsS(Tr,Ts)) {

		//			!!! test that this works with null
					new_body.addAll(Tr.getBody());
					new_body.addAll(Ts.getBody());
	//				concatenate the column labels on each side to get the schema for the joined tuple
					new_schema.addAll(Tr.getSchema());
					new_schema.addAll(Ts.getSchema());
	
					newTuple = new tuple(new_body, new_schema, Tr.getLastTable());
					
//						advance Ts
					Ts=rc.getNextTuple();

					innerRightInd+=1;
					innerReset = true;
					
					return newTuple;

				} else {
//					if this is not a match anymore, then increment outer index and operator
//					advance Tr
//					Ts = Gs
					Tr = lc.getNextTuple();
					leftInd+=1;
					innerReset = false;

				}

				
			} 

			innerReset = false;
			rightInd=innerRightInd;
			rc.reset(innerRightInd);
			Gs=rc.getNextTuple();

//				rightInd+=1; ???


			
		}		
		
		
		return new tuple();
	}

	@Override
	public void reset() {
		lc.reset(0);
		rc.reset(0);
		leftInd=0;
		rightInd=0;
		innerRightInd=0;
		Gs = rc.getNextTuple();
		Tr = lc.getNextTuple();
		innerReset=false;
		Ts = null;

		// TODO Auto-generated method stub
		
	}
	
	/**RgreaterThanS returns true if every comparison attribute in R is greater than 
	 *  its corresponding comparison attribute in S
	 *  
	 * @return
	 */
	public Boolean RgreaterThanS(tuple R, tuple S) {

//		for each attribute in the left order
		int index1;
		int index2;
		String col;
		Boolean compare = false;
		int i=0;
		while (i<leftIndices.size() && i<rightIndices.size()) {
			index1 = leftIndices.get(i);
			index2 = rightIndices.get(i);

//			if any attribute is not strictly greater
			if (R.getBody().get(index1) > S.getBody().get(index2)) {
				compare = true;
				break;
			}
			else if (R.getBody().get(index1) < S.getBody().get(index2)){
				compare = false;
				break;
			}
			i++;
		}

		return compare;
	}
	
	/**RlessThanS returns true if every comparison attribute in R is less than 
	 *  its corresponding comparison attribute in S
	 *  
	 * @return
	 */
	public Boolean RlessThanS(tuple R, tuple S) {
//		for each attribute in the left order
		int index1;
		int index2;
		String col;
		Boolean compare = false;


		int i=0;
		while (i<leftIndices.size() && i<rightIndices.size()) {
			index1 = leftIndices.get(i);
			index2 = rightIndices.get(i);
				
//			if any attribute is not strictly less than

			if (R.getBody().get(index1) < S.getBody().get(index2)) {
				compare = true;
				break;
			}
			else if (R.getBody().get(index1) > S.getBody().get(index2)){
				compare = false;
				break;
			}
			i++;
		}


		return compare;
	}
	
	/**RequalsS returns true if every comparison attribute in R is equal to
	 *  its corresponding comparison attribute in S
	 *  
	 * @return
	 */
	public Boolean RequalsS(tuple R, tuple S) {
//		for each attribute in the left order
		int index1;
		int index2;
		String col;
		Boolean compare = true;
		int i=0;
		while (i<leftIndices.size() && i<rightIndices.size()) {
			index1 = leftIndices.get(i);
			index2 = rightIndices.get(i);

//			if any attribute is not strictly greater
			if (!R.getBody().get(index1).equals(S.getBody().get(index2))) {

				compare = false;
			}
			i++;
			
		}
//		System.out.println(R.getBody());
//		System.out.println(S.getBody());
//		System.out.println(compare);

		return compare;
	}

//	!!! need reset to index for external sort!
//	public void reset(int index) {
//		System.out.println("Reset unimplememnted for smj");
//
//		// TODO Auto-generated method stub
//		
//	}
	
	public void reset(int i) {};
	/**	getSchema() returns the schema an oeprator uses
	 */
	public List<String> getSchema(){
		System.out.println("getSchema unimplemented for this operator");
		return new ArrayList<String>();
	};

}
