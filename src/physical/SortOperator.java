package physical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import main.tuple;
//import net.sf.jsqlparser.statement.select.OrderByElement;


public class SortOperator extends operator{
	public LinkedList<tuple> buffer;
	operator op;
	List<String> or;
	String table;
	int bufferIndex;
	//ArrayList<String> schema;
	
	/**
	 * Sort Operator takes a child operator c and List<OrderByElement> order (list of attributes to consider)
	 * and initiates a buffer to add tuples in
	 * @param c is a child operator
	 */
	public SortOperator(operator c, List<String> order){
		op = c;
		or = order;

		buffer = getBuffer();
		bufferIndex=0;

	}
	
	
	/**
	 * method that initiates buffer variable
	 * @return buff, which is a LinkedList buffer with sorted tuples
	 */
	public LinkedList<tuple> getBuffer(){
		LinkedList<tuple> buff = new LinkedList<tuple>();
		tuple t=op.getNextTuple();
		


		while (t.getBody()!=null){
			buff.add(t);

			t = op.getNextTuple();
		
		}

		Collections.sort(buff, new Comparator<tuple>(){
			/**
			 * Comparator to compare two tuples used for sorting
			 * Also using i, accounts for next point of comparison if first priority
			 * can't be compared
			 * @return int value -1, 0 or 1 (-1 if t1<t2, 1 if t1>t2, 0 if after going through all attributes, still equal)
			 */
			@Override
			public int compare(tuple t1,tuple t2){

				int i = 0; //attribute being used
//				OrderByElement attribute = or.get(i); //choose first or i-th attribute from list of orderbyelements
				String temp = or.get(i);

				int index = t1.getSchema().indexOf(temp); //index of tuple that corresponds to specific attribute

				while (i<or.size()){


					if ((t1.getBody().get(index))<(t2.getBody().get(index)))
						return -1;
					else if ((t1.getBody().get(index))>(t2.getBody().get(index)))
						return 1;
					else{
						i++; 
						if (i<or.size()){
							temp = or.get(i);
							index = t1.getSchema().indexOf(temp); //index of tuple that corresponds to specific attribute

						}
						
					}
				}
				return 0;
			}


		});

		return buff;
	}
	
	
	/**
	 * @return tuple when requested
	 */
	public tuple getNextTuple(){
//		while index is not at end of buffer
		if (bufferIndex<buffer.size()){
			try{
				tuple next = buffer.get(bufferIndex);
				bufferIndex++;
				return next;
			}
			catch(Exception e){
				System.err.println("Exception occurred in sort");
				e.printStackTrace();

				return null;
			}
		}
		else return new tuple();
		
		
	}
	
	/**
	 * Reset method to clear out the buffer and 
	 * also reset child operator
	 */
	public void reset(){
//		buffer.clear();
		bufferIndex = 0;
//		op.reset();
		
	}
	
	/**
	 * Reset method to move pointer in buffer to index
	 * @param index is the location in the buffer to start from on 
	 * next call to getNextTuple()
	 */
	public void reset(int index){
		bufferIndex=index;
//		System.out.println(buffer.get(bufferIndex).getBody());
		
	}
	/**	getSchema() returns the schema an oeprator uses
	 */
	public List<String> getSchema(){
		System.out.println("getSchema unimplemented for this operator");
		return new ArrayList<String>();
	};
}