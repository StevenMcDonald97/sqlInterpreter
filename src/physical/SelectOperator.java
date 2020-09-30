package physical;
/** An instance of selectOperator is used to find the next row returned by a scan operator 
 * which satisfies a given expression
 */
import java.util.ArrayList;
import java.util.List;

import main.tuple;
import net.sf.jsqlparser.expression.Expression;


public class SelectOperator extends operator {
	operator so;
	Expression ex;
//	String table;
	ArrayList<String> schema;
	
	/** SelectOperator(ScanOperator sc, Expression e) creates an instance of selectOperator 
	 * 	with a scan operator as a child  
	 * 	@param [ScanOperator sc] is the child scan operator to take tuples from
	 * 	@param [Expression e] is the expression to test tuples against
	 * 	@return an instance of selectOperator with sc as its child operator and e as its expression
	 * */
	public SelectOperator(operator sc, Expression e){
		so = sc;
		ex = e;
		schema =(ArrayList<String>) sc.getSchema();

	}
	
	/**getNextTuple() finds the next tuple returned by the child satisfying the Expression ex. 
	 * @return a tuple with a body containing the next row of the table which satisfies the given expression
	 */
	public tuple getNextTuple(){
		tuple t;
		ArrayList<Integer> tupleBody;

		while((t = so.getNextTuple()).getBody() !=null){
			boolean cond_met = false;
			tupleBody =  (ArrayList<Integer>) t.getBody();
			SelectionCondition selectCon = new SelectionCondition(tupleBody, schema);

			ex.accept(selectCon);
			cond_met = selectCon.returnCondition();
			
			if (cond_met) return t;
		}
		

		return t;
	}
	
	/**reset() resets the child operator to its first tuple
	  */
	public void reset(){
		so.reset();
	}
	
	public void reset(int i) {};
	/**	getSchema() returns the schema an oeprator uses
	 */
	public List<String> getSchema(){
		return schema;
	};
	
}
