package physical;
import net.sf.jsqlparser.statement.select.SelectItem;
import java.util.List;

import main.tuple;

import java.util.ArrayList;

/** An instance of projectionOpertor filters a specified table for the columns specified in 
 * the SELECT statement
 */
public class projectionOperator extends operator{
	operator child;
//	String table;
	List<String> selectAttributes;
//	these are the two schemas before and after the projection
	ArrayList<String> schema;
	ArrayList<String> newSchema;

	/** projectionOperator(operator c, List<SelectItem> attrs, String t) creates an instance 
	 * 	of projection Operator   
	 * 	@param [operator c] is a child operator, either a projectionOperator or a selectOperator 
	 * 	@param [List<SelectItem> attrs] is a list of columns names to be extracted 
	 * 	@param [string t] is the name of the table the scan operator is opened on 
	 * 	@return the instance of ScanOperator with a reader opened on the file associated with table
	 * */
	public projectionOperator(operator c, ArrayList<String> attrs, ArrayList<String> schm){
		this.child=c;
		this.selectAttributes=attrs;
		this.schema=schm;
		this.newSchema=attrs;

	}
	
	/**getNextTuple() takes the next tuple from the current operator's child and filters it for the 
	 * desired columns
	 * @return a tuple with the next line from its child with columns not specified in selectAttributes
	 *  having been removed.
	 */
	public tuple getNextTuple(){
		String colTitle;
		tuple c = child.getNextTuple();

		ArrayList<Integer> values = new ArrayList<Integer>();
		if (c.getBody() != null){
			for (String item : newSchema){
				colTitle=item;

				int index = schema.indexOf(colTitle);
				
				try {
					values.add(c.getBody().get(index));
				} catch (Exception e) {
					System.err.println("Unknown column id encoutered");
					e.printStackTrace();
				}
				
			}
		} else {
			values = null;
		}

		return new tuple(values, newSchema, c.getLastTable());
	}
	
	/**reset() resets the child operator. The next call to getNextTuple() will then filter the 
	 * first tuple returned by child's getNextTuple() method again. 
	 * */
	public void reset(){
		child.reset();
	}
	
	public void reset(int i) {};
	/**	getSchema() returns the schema an oeprator uses
	 */
	public List<String> getSchema(){
		return newSchema;
	};
}