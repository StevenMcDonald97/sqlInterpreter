package main;

public class RecordId {
	private int pageid;
	private int tupleid;
	
	/**
	 * Instantiate an rid
	 * @param p is the page id
	 * @param t is the tuple id
	 */
	public RecordId(int p, int t){
		pageid=p;
		tupleid=t;
	}
	
	/**
	 * 
	 * @return the pageid
	 */
	public int getPageId(){
		return pageid;
	}
	
	/**
	 * 
	 * @return the tupleid
	 */
	public int getTupleId(){
		return tupleid;
	}
	
}