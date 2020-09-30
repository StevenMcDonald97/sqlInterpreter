package physical;

import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import main.TupleReader;
import main.tuple;
import java.util.ArrayList;

import main.TupleReader;
import main.binaryTupleReader;
import main.databaseCatalog;

public class IndexScan extends operator{

//	private String table;
	private String dataFile;
//	tuple reader on datafile
	private TupleReader reader;
//	tuple schema
	private ArrayList<String> schema;
//	boolean flag, true if index is clustered 
	boolean clustered;
	int columnInd;
//	track which tuple is currently being looked at
	private int position;
//	the catalog for database files
	databaseCatalog cat = databaseCatalog.getInstance();
	
	private int lowKey;
	private int highKey;
	
//	keep file channel open on data file
	private FileInputStream indexIn;
	private FileChannel fcIndex;
	private String indexFile;
//	create buffer to store bytes
	public ByteBuffer indexBuf;
		
//	keep file channel open on data file
//	private FileInputStream dataIn;
//	private FileChannel fcData;
//	private String fpath;
	
	
//	store data about tree
//	how many leaves are in the tree
	int leafNum;
	int leavesSeen;
	int firstLeaves;
	
//	bound for how many nodes must be in each level
	int order;
//	the leaf or index being looked at currently
	int currentPage;
	int firstPage;

//	how many keys are on this page
	int numPageEntries;
	int firstPageEntries;

//	position of current data entry (key) in leaf 
	int dataEntryNum;
	int firstDataEntryNum;

//	which rid was seen last from the current key
	int allRidsForKey;
	int ridNum;
	int firstRidNum;

//	track where the current key is in the buffer
	int key_position;
	int first_key_position;
	
	/**IndexScan(String table, String index, ArrayList<String> Schema, int low, int high)
	 *  creates an IndexScan operator returning tuples from low key to high key from indexFile
	 *  @param table is the relation being scanned
	 *  @param index is the file where the index on table is stored 
	 *  @param schema is the tuple schema for the relation
	 *  @param low is the lower boundary (inclusive) on the index for returning tuples 
	 *  @param high is the upper boundary (inclusive) on the index for returning tuples 
	 *  @param cluster is true if the index is clsutered, or false if unclustered
	 */
	public IndexScan(String t, String index, ArrayList<String> Schema, Integer low, Integer high, Integer indexPos, boolean cluster) {

		this.schema=Schema;
		this.indexFile=index;
		this.columnInd=indexPos;
		this.clustered =cluster;
		this.dataFile=(cat.getBinaryFile(t));
		this.schema=Schema;
		

	

//		try {
//			this.reader = new humanTupleReader(this.dataFile, table, schema);
//		} catch (Exception e) {
//			System.err.println("Exception occurred opening file reader");
//			e.printStackTrace();
//		}

		try {
			this.reader = new binaryTupleReader(this.dataFile, t, schema);
		} catch (Exception e) {
			System.err.println("Exception occurred opening file reader for indexScan");
			e.printStackTrace();
		}
		
//		if either low or high is null, set bound so that all key from start or to end respectively
//		are considered
		this.lowKey=low;
		this.highKey=high;

//		open reader on index file, find page and location of first data key
		try {
			this.indexIn = new FileInputStream(index);
			this.fcIndex = indexIn.getChannel();
			this.indexBuf = ByteBuffer.allocate( 4096 );
			this.indexBuf.clear();
		    int r = fcIndex.read(indexBuf);

		    if (r!=-1) {
//				     read in the size and number of tuples
		    	int root=indexBuf.getInt(0);

		    	leafNum = indexBuf.getInt(4);
				
//		    	move point in file channel on index to root page. Read in root page to buffer
//		    	Long root_position = Long.valueOf(root*4096);
//		    	fcIndex.position(root_position);
//		    	indexBuf.clear();
//		    	fcIndex.read(indexBuf);
		    	loadNextPage(root);
		    	
		    	int nodeType = indexBuf.getInt(0);
//		    	int numberEntries=indexBuf.getInt(4);
		    	
//		    	iterate until a leaf node is found 
		    	while (nodeType==1) {
		    		int posInBuffer = 8;
		    		int currentKey = indexBuf.getInt(posInBuffer);

		    		int keyNum=0;
//		    		as long as the low key is more than the current key, and not every key has been seen
//		    		be careful not to overflow into the child addresses!
		    		while (lowKey>currentKey && posInBuffer<8+this.numPageEntries*4) {
//		    			move to next int in buffer
		    			posInBuffer+=4;
		    			keyNum+=1;
		    			currentKey = indexBuf.getInt(posInBuffer);

		    		}

//		    		now we have the number of the child we want, so we use it to find the 
//		    		next data page to load. Offset by 8 and number of keys to find child address
		    		int childPage = indexBuf.getInt(8+4*(this.numPageEntries+keyNum));
		    		
//		    		load child page

			    	loadNextPage(childPage);

//		    		read child page into buffer
//			    	Long childPosition = Long.valueOf(childPage*4096);
//			    	fcIndex.position(childPosition);
//			    	indexBuf.clear();
//			    	fcIndex.read(indexBuf);
//			    	update node information
//			    	numberEntries=indexBuf.getInt(4);
			    	nodeType = indexBuf.getInt(0);
			    	
		    	}
		    	
		    	leavesSeen = 1;
//		    	start by looking at first data entry on leaf page
		    	dataEntryNum =0;
		    	
//		    	now loop until you find first key satisfying the lowkey bound
//		    	track entries seen in case no key satsify bound
		    	int entriesSeen=0;
//		    	first key will be the third integer in the buffer
		    	this.key_position=8;
		    	int key = indexBuf.getInt(key_position);
		    	int numRids = indexBuf.getInt(key_position+4);

//		    	loop until you find first key within bounds
		    	while (entriesSeen<numPageEntries && lowKey>key) {
//		    		the next key comes after the number of rids, and each rid in the current key
		    		key_position=key_position+8+numRids*8;
			    	key = indexBuf.getInt(key_position);
			    	numRids = indexBuf.getInt(key_position+4);
		    		entriesSeen++;
		    	}
		    	
//		    	for loading new key, update which data entry the key is at, how many rids it has
//		    	and start at the first rid
		    	this.firstPage=currentPage;
		    	this.first_key_position=key_position;
		    	this.dataEntryNum=entriesSeen;
		    	this.firstDataEntryNum=entriesSeen;
		    	this.allRidsForKey = numRids;
		    	this.ridNum=0;
		    	
		    	if (clustered) {
		    		int pageNum = indexBuf.getInt(key_position+ridNum*8+8);
		    		int tuplePos = indexBuf.getInt(key_position+ridNum*8+12);

		    		reader.setFileReaderPosition(pageNum, tuplePos);
		    	}

			 } else {
				 System.err.println("Error traversing tree index to leaf node");
				 indexIn.close();
			 }		
				
		} catch (Exception e) {
			System.err.println("Exception occurred opening file reader");
			e.printStackTrace();
		}

	}

	/**getNextTuple() finds and returns the next tuple from the index in the selected range.
	 * Assume that indexbuf is currently loaded with a 
	 * @return the next tuple of data between the low and high keys (inclusive), or an empty tuple
	 * if no more tuples exist between these bounds
	 */
	public tuple getNextTuple() {
//		if every rid for the current key is seen, take next key
//		the next key is at the current key_position plus 8 bytes for the key metadata plus the number of
//		rids times the number of bytes in each rid (8) 

//		if unclustered, each call to getNextTuple loads a new page of the data file
		if (!clustered) {
			if(ridNum>=allRidsForKey) {
				key_position=key_position+8+allRidsForKey*8;
				allRidsForKey=indexBuf.getInt(key_position+4);
				ridNum=0;
				dataEntryNum++;
			}	
			
	//		if every key is seen and every rid for the current key is seen, load next page
			if (dataEntryNum>=numPageEntries) {
	//			next leaf page is consecutive after the previous
				loadNextPage(currentPage+1);
			}
			
	//		if there are no more leaf pages (leaked into index pages), or high key is reached, 
	//		return an empty tuple 
			if (indexBuf.getInt(key_position)>highKey || indexBuf.getInt(0)==1) {
				return new tuple();
			}
			
	//		else take next tuple
	//		find how many rids there are for current key
			
	//		pageId is at key position, plus the number of Rids seen*size of each rid
	//		plus 8 for the key value and number of rid fields
			int pageRid = indexBuf.getInt(key_position+ridNum*8+8);
			int tupleLocation = indexBuf.getInt(key_position+ridNum*8+12);
	
	
			
	//		increment pointer to rid
			ridNum++;
			
	//		need to track: current page, number of leaf pages seen, current key, number of keys seen,
	//		number of rids seen for this key
	//		System.out.println(pageRid);
			return reader.getSpecificTuple(pageRid, tupleLocation);
//		if clustered, just read next tuple from current position in file
		} else {

//			check if next tuple satisfies constraints
			tuple nextTup = reader.readNextTuple();
			
			if (nextTup.getBody()==null || nextTup.getBody().get(columnInd)>highKey || indexBuf.getInt(0)==1) {
				return new tuple();
			} else {
				return nextTup;
			}
		}
	}

	@Override
	/**reset returns the index to the first key within the desired bounds, including resetting
	 * all metadata values to their original values
	 */
	public void reset() {
		// TODO Auto-generated method stub
//		System.err.println("indexScan reset unimplemented!");

		try {
	    	loadNextPage(firstPage);
	    	key_position=first_key_position;
	    	ridNum=0;
	    	allRidsForKey=indexBuf.getInt(key_position+4);
	    	leavesSeen = 1;
	    	dataEntryNum=firstDataEntryNum;

	    	if (clustered) {
	    		int pageNum = indexBuf.getInt(key_position+ridNum*8+8);
	    		int tuplePos = indexBuf.getInt(key_position+ridNum*8+12);
	    		reader.reset();
	    		reader.setFileReaderPosition(pageNum, tuplePos);
	    	}
	    	
		} catch (Exception e){
			System.err.println("Exception occurred resetting indexScan");
			e.printStackTrace();
		}
//		reset: ridNum, allRidsForKey, key_position, indexBuf, dataEntryNum, currentPage
		

	}
	
	/** loadNextPage(int pageNum) loads the page specified by pageNum into indexBuf
	 * 
	 * @param pageNum is the number of the page to be retrieved
	 */
	private void loadNextPage(int pageNum) {
//		System.out.println(pageNum);
//		System.out.println(pageNum);

    	Long page_position = Long.valueOf(pageNum*4096);
    	currentPage = pageNum;
    	
    	try {
	    	fcIndex.position(page_position);
	    	indexBuf.clear();
	    	fcIndex.read(indexBuf);

//	    	if its a leaf, increment leaf information 
	    	if (indexBuf.getInt(0)== 0) {
	    		leavesSeen++;
	    		key_position=8;
		    	allRidsForKey = indexBuf.getInt(key_position+4);
				ridNum=0;

	    	} 
	    	numPageEntries=indexBuf.getInt(4);
			dataEntryNum=0;

		} catch (Exception e) {
			System.err.println("Exception occurred loading next page");
			e.printStackTrace();
		}
	}

	@Override
	public void reset(int i) {
		// TODO Auto-generated method stub
		System.err.println("indexScan reset(i) unimplemented!");

	}
	
	/**	getSchema() returns the schema an oeprator uses
	 */
	public List<String> getSchema(){
		return schema;
	};

}
