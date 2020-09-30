package main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import main.BPlusTree.IndexNode;
import physical.ExternalSortOperator;
import physical.ScanOperator;
import physical.SortOperator;

public class BPlusTree {
	private int d;
	private TreeNode root;
	private String att;
	
	private int clustered;
	//private int pages;
	private String table;
	private String dataFile;
	static int pageSize = 4096; //page size
	
	private List<tuple> tuples;
	
	//variables for the layers of the tree
	private List<LeafNode> leaflayer;
	private List<IndexNode> indexlayer;
	private List<List<IndexNode>> upperlayers;
	//temporary placeholder
	private List<IndexNode> toplayer;
	public int leafNum;

	/**
	 * @param datafile is path to file for table
	 * @param table
	 * @param attribute
	 * @param schema
	 * @param d (order of tree)
	 * @param 
	 */
	public BPlusTree(String table, String attribute, int d, int clustered){
		//set variables
		this.d = d;
		this.clustered = clustered; //flag whether indexes are clustered (1 if yes, 0 if no)
		//pages = p;
		root = new LeafNode();
		att=attribute;
		databaseCatalog cat = databaseCatalog.getInstance();
		ArrayList<String> schema = cat.getSchema(table);
		this.table = table;
		this.dataFile=(cat.getBinaryFile(table)); 
		List<String> ol = new ArrayList<String>(); ol.add(att);
		
		
		binaryTupleReader br = new binaryTupleReader(dataFile, table, schema);
		tuple t;
		tuples = new ArrayList<tuple>();
		//iterate through sorted file based on attribute and add to temp list
		while ((t=br.readNextTuple()).getSchema()!=null){
			tuples.add(t);
		}
		br.reset();
		

		
		//Sort data if clustered = 1
		try {
			if (this.clustered==1){
				Collections.sort(tuples, new Comparator<tuple>(){
					/**
					 * Comparator to compare two tuples used for sorting
					 * Also using i, accounts for next point of comparison if first priority
					 * can't be compared
					 * @return int value -1, 0 or 1 (-1 if t1<t2, 1 if t1>t2, 0 if after going through all attributes, still equal)
					 */
					@Override
					public int compare(tuple t1,tuple t2){

						int i = 0; //attribute being used
						String temp = ol.get(i);
						
						int index = t1.getSchema().indexOf(temp); //index of tuple that corresponds to specific attribute
						
						while (i<ol.size()){
							
							if ((t1.getBody().get(index))<(t2.getBody().get(index)))
								return -1;
							else if ((t1.getBody().get(index))>=(t2.getBody().get(index)))
								return 1;
							else{
								i++; 
								if (i<ol.size()){
									temp = ol.get(i);
									index = t1.getSchema().indexOf(temp); //index of tuple that corresponds to specific attribute
								}
								
							}
						}
						return 0;
					}
				});
				
				
				//overwrite original file with sorted when clustered = 1
				overwrite(tuples, dataFile);
			}
			
			//Associate each key with a list of RecordIds by creating a TreeMap (sorted by keys)
			TreeMap<Integer, List<RecordId>> hm = new TreeMap<Integer, List<RecordId>>();
			hm = getTreeMap(hm, schema, tuples);

			
			
			//initiate indexlayer
			indexlayer = new ArrayList<IndexNode>();
			//initiate toplayer
			toplayer = new ArrayList<IndexNode>();
			
			//create leaf layer
			createLeafLayer(hm);
			
			
			//create index layer
			indexlayer=createIndexLayer();
			
			
			upperlayers = new ArrayList<List<IndexNode>>();
			//create upper index layer(s)
			toplayer=createTopLayer(indexlayer);
			
			upperlayers.add(toplayer);
			while (upperlayers.get(upperlayers.size()-1).size()>2*d){ //**
				upperlayers.add(createTopLayer(upperlayers.get(upperlayers.size()-1)));
			}
		
			
			//serialization
			serialize();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	
	/**
	 * Generate TreeMap containing keys and their RecordIds 
	 * @param hm empty treemap 
	 * @param schema of tuples from file
	 * @return hm with added entries
	 */
	public TreeMap<Integer, List<RecordId>> getTreeMap(TreeMap<Integer, List<RecordId>> hm, List<String> schema, List<tuple> tuples){
		int tuplesize = tuples.get(0).getBody().size();
		int tup_per_page = pageSize / (4*tuplesize);
		int pageid=0;
		int tupid=0;
		for (tuple tup: tuples){
			int k = tup.getBody().get(schema.indexOf(att));
			if (hm.keySet().contains(k)){
				
				List<RecordId> ridlist = hm.get(k);
				
				ridlist.add(new RecordId(pageid, tupid));
				hm.put(k, ridlist);
				
			}
			else {
				List<RecordId> ridlist = new ArrayList<RecordId>();
				ridlist.add(new RecordId(pageid, tupid));
				hm.put(k,ridlist);
			}
			//System.out.println(pageid+" "+tupid);
			tupid++;
			
			if (tupid>=tup_per_page-1){
				
				tupid=0; pageid++;
			}
		}
		

		
		return hm;
	}
	
	/**
	 * For clustered indexes, overwrite datafile with sorted tuples
	 * @param tuples
	 * @param datafile
	 * @throws IOException 
	 */
	public void overwrite(List<tuple> tuples, String datafile) throws IOException{
		FileOutputStream fout = new FileOutputStream(datafile);
		FileChannel fcOut = fout.getChannel();
		ByteBuffer outBuf = ByteBuffer.allocate( 4096 );
		TupleWriter writer = new binaryTupleWriter(outBuf);

		List<Integer> body;
//	    variables for buffer meta-data
		int tuple_size=0;
//		tuples seen is how many tuples have been read into the current buffer
		int tuples_seen=0;
		int buffer_capacity=0;

		for (tuple temp: tuples){
			try {
//			outBuf.clear();
			body=temp.getBody(); 
			tuple_size = body.size();
//			calculates buffer capacity as integer division of buffer size by the number of bytes in a tuple body
			buffer_capacity = 4088/(tuple_size*4);

//			write tuple into buffer. Current position in buffer should be the number of bytes in the tuples 
//			already read into the buffer
			writer.writeNextTuple(body, (tuples_seen*tuple_size)*4);
			
			tuples_seen++;
			
//			if the max number of tuples possible has been read into the buffer, write to file and reset buffer 
			if (tuples_seen==buffer_capacity){
//				write meta-data to buffer
				outBuf.putInt(0,tuple_size);
				outBuf.putInt(4,tuples_seen);

//				now write buffer to file
//				outBuf.flip();
				fcOut.write( outBuf );
				
				tuples_seen=0;
				outBuf.clear();
			} 
			} catch (Exception e){
				System.out.println("exception in dump");
				e.printStackTrace();
			}
			
		}

//		after while-loop might still have data in buffer to write to file
//		fill in metadata
		outBuf.putInt(0,tuple_size);
		outBuf.putInt(4,tuples_seen);
		
//		the last filled position in the buffer is the number of tuples seen time the size of the tuples 
		int position = 4*(2+(tuples_seen)*tuple_size);


		for (int k=position; k<4096; k++){
			outBuf.put(k, (byte) 0);
		}
		
		fcOut.write( outBuf );
		fout.close();
		
	}
	
	
	/**
	 * Serialize the tree starting with header 
	 * @throws IOException
	 */
	public void serialize() throws IOException {
		
		int num_ints_per_page = 4096/4;
		ByteBuffer buffer = ByteBuffer.allocate(4096);
		File indexPath = new File(Interpreter.getInputDir()+"/db/indexes/"+att); //***************Interpreter.getInputDir()+
		FileOutputStream fout = new FileOutputStream(indexPath);
		FileChannel fc = fout.getChannel();
		
		//*******************************header information***************************
		int offset=0;
		for (List<IndexNode> l: upperlayers){
			offset+=l.size();
		}
		int root_address = leaflayer.size()+indexlayer.size()+offset; 
		int num_leaves = leaflayer.size();
		int order = d;
		//create page for header
		//buffer.putInt(root_offset);
		
		buffer.putInt(root_address);
		buffer.putInt(num_leaves);
		buffer.putInt(order);
		
		//fill in remaining space with 0
		while (buffer.remaining()>0){
			buffer.putInt(0);	
			
		} 
		
		buffer.flip();

	    fc.write(buffer);
	    
		buffer = ByteBuffer.allocate(4096);
		
		
		//****************************leaf node information**************************
		for (int i=0; i<leaflayer.size();i++){
			buffer.putInt(0);
			buffer.putInt(leaflayer.get(i).keys.size());
			
			//for each data entry, put key, #rids for key, and pid, tid
			for(int k=0; k<leaflayer.get(i).keys.size(); k++){
				buffer.putInt(leaflayer.get(i).keys.get(k)); 
				buffer.putInt(leaflayer.get(i).values.get(k).size());
				for (RecordId rid: leaflayer.get(i).values.get(k)){
					buffer.putInt(rid.getPageId());
					buffer.putInt(rid.getTupleId());
				}
				//System.out.println();
			}
			
			//fill in remaining space with 0
			while (buffer.remaining()>0){
				buffer.putInt(0);	
				
			} 
			
			//write out
			buffer.flip();
			fc.write(buffer);
			buffer=ByteBuffer.allocate(4096);
		}
		
		
		//***************************index node information**********************************
		for (int i=0; i<indexlayer.size();i++){
			buffer.putInt(1);
			buffer.putInt(indexlayer.get(i).getChildren().size()-1);
			//put keys in order
			for (int key: indexlayer.get(i).keys){
				buffer.putInt(key);
			}
			//put children addresses in order
			for (int add: indexlayer.get(i).getChildrenAddress()){
				buffer.putInt(add); //if (i==0) System.out.println(add);
			}
			
			//fill in remaining space with 0
			while (buffer.remaining()>0){
				buffer.putInt(0);	
				
			} 
			
			//write out
			buffer.flip();
			fc.write(buffer);
			buffer=ByteBuffer.allocate(4096);
			
		}
		


		
		
		//************************* upper layers (including root) information******************************
		for (List<IndexNode> upper: upperlayers){
			toplayer = upper;
			for (int i=0; i<toplayer.size();i++){
				buffer.putInt(1);
				buffer.putInt(toplayer.get(i).getChildren().size()-1);
				//put keys in order
				for (int key: toplayer.get(i).keys){
					buffer.putInt(key); 
				}
				//put children addresses in order
				for (int add: toplayer.get(i).getChildrenAddress()){
					buffer.putInt(add); 
				}
			
				//fill in remaining space with 0
				while (buffer.remaining()>0){
					buffer.putInt(0);	
				
				} 
			
				//write out
				buffer.flip();
				fc.write(buffer);
				buffer=ByteBuffer.allocate(4096);
			}
		}
		
		
		//close writers
		fc.close();fout.close();
		
	}
	
	
	
	/**
	 * Determine which keys are in the entire source to add to leaflayer
	 * @param br Treemap of keys and record id lists
	 * @return list of keys from treemap
	 */
	public List<Integer> getAllKeys(TreeMap<Integer, List<RecordId>> br){
		List<Integer> l = new ArrayList<Integer>();
		l.addAll(br.keySet());

		return l;
	}
	
	
	public void createLeafLayer(TreeMap<Integer, List<RecordId>> hm){
		List<Integer> allKeys = getAllKeys(hm);
		int key;
		List<LeafNode> leaves = new ArrayList<LeafNode>();
		LeafNode temp = new LeafNode();
		for (int i=0; i<allKeys.size(); i++){
			key = allKeys.get(i);
			//account for case where remaining # tuples(data entries) is > 2d and <3d
			
			//add m/2 data entries
			if (allKeys.size()-i>2*d && allKeys.size()-i<3*d){
				for (int j = i; j<(allKeys.size()-i)/2+i; j++){	
					key = allKeys.get(j);
					temp.insertEntry(key, hm.get(key));

				}
				leaves.add(temp); 
				temp = new LeafNode();

				//add remaining to last
				for (int j = i+(allKeys.size()-i)/2; j<allKeys.size(); j++){
					key = allKeys.get(j);
					temp.insertEntry(key, hm.get(key));
				}
				leaves.add(temp); 
				temp = new LeafNode();
				leaflayer=leaves;
				
				break;
			}
			//otherwise add 2d keys per node
			else{
				
				for (int j= i; j<i+2*d; j++){
					if (j>=allKeys.size()) break;
					key = allKeys.get(j);
					temp.insertEntry(key, hm.get(key));
				}
				
				i=i+2*d-1; 
				leaves.add(temp);
				temp = new LeafNode();
			 }

		}
		leaflayer = leaves;
		leafNum=leaflayer.size();

	}

	/**
	 * Create the index layer right above the leaf node layer
	 * @param lowerlayer is the lower leaf layer
	 */
	public List<IndexNode> createIndexLayer(){
		
		List<IndexNode> outlayer = new ArrayList<IndexNode>();
		IndexNode in = new IndexNode();

		for (int i=0; i<leaflayer.size(); i++){
			//If there are 2 nodes left and have m leaf nodes left where 2d+1<m<3d+2
			if (leaflayer.size()-i>2*d+1 && leaflayer.size()-i<3*d+2){
				
				//second to last node gets m/2
				for (int j = i; j<i+(leaflayer.size()-i)/2; j++){
					in.addChild(leaflayer.get(j));
					in.childaddresses.add(j+1); //add address of child
					if (j>i) in.insertEntry(leaflayer.get(j).getFirstLeafKey());
				}
				outlayer.add(in);
				
				//last node get remaining
				in = new IndexNode();
				for (int k = i+(leaflayer.size()-i)/2; k<leaflayer.size(); k++){
					
					in.addChild(leaflayer.get(k));
					in.childaddresses.add(k+1); //add address of child
					if (k>i+(leaflayer.size()-i)/2) in.insertEntry(leaflayer.get(k).getFirstLeafKey());
				}
//				in.insertEntry(in.getChildren().get(in.getChildren().size()-2).getFirstLeafKey()); //add key (first key of last node)
				outlayer.add(in);
				

				in = new IndexNode();

				return outlayer;
				
			}
			else{ //add nodes as normal
				
				//keep track of how many children added to a single node (so that 2d+1 is put in)
				int countadded=0;
				
				while (countadded<2*d+1){

					in.addChild(leaflayer.get(i));
					in.childaddresses.add(i+1); //add address of child
					if (countadded>0) in.insertEntry(leaflayer.get(i).getFirstLeafKey());
					i++; countadded++;

					if (i>=leaflayer.size()) break;
				}
				i--;
				outlayer.add(in);


				in = new IndexNode();
				
			}
		}

		return outlayer;
	}
	
	
	
	/**
	 * Create an index layer above the first index node layer
	 * @param lowerlayer is the index layer below
	 */
	public List<IndexNode> createTopLayer(List<IndexNode> lowerlayer){
		List<IndexNode> outlayer = new ArrayList<IndexNode>();
		IndexNode in = new IndexNode();
		int offset=0;
		for (int i=0; i<lowerlayer.size(); i++){
			
			//If there are 2 nodes left and have m leaf nodes left where 2d+1<m<3d+2
			if (lowerlayer.size()-i>2*d+1 && lowerlayer.size()-i<3*d+2){
				
				//second to last node gets m/2
				for (int j = i; j<i+(lowerlayer.size()-i)/2; j++){
					in.addChild(lowerlayer.get(j));
					
					offset=0; //address offset
					if (upperlayers.size()>0)
					for (List<IndexNode> l: upperlayers){
						offset+=l.size();
					}
					in.childaddresses.add(j+1+lowerlayer.size()+offset); //add address of child
					TreeNode temp = lowerlayer.get(i);
					while (!temp.isLeafNode()){
						temp = temp.getChildren().get(0);
					}
					in.insertEntry(temp.getFirstLeafKey());
				}
				outlayer.add(in);
				
				//last node get remaining
				in = new IndexNode();
				for (int j = i+(lowerlayer.size()-i)/2; j<lowerlayer.size(); j++){
					
					in.addChild(lowerlayer.get(j));
					
					offset=0; //address offset
					if (upperlayers.size()>0)
					for (List<IndexNode> l: upperlayers){
						offset+=l.size();
					}
					in.childaddresses.add(j+1+lowerlayer.size()+offset); //add address of child
					TreeNode temp = lowerlayer.get(i);
					while (!temp.isLeafNode()){
						temp = temp.getChildren().get(0);
					}
					in.insertEntry(temp.getFirstLeafKey());
				}

				outlayer.add(in);
				return outlayer;
				
			}
			else{ //add nodes as normal
				
				//keep track of how many children added to a single node (so that 2d+1 is put in)
				int countadded=0;
				
				while (countadded<2*d+1){
					
					in.addChild(lowerlayer.get(i));
					
					offset=0; //address offset
					if (upperlayers.size()>0)
					for (List<IndexNode> l: upperlayers){
						offset+=l.size();
					}
					in.childaddresses.add(i+1+leaflayer.size()+offset); //add address of child
					if (countadded>0){
						TreeNode temp = lowerlayer.get(i);
						while (!temp.isLeafNode()){
							temp = temp.getChildren().get(0);
						}
						in.insertEntry(temp.getFirstLeafKey());
					}
					i++; countadded++;
					if (i>=lowerlayer.size()) break;
				}
				//in.insertEntry(in.getFirstKey());
				//i--;//******************
				outlayer.add(in);
				in = new IndexNode();
				
			}
		}
		return outlayer;
	}
	
	
	/**
	 * print tree (used for debugging)
	 * @param tree
	 * @throws IOException
	 */
	public void print_tree() throws IOException{
		System.out.println(toplayer.size());
		for (int i=0; i<toplayer.size(); i++){
			System.out.print("toplayer node: "+toplayer.get(i).keys+" "+toplayer.get(0).children.size() + " ");

		}

		System.out.println();
		for (int i=0; i<toplayer.size(); i++){
			System.out.print(toplayer.get(i).getChildrenAddress());
		}
		System.out.println();
//		for (int i=0; i<indexlayer.size(); i++){
//			System.out.print(indexlayer.get(i).getFirstKey()+" ");
//		}
//		System.out.println();
		for (int i=0; i<indexlayer.size(); i++){
			System.out.print(indexlayer.get(i).getChildrenAddress());
		}
//		for (int i=0;i<leaflayer.get(0).keys.size();i++){
//			System.out.println(leaflayer.get(0).keys.get(i));
//		}
		System.out.println();

		for (int i=0; i<leaflayer.size(); i++){
			System.out.print(leaflayer.get(i).getFirstLeafKey()+" ");
		}
		System.out.println();
		for (int i=0; i<leaflayer.size(); i++){
			if (i==leaflayer.size()-1){
				for (int j=0; j<leaflayer.get(i).keys.size(); j++){

					System.out.println("key: "+leaflayer.get(i).keys.get(j));
					System.out.println("values: "+leaflayer.get(i).values.get(j).get(0).getPageId()+" "+leaflayer.get(i).values.get(j).get(0).getTupleId());
				}
			}
		}
		System.out.println(leaflayer.size());


		

	}
	
	

	
	
	/**
	 * defines node that is in leaf layer 
	 *
	 */
	public class LeafNode extends TreeNode {
		List<Integer> keys;
		List<List<RecordId>> values;
		
		/**
		 * instantiate key, value pairs
		 */
		LeafNode(){
			keys = new ArrayList<Integer>();
			values = new ArrayList<List<RecordId>>();
		}
		
		/**
		 * 
		 * @return whether node is a leaf node
		 */
		boolean isLeafNode(){
			return true;
		}
		
		/**
		 * @return List<RecordId> corresponding to key
		 */
		List<RecordId> getValue(int key) {
			// TODO Auto-generated method stub
			for (int i = 0; i<keys.size(); i++){
				if (keys.get(i)==key) return values.get(i);
			}
			return null;
		}

		/**
		 * delete value corresponding to key
		 */
		void deleteValue(int key) {
			// TODO Auto-generated method stub
			for (int i=0; i<keys.size(); i++){
				if (keys.get(i)==key){
					keys.remove(i);
					values.remove(i);
				}
			}
		}
		
		/**
		 * return list of children
		 * @return
		 */
		List<TreeNode> getChildren(){
			return null;
		}

		/**
		 * return value of first leaf key of the node
		 */
		int getFirstLeafKey() {
			// TODO Auto-generated method stub
			return keys.get(0);
		}

		/**
		 * insert a key, value pair into the node
		 */
		void insertEntry(int key, List<RecordId> value) {
			// TODO Auto-generated method stub
			keys.add(key);
			values.add(value);
		}
		
	}
	

	/**
	 * define index node for index layers
	 *
	 */
	public class IndexNode extends TreeNode {
		List<Integer> keys;
		//IndexNode parent;
		List<TreeNode> children;
		List<Integer> childaddresses;
		
		/**
		 * instantiate the keys for the node
		 */
		IndexNode(){
			keys = new ArrayList<Integer>();
			children = new ArrayList<TreeNode>();
			childaddresses = new ArrayList<Integer>();
		}
		
		/**
		 * 
		 * @return whether node is a leaf node
		 */
		boolean isLeafNode(){
			return false;
		}
		
		/**
		 * delete key from keys
		 */
		void deleteValue(int key) {
			// TODO Auto-generated method stub
			for (int i=0; i<keys.size(); i++){
				if (keys.get(i)==key){
					keys.remove(i);
				}
			}
		}
		
		/**
		 * insert a child leafnode
		 * @param child
		 */
		void addChild(TreeNode child){
			children.add(child);
		}
		
		/**
		 * return list of children
		 * @return
		 */
		List<TreeNode> getChildren(){
			return children;
		}
		
		List<Integer> getChildrenAddress(){
			return childaddresses;
		}

		/**
		 * return first key of the node
		 * @return
		 */
		int getFirstKey() {
			// TODO Auto-generated method stub
			return keys.get(0);
		}

		void insertEntry(int key) {
			// TODO Auto-generated method stub
			keys.add(key);
			
		}

		/**
		 * return List<RecordId> associated with the key
		 * unused
		 */
		@Override
		List<RecordId> getValue(int key) {
			// TODO Auto-generated method stub
			return null;
		}

		/**
		 * unused
		 */
		@Override
		int getFirstLeafKey() {
			// TODO Auto-generated method stub
			return keys.get(0);
		}

		/**
		 * unused
		 */
		@Override
		void insertEntry(int key, List<RecordId> value) {
			// TODO Auto-generated method stub
			
		}

	}
	
}