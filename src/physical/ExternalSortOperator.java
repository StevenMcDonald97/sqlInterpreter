package physical;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import main.Interpreter;
import main.tuple;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class ExternalSortOperator extends operator {
	public LinkedList<tuple> buffer;
	int B;
	
	int tuplesize;
	static int pageSize = 4096; //page size
	static int int_size = 4; //int is 4 bytes
	
	int bufferTupleSize;
	int pageTupleSize;
	
	
	String temp_dir; //temp directory name
	File temp; //temp directory
	String temp_subDir; //temp subDirectory name
	File subTemp; //temp subDirectory
	operator op;
	List<String> or;
	String table;
	String final_file;
	

//	main.binaryTupleWriter write;
	main.binaryTupleReader tupRead;
	
	Comparator<tuple> cmp;
	
	ArrayList<String> schema;
	
	main.binaryTupleReader final_reader; 
	int filecount;
	
	/**
	 * External Sort Operator constructor initiates buffer and creates directory to put files in
	 * then sorts through an out of memory merge sort
	 * @param c is a child operator
	 * @param ol is list of attributes
	 * @param pages 
	 * @param dirname is the name of the directory to add files in
	 * @param schema
	 * @throws IOException 
	 */
	public ExternalSortOperator(operator c, List<String> ol, int pages, String dirname) throws IOException{
		//initiate variables
		op = c;
		or = ol;
		//System.out.println(ol);
		this.schema = (ArrayList<String>) c.getNextTuple().getSchema();
		c.reset();
		B = pages;
				
		//make new directory
		temp_dir = dirname;
		temp = new File(temp_dir); 
		temp.mkdir();
		temp_subDir = dirname+"/op1";
		subTemp = new File(temp_subDir); 
		
		//check what subdirectory to make for the current operator, 
		//	so scratch files don't get confused
		int open_ops=1;
//		System.out.println(subTemp.isDirectory());
		while (subTemp.exists() && subTemp.isDirectory()) {
			open_ops++;
			temp_subDir = dirname+"/op"+Integer.toString(open_ops);
			subTemp = new File(temp_subDir);
		}

		subTemp.mkdir();

        //define tuple comparator
		cmp = new Comparator<tuple>(){
			/**
			 * Comparator to compare two tuples used for sorting
			 * Also using i, accounts for next point of comparison if first priority
			 * can't be compared
			 * @return int value -1, 0 or 1 (-1 if t1<t2, 1 if t1>t2, 0 if after going through all attributes, still equal)
			 */
			@Override
			public int compare(tuple t1,tuple t2){

				int i = 0; //attribute being used
				String attribute = or.get(i); //choose first or i-th attribute from list of orderbyelements
				
				String temp = attribute;
				
				int index = t1.getSchema().indexOf(temp); //index of tuple that corresponds to specific attribute


				while (i<or.size()){

					if ((t1.getBody().get(index))<(t2.getBody().get(index)))
						return -1;
					else if ((t1.getBody().get(index))>(t2.getBody().get(index)))
						return 1;
					else{
						i++; 
						if (i<or.size()){
							attribute = or.get(i);
							temp = attribute.toString();
							index = t1.getSchema().indexOf(temp); //index of tuple that corresponds to specific attribute	
						}
						
					}
				}
				return 0;
			}


		};
		
		//ALGORITHM
		ArrayList<String> files0 = pass0(); //pass 0
//		System.out.println("\n\n\n\n\n FINISHED PASS) \n\n\n\n\n");
		for (int i = 0;i<files0.size(); i++){
			sortOneFile(files0.get(i));//in memory sort
			files0.set(i, files0.get(i)+"sorted");
		}

		//B-1 merges below
		//first merge or no merge if only one file
		ArrayList<String> firstmerge = new ArrayList<String>();
		if (files0.size()>1){
			firstmerge.add(files0.get(0));
			firstmerge.add(files0.get(1));
			mergeSortedFiles(firstmerge);
			//remaining merges

			if (files0.size()>2){
				for (int j=2; j<files0.size(); j++){
					ArrayList<String> mergefiles = new ArrayList<String>();
					mergefiles.add(temp_subDir+"/output");
					mergefiles.add(files0.get(j));
					mergeSortedFiles(mergefiles);
				}
			}

		}
		
		//case where there's only one sorted file in system
		else{
			firstmerge.add(files0.get(0));
			mergeSortedFiles(firstmerge);
		}

//		tupRead.close();
		tupRead = new main.binaryTupleReader(temp_subDir+"/output", table, schema);


	}
	
	/**
	 * method that conducts pass 0
	 * @ return list of files names generated (1, 2, 3, ....)
	 * @throws IOException 
	 */
	public ArrayList<String> pass0() throws IOException{
		//initiate readers and writers
		filecount=1;
		tuple t;
		ArrayList<String> files = new ArrayList<String>();
//		LinkedList<tuple> tuples = new LinkedList<tuple>();
		
		tuple init = op.getNextTuple(); //first tuple

		if (init.getBody()==null) return files;
		table=init.getLastTable();
		schema = (ArrayList<String>) init.getSchema();
		
		String newfile=temp_subDir+"/"+Integer.toString(filecount);
		files.add(newfile);
//		FileWriter fw = new FileWriter(newfile); //tempfile
		FileOutputStream fout = new FileOutputStream(newfile);
		FileChannel fcOut = fout.getChannel();
		

		//human read/write
		main.binaryTupleReader read = new main.binaryTupleReader(newfile, table, schema);
		
		//initialize sizes
		tuplesize = init.getBody().size();
		bufferTupleSize = (B*pageSize-8)/(4*tuplesize); 
		pageTupleSize = (pageSize-8)/(4*tuplesize); 
		
//		BufferedWriter bw = new BufferedWriter(fw);
		ByteBuffer bw = ByteBuffer.allocate( pageSize );

		main.binaryTupleWriter write = new main.binaryTupleWriter(bw);
//		respecitvely track how many tuples are in the file so far, and how many are in the bytebuffer
		int count = 0;
		int tuplesSeen=0;
		
		write.writeNextTuple(init.getBody(), (tuplesSeen*tuplesize)*4);
		count++;
		tuplesSeen++;
//		tuples.add(init);

		while ((t = op.getNextTuple()).getBody()!=null){  //take tuples from operator and distribute into files
			
//				tuples.add(t);
			write.writeNextTuple(t.getBody(), (tuplesSeen*tuplesize)*4);
			count++;
			tuplesSeen++;
			
       	
			if (tuplesSeen==pageTupleSize){
				bw.putInt(0,tuplesize);
				bw.putInt(4,tuplesSeen);
				fcOut.write( bw );

				bw.clear();
				tuplesSeen = 0;
				
			}
			if (count==bufferTupleSize) {
				filecount++; 
				newfile=temp_subDir+"/"+Integer.toString(filecount);
//				bw.close();
//				fw.close();
//				fw = new FileWriter(newfile); //tempfile
				fout.close();
				fcOut.close();
				fout = new FileOutputStream(newfile);
				fcOut = fout.getChannel();		
				files.add(newfile);
				count=0;
			}
			
		}
		if (tuplesSeen != 0) {
			bw.putInt(0,tuplesize);
			bw.putInt(4,tuplesSeen);
	//		the last filled position in the buffer is the number of tuples seen time the size of the tuples 
			int position = 4*(2+(tuplesSeen)*tuplesize);
	
			for (int k=position; k<pageSize; k++){
				bw.put(k, (byte) 0);
			}
			fcOut.write( bw );
		}
//		bw.close();
//		fw.close(); 
		fout.close();
		fcOut.close();
		read.close();

		return files;
		

	}
	

    /**
     * load lines for an individual, then sort contents of file in-memory
     * write results to corresponding temp file (will be merged later)
     * @param f is the file path to read from
     * @throws IOException 
     */
    public void sortOneFile(String f) throws IOException{
            List<tuple> tmplist = new ArrayList<tuple>();
            tuple line;
            
            main.binaryTupleReader read = new main.binaryTupleReader(f, table, schema);
            

            while ((line = read.readNextTuple()).getBody()!=null) {
            	tmplist.add(line);	

            }

            Collections.sort(tmplist, cmp);

            //put into new sorted file
            String tempfile = f+"sorted";
//            FileWriter fw = new FileWriter(tempfile);
			FileOutputStream fout = new FileOutputStream(tempfile );
			FileChannel fcOut = fout.getChannel();
//			BufferedWriter bw = new BufferedWriter(fw);
    		ByteBuffer bw = ByteBuffer.allocate( pageSize );

    		main.binaryTupleWriter write = new main.binaryTupleWriter(bw);
            int tuplesSeen = 0;
            for (int i = 0; i<tmplist.size(); i++){
            	write.writeNextTuple(tmplist.get(i).getBody(), (tuplesSeen*tuplesize)*4);
            	tuplesSeen++;

    			if (tuplesSeen==pageTupleSize){
    				bw.putInt(0,tuplesize);
    				bw.putInt(4,tuplesSeen);
    				fcOut.write( bw );
    				
    				bw.clear();
    				tuplesSeen = 0;
    			}

            	
            }
            if (tuplesSeen!=0) {
				bw.putInt(0,tuplesize);
				bw.putInt(4,tuplesSeen);
	
	//    			the last filled position in the buffer is the number of tuples seen time the size of the tuples 
				int position = 4*(2+(tuplesSeen)*tuplesize);
	
				for (int k=position; k<pageSize; k++){
					bw.put(k, (byte) 0);
				}
				fcOut.write( bw );
            }


            fout.close();
            fcOut.close();
//            bw.close(); fw.close();
            read.close();
           
    }

	
	/**
     * merges files in list to an output
     * @param buffers (where tuples are read from)
	 * @throws IOException 
     */
    public void mergeSortedFiles(ArrayList<String> files) throws IOException {

            PriorityQueue<tuple> pq = new PriorityQueue<tuple>(4096, cmp);

           //for each file, add its tuples into the priority queue
            for (String f : files) {
            	main.binaryTupleReader read = new main.binaryTupleReader(f, table, schema);
            	
    			tuple t;
            	while ((t=read.readNextTuple()).getBody()!=null) {
            		pq.add(t);
            	}
            	read.close();
            }
            
    		//write out tuples from these files to a single file
//			FileWriter fw = new FileWriter(temp_subDir+"/output");
			FileOutputStream fout = new FileOutputStream(temp_subDir+"/output");
			FileChannel fcOut = fout.getChannel();
//			BufferedWriter bw = new BufferedWriter(fw);
			ByteBuffer bw = ByteBuffer.allocate( pageSize );

			main.binaryTupleWriter write = new main.binaryTupleWriter(bw);
			
//			int count = 0;
			int tuplesSeen = 0;

            while (pq.size() > 0) {
            	tuple t = pq.poll();
            	write.writeNextTuple(t.getBody(), (tuplesSeen*tuplesize)*4); 
//            	count++;
            	tuplesSeen++;
            	
            	
    			if (tuplesSeen==pageTupleSize){
    				bw.putInt(0,tuplesize);
    				bw.putInt(4,tuplesSeen);
    				fcOut.write( bw );
    				
    				bw.clear();
    				tuplesSeen = 0;
    				
    			}
            	
            }
            if (tuplesSeen !=0) {
				bw.putInt(0,tuplesize);
				bw.putInt(4,tuplesSeen);
	//			the last filled position in the buffer is the number of tuples seen time the size of the tuples 
				int position = 4*(2+(tuplesSeen)*tuplesize);
	
				for (int k=position; k<pageSize; k++){
					bw.put(k, (byte) 0);
				}
				fcOut.write( bw );
            }
            fout.close();
            fcOut.close();
//            bw.close();fw.close();
                    
            pq.clear();

    }

	
	
	/**
	 * @return tuple when requested
	 * uses binaryTupleReader
	 */
	public tuple getNextTuple(){
		tuple next_tuple;
		
		if ((next_tuple=tupRead.readNextTuple()).getBody()!=null){ //final_reader

			try{

				return next_tuple;
			}
			catch(Exception e){
				System.err.println("Exception occurred in sort");
				e.printStackTrace();

				return new tuple();
			}
		}
		else return new tuple();
		
		//return new tuple(null, null, null);
	}
	
	/**
	 * Reset method to clear out the buffer and 
	 * also reset child operator.
	 * and close the reader
	 */
	public void reset(){
		
//		File[] files = temp.listFiles();
//	    if(files!=null) {
//	        for(File f: files) {
//	            f.delete();
//	        }
//	    }
	    //reader.close(); //binary
	    tupRead.reset(); //human
		//buffer.clear();
//		op.reset();
		
	}
	
	/**
	 * Reset method to reset operator to specific tuple
	 */	
	public void reset(int i) {
	    tupRead.reset();
	    tuple temp;

	    for (int j=0; j<i; j++) {
	    	temp=getNextTuple();
	    }
	};
	/**	getSchema() returns the schema an oeprator uses
	 */
	public List<String> getSchema(){
		System.err.println("getSchema unimplemented for this operator");
		return new ArrayList<String>();
	};

}