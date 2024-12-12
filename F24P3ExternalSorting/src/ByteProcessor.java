import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
/**
 * Processor class containing methods to sort the input file
 * 
 * @author kuzoto
 * @version October 2024
 */
public class ByteProcessor {
    /**
     * the number of records in one block
     */
    public final static int RECORDS_PER_BLOCK = 512;
    /**
     * the number of bytes in one record
     */
    public final static int BYTES_PER_RECORD = 16;
    /**
     * the number of bytes in one block
     */
    public final static int BYTES_PER_BLOCK = BYTES_PER_RECORD
        * RECORDS_PER_BLOCK;

    private String filename;
    private int numBlocks;
    private MinHeap<Record> heap;

    // ----------------------------------------------------------
    /**
     * Create a new ByteProcessor object.
     *
     * @param filename
     *            file name
     * @param heap
     *            the MinHeap for this processor
     */
    public ByteProcessor(String filename, MinHeap<Record> heap) {
        this.filename = filename;
        this.heap = heap;
    }
    
    /**
     * Get the number of blocks in the input file
     * 
     * @return
     *          The number of blocks in the input file
     * @throws IOException
     */
    public int getBlocks() throws IOException
    {
        RandomAccessFile raf = new RandomAccessFile(filename, "r");
        int blocks = 0;
        
        byte[] buffer = new byte[BYTES_PER_BLOCK];
        int bytesRead;

        //While not at the end of file increment block counter
        while ((bytesRead = raf.read(buffer)) != -1) {
            blocks++;
        }
        
        raf.close();
        return blocks;
    }
    
    // ----------------------------------------------------------
    /**
     * Read the records from the input file and 
     * create the runFile using replacement selection
     *
     * @throws IOException
     */
    public void readRecords() throws IOException {
        this.numBlocks = getBlocks();
        //System.out.println(numBlocks);
        byte[] basicBuffer = new byte[BYTES_PER_BLOCK];
        ByteBuffer bb = ByteBuffer.wrap(basicBuffer);
        File theFile = new File(filename);
        File runFile = new File("solutionTestData/runFile.bin");
        
        RandomAccessFile raf = new RandomAccessFile(theFile, "r");
        RandomAccessFile rf = new RandomAccessFile(runFile, "rw");
        rf.setLength(0);
        int i = 0;
        //Read each block in the input file
        for (int block = 0; block < numBlocks; block++) {
            bb.position(0); // resets to byte position zero in ByteBuffer
            int currBytes = raf.read(basicBuffer);
            
            if (currBytes == -1)
            {
                break;
            }
            //Build the heap with 8 blocks if heap is empty
            if (i < 8) {
                for (int rec = 0; rec < RECORDS_PER_BLOCK; rec++) 
                {
                    Record curr = new Record(bb.getLong(), bb.getDouble());
                        
                    heap.insert(curr);
                }
                i++;
            }
            else
            {
                //If the heap is not empty
                if (heap.heapSize() > 0)
                {
                    //Perform replacement selection on the heap
                    rf.seek(rf.length());
                    rf.write(replacementSelection(heap, bb).array());
                }
                else
                {
                    //Write the hidden values to the runFile
                    i = 0;
                    heap.setHeapSize(4096);
                    heap.buildHeap();
                    while (heap.heapSize() > 0)
                    {
                        rf.seek(rf.length());
                        rf.write(replacementSelection(heap).array());
                    }
                }
            }
        }
        //Write the hidden values to the run file
        heap.setHeapSize(4096);
        heap.buildHeap();
        while (heap.heapSize() > 0)
        {
            rf.seek(rf.length());
            rf.write(replacementSelection(heap).array());
        }
        raf.close(); // be sure to close file
        rf.close();
    }

    // ----------------------------------------------------------
    /**
     * checks if a file of records is sorted or not
     *
     * @return true if it is sorted, otherwise false
     * @throws IOException
     */
    public boolean isSorted() throws IOException {
        byte[] basicBuffer = new byte[BYTES_PER_BLOCK];
        ByteBuffer bb = ByteBuffer.wrap(basicBuffer);
        this.numBlocks = getBlocks();
        
        File theFile = new File(filename);
        RandomAccessFile raf = new RandomAccessFile(theFile, "r");
        raf.seek(0);
        Double prevRecKey = Double.MIN_VALUE;
        
        for (int block = 0; block < numBlocks; block++) {
            raf.read(basicBuffer);
            // ^^^ the slow, costly operation!!! Good thing we use buffer

            bb.position(0); // goes to byte position zero in ByteBuffer
            for (int rec = 0; rec < RECORDS_PER_BLOCK; rec++) {
                long recID = bb.getLong();
                // ^^^ reading the recID is important to advance the byteBuffer
                // position, but it is not used in the sort order
                double recKey = bb.getDouble();
                if (recKey < prevRecKey) {
                    raf.close();
                    return false;
                }
                else {
                    prevRecKey = recKey;
                }
            }
        }
        raf.close(); // be sure to close file
        return true;
    }
    
    /**
     * Perform replacement selection when we have an input buffer
     * 
     * @param recHeap
     *          The heap built from the input file
     * @param inputBuffer
     *          The input buffer containing a block of records
     * @return
     *          The output buffer containing replacement selected records
     */
    public ByteBuffer replacementSelection(MinHeap<Record> recHeap, 
        ByteBuffer inputBuffer)
    {
        byte[] outBuffer = new byte[8192];
        ByteBuffer ob = ByteBuffer.wrap(outBuffer);
        
        //If inputBuffer has elements still continue selection
        while (inputBuffer.hasRemaining())
        {
            //Get the min record and add it to the outBuffer
            Record min = recHeap.getMin();
            ob.putLong(min.getID());
            ob.putDouble(min.getKey());
            //Get the next record from the inputBuffer
            long currID = inputBuffer.getLong();
            double currKey = inputBuffer.getDouble();
            Record rec = new Record(currID, currKey);
            //If the next record is greater than the last added record
            if (rec.compareTo(min) > 0)
            {
                //Add the record to heap
                if (recHeap.heapSize() > 0)
                {
                    recHeap.modify(0, rec);
                }
                else
                {
                    recHeap.insert(rec);
                }
            }
            else
            {
                //Add the record to heap then hide it
                if (recHeap.heapSize() > 0)
                {
                    recHeap.modify(0, rec);
                    recHeap.removeMin();
                }
                else
                {
                    recHeap.insert(rec);
                    recHeap.removeMin();
                }
            }
        }
        return ob;
    }
    
    /**
     * Perform replacement selection with no input buffer
     * 
     * @param recHeap
     *          The heap built from the input file
     * @return
     *          A block of min records from the heap
     */
    public ByteBuffer replacementSelection(MinHeap<Record> recHeap)
    {
        byte[] outBuffer = new byte[8192];
        ByteBuffer ob = ByteBuffer.wrap(outBuffer);
        //Counter to tell how many bytes are in outBuffer
        int n = 0;
        //while the buffer is not full and heap is not empty add the min to ob
        while (n < 8192 && recHeap.heapSize() > 0)
        {
            Record min = recHeap.removeMin();
            ob.putLong(min.getID());
            ob.putDouble(min.getKey());
            //System.out.println("OutBuffer: " + ob.position());
            n += 16;
        }
        return ob;
    }
    
    /**
     * Perform multiway merge on the runFile and return to outFile
     * 
     * @param bpr
     *          The number of blocks per run
     * @throws IOException
     */
    public void multiMerge(int bpr) throws IOException
    {
        int numRuns;
        byte[] outBuffer = new byte[8192];
        ByteBuffer ob = ByteBuffer.wrap(outBuffer);
        byte[] blockBuffer = new byte[8192];
        ByteBuffer bb = ByteBuffer.wrap(blockBuffer);
        int[] currBlock;
        int[] lastBlock;
        File runFile = new File("solutionTestData/runFile.bin");
        File outFile = new File("solutionTestData/outFile.bin");
        
        MinHeap<Record> mergeHeap = new MinHeap<Record>(new Record[4096], 
            0, 4096);
        
        //Run file
        RandomAccessFile rf = new RandomAccessFile(runFile, "r");
        //Out file
        RandomAccessFile of = new RandomAccessFile(outFile, "rw");
        of.setLength(0);
        if (bpr > numBlocks || bpr == 0)
        {
            bpr = numBlocks;
        }
        if (numBlocks % bpr == 0)
        {
            numRuns = numBlocks / bpr; 
        }
        else
        {
            numRuns = (numBlocks / bpr) + 1;
        }
        
        //Counter to tell how many runs have been merged
        int currRun = 0;
        //Counter to tell how many bytes are in outBuffer
        int n = 0;
        //Perform merge on 8 runs at a time
        while (numRuns >= 8)
        {
            currBlock = new int[8];
            lastBlock = new int[8];
            
            n = 0;
            //Build the heap using the first block from 8 runs
            for (int i = 0; i < 8; i++)
            {
                //Calculate the file position for the first block of each run
                currBlock[i] = ((bpr * i) * BYTES_PER_BLOCK) 
                    + ((bpr * currRun) * BYTES_PER_BLOCK);
                rf.seek(currBlock[i]);
                bb.position(0);
                //Read in the first block for run i
                int currIn = rf.read(blockBuffer);
                //Store the number of records in the block for run i
                lastBlock[i] = (currIn / 16);
                for (int rec = 0; rec < RECORDS_PER_BLOCK; rec++) 
                {
                    Record curr = new Record(bb.getLong(), 
                        bb.getDouble(), i);
                        
                    mergeHeap.insert(curr);
                }
            }
            
            //Current run being placed in out buffer
            int block = -1;
            //Continue merging until all runs are exhausted
            while (mergeHeap.heapSize() > 0)
            {
                //Write outbuffer to outfile and clear outbuffer if it is full
                if (n >= 8192)
                {
                    of.write(outBuffer);
                    outBuffer = new byte[8192];
                    ob = ByteBuffer.wrap(outBuffer);
                    n = 0;
                }
                
                //Remove the min record and add it to outBuffer
                Record min = mergeHeap.removeMin();
                ob.putLong(min.getID());
                ob.putDouble(min.getKey());
                n += 16;
                //Get the current run
                block = min.getRun();
                //Decrement the number of records left in the heap for the run
                lastBlock[block]--;
                //If there are no records left in the heap for the run
                if (lastBlock[block] == 0)
                {
                    //Calculate the file position of the first block
                    int firstBlock = ((bpr * block) * BYTES_PER_BLOCK) 
                        + ((bpr * currRun) * BYTES_PER_BLOCK);
                    //Check if there are still blocks remaining for this run
                    if (currBlock[block] < 
                        (firstBlock + ((bpr - 1) * BYTES_PER_BLOCK)))
                    {
                        //Move to next block for this run
                        currBlock[block] += BYTES_PER_BLOCK;
                        rf.seek(currBlock[block]);
                        bb.position(0);
                        //Read the block and add it to heap
                        int currIn = rf.read(blockBuffer);
                        if (currIn != -1)
                        {
                            lastBlock[block] = (currIn / 16);
                            while (bb.position() < bb.limit()) 
                            {
                                Record curr = new Record(bb.getLong(), 
                                    bb.getDouble(), block);
                                    
                                mergeHeap.insert(curr);
                            }
                        }
                    }
                    block = -1;
                }
                
            }
            
            //Decrement number of runs by 8
            numRuns -= 8;
            //Increment number of runs merged by 8
            currRun += 8;
            
            //If there are still records in output buffer at end write them
            if (n > 0)
            {
                of.write(outBuffer);
                //ob.clear();
                outBuffer = new byte[8192];
                ob = ByteBuffer.wrap(outBuffer);
                ob.position(0);
                n = 0;
            }
            //Flush heap
            mergeHeap = new MinHeap<Record>(new Record[4096], 0, 4096);
        }
        
        currBlock = new int[numRuns];
        lastBlock = new int[numRuns];
        //Build the heap with the first block of numRuns
        for (int i = 0; i < numRuns; i++)
        {
            currBlock[i] = ((bpr * i) * BYTES_PER_BLOCK) 
                + ((bpr * currRun) * BYTES_PER_BLOCK);
            rf.seek(currBlock[i]);
            bb.position(0);
            int currIn = rf.read(blockBuffer);
            lastBlock[i] = (currIn / 16);
            for (int rec = 0; rec < RECORDS_PER_BLOCK; rec++) 
            {
                Record curr = new Record(bb.getLong(), 
                    bb.getDouble(), i);
                    
                mergeHeap.insert(curr);
            }
        }
        
        int block = -1;
        //Continue until all runs are exhausted
        while (mergeHeap.heapSize() > 0)
        {
            //If outBuffer is full write it to outFile and clear it
            if (n >= 8192)
            {
                of.write(outBuffer);
                //ob.clear();
                outBuffer = new byte[BYTES_PER_BLOCK];
                ob = ByteBuffer.wrap(outBuffer);
                n = 0;
            }
            
            
            Record min = mergeHeap.removeMin();
            ob.putLong(min.getID());
            ob.putDouble(min.getKey());
            n += 16;
            block = min.getRun();
            lastBlock[block]--;
            if (lastBlock[block] == 0)
            {
                int firstBlock = ((bpr * block) * BYTES_PER_BLOCK) 
                    + ((bpr * currRun) * BYTES_PER_BLOCK);
                if (currBlock[block] < 
                    (firstBlock + ((bpr - 1) * BYTES_PER_BLOCK)))
                {
                    currBlock[block] += BYTES_PER_BLOCK;
                    rf.seek(currBlock[block]);
                    bb.position(0);
                    int currIn = rf.read(blockBuffer);
                    if (currIn != -1)
                    {
                        lastBlock[block] = (currIn / 16);
                        while (bb.position() < bb.limit()) 
                        {
                            Record curr = new Record(bb.getLong(), 
                                bb.getDouble(), block);
                                
                            mergeHeap.insert(curr);
                        }
                    }
                }
                block = -1;
            }
            
        }
        
        if (n > 0)
        {
            of.write(outBuffer);
            //ob.clear();
            n = 0;
        }
        
        rf.close();
        of.close();
    }
    
//    private int contains(long ID, long[] lastID)
//    {
//        int i = 0;
//        for (long last : lastID)
//        {
//            if (last == ID)
//            {
//                return i;
//            }
//            i++;
//        }
//        return -1;
//    }
    
    /**
     * Print the first record from each block of a given file
     * 
     * @param file
     *          The file to print records from
     * @throws IOException
     */
    public void print(String file) throws IOException
    {
        int i = 0;
        byte[] basicBuffer = new byte[BYTES_PER_BLOCK];
        ByteBuffer bb = ByteBuffer.wrap(basicBuffer);
        RandomAccessFile rf = new RandomAccessFile(file, "r");
        
        bb.position(0);
        int currBytes = rf.read(basicBuffer);
        
        while (currBytes != -1)
        {
            System.out.print(bb.getLong() + " " + bb.getDouble() + " ");
            i++;
            if (i % 5 == 0)
            {
                System.out.println("");
            }
            bb.position(0);
            currBytes = rf.read(basicBuffer);
        }
        //System.out.println();
    }
    
}