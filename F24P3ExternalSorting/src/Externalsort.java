/**
 * {Project Description Here}
 */

// On my honor:
//
// - I have not used source code obtained from another student,
// or any other unauthorized source, either modified or
// unmodified.
//
// - All source code and documentation used in my program is
// either my original work, or was derived by me from the
// source code published in the textbook for this course.
//
// - I have not discussed coding details about this project with
// anyone other than my partner (in the case of a joint
// submission), instructor, ACM/UPE tutors or the TAs assigned
// to this course. I understand that I may discuss the concepts
// of this program with other students, and that another student
// may help me debug my program so long as neither of us writes
// anything during the discussion or modifies any computer file
// during the discussion. I have violated neither the spirit nor
// letter of this restriction.

import java.io.IOException;

/**
 * The class containing the main method.
 *
 * @author kuzoto
 * @version October 2024
 */
public class Externalsort {
    
    /**
     * @param args
     *     Command line parameters
     */
    public static void main(String[] args) {
        //Create the heap for the external sort
        Record[] arrayForHeap = new Record[4096];
        MinHeap<Record> heap = new MinHeap<Record>(arrayForHeap, 0, 4096);
        //Create the processor for the input file
        ByteProcessor processor = new ByteProcessor(args[0], heap);
        try 
        {
            //outFile object
            ByteFile outFile = new ByteFile("solutionTestData/outFile.bin",
                processor.getBlocks());
            //runFile object
            ByteFile runFile = new ByteFile("solutionTestData/runFile.bin", 
                processor.getBlocks());
            //If the input file is sorted, we are done
            if (!processor.isSorted())
            {
                //Incrementer for the number of blocks per run 
                int i = 0;
                //Create the runFile using replacement selection on inputFile
                processor.readRecords();
                //Continue while the blocks per run is less than num blocks
                while ((1 << i) < processor.getBlocks())
                {
                    //Merge runs
                    processor.multiMerge((1 << i));
                    i += 3;
                    //Copy merged data to runFile
                    outFile.updateRunFile("solutionTestData/runFile.bin");
                }
                //Copy sorted data to inputFile
                runFile.updateRunFile(args[0]);
            }
            //Print the first record from each sorted block
            processor.print(args[0]);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

}
