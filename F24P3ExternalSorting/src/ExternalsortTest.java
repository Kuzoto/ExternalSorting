import student.TestCase;

/**
 * @author {Your Name Here}
 * @version {Put Something Here}
 */
public class ExternalsortTest extends TestCase {
    
    
    /**
     * set up for tests
     */
    public void setUp() {
        //nothing to set up.
    }
    
    /**
     * T
     */
    public void testExternalsort() {
        //String[] args = {"solutionTestData/sampleInput16_backup.bin"};
        //Externalsort.main(args);
        
        String[] args1 = {"solutionTestData/testInput.bin"};
        Externalsort.main(args1);
        
        //Externalsort.main(args);
    }

}
