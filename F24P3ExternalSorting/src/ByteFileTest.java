import java.io.IOException;
import student.TestCase;

/**
 * @author kuzoto
 * @version October 2024
 */
public class ByteFileTest extends TestCase {
    private ByteFile trial;
    
    /**
     * set up for tests
     */
    public void setUp() {
        trial = new ByteFile("solutionTestData/testInput.bin", 16);
    }
    
    /**
     * T
     */
    public void testIsSorted() {
        try {
            trial.writeRandomRecords();
            //System.out.println(trial.isSorted());
            
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}