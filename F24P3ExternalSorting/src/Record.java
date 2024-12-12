/**
 * Holds a single record
 *
 * @author CS Staff
 * @version Fall 2024
 */
public class Record
    implements Comparable<Record>
{
    /**
     * 16 bytes per record
     */
    public static final int BYTES = 16;

    private long            recID;
    private double          key;
    private int             run;

    /**
     * The constructor for the Record class
     *
     * @param recID
     *            record ID
     * @param key
     *            record key
     */
    public Record(long recID, double key)
    {
        this.recID = recID;
        this.key = key;
        this.run = -1;
    }
    
    /**
     * The constructor for the Record class
     *
     * @param recID
     *            record ID
     * @param key
     *            record key
     * @param run
     *            record run
     */
    public Record(long recID, double key, int run)
    {
        this.recID = recID;
        this.key = key;
        this.run = run;
    }


    // ----------------------------------------------------------
    /**
     * Return the ID value from the record
     *
     * @return record ID
     */
    public long getID()
    {
        return recID;
    }


    // ----------------------------------------------------------
    /**
     * Return the key value from the record
     *
     * @return record key
     */
    public double getKey()
    {
        return key;
    }
    
 // ----------------------------------------------------------
    /**
     * Return the run value from the record
     *
     * @return record run
     */
    public int getRun()
    {
        return run;
    }


    // ----------------------------------------------------------
    /**
     * Compare two records based on their keys
     *
     * @return int
     */
    @Override
    public int compareTo(Record toBeCompared)
    {
        return Double.compare(this.key, toBeCompared.key);
    }
}
