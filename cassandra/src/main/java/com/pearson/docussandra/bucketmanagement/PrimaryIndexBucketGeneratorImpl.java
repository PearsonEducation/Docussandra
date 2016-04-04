package com.pearson.docussandra.bucketmanagement;

import com.pearson.docussandra.Utils;
import com.pearson.docussandra.domain.objects.FieldDataType;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for generating our bucket lists. This (probably) will NOT be executed
 * by Docussandra it self, but as a separate run prior to actual Docussandra
 * startup. This should only need to be run once (total) per datatype; the
 * results can be saved off and packaged with the app. Writes results to the
 * file system as a CSV with an extension of .buckets.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class PrimaryIndexBucketGeneratorImpl {

    /**
     * Logger for this class.
     */
    private static Logger logger = LoggerFactory.getLogger(PrimaryIndexBucketGeneratorImpl.class);
    /**
     * Comma delimiter.
     */
    public static final String DE = ",";//delimiter

    /**
     * Main method.
     *
     * @param args Command line arguments. Should be a single parameter in the
     * form of a FieldDataType.
     */
    public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
        PrimaryIndexBucketGeneratorImpl gen = new PrimaryIndexBucketGeneratorImpl(args);
        gen.generateAndWrite();
    }

    //start input tweaks
    //number of buckets for each data type    
    public final static int TEXT_BUCKET_SIZE = 50000;
    public final static int DATE_TIME_BUCKET_SIZE = Integer.MAX_VALUE / 96;
    public final static int INTEGER_BUCKET_SIZE = 5000000;
    public final static int DOUBLE_BUCKET_SIZE = 1000000;
    public final static int BOOLEAN_BUCKET_SIZE = 2;
    public final static int UUID_BUCKET_SIZE = 10000;
    public final static int BINARY_BUCKET_SIZE = 500000;//seems about right
    public final static int LONG_BUCKET_SIZE = Integer.MAX_VALUE / 96;
    public final static int TIMEPOINT_BUCKET_SIZE = 7889760;//one bucket per two minutes

    //prune factors for each data type; if 0; no pruning -- the smaller the prune factor, the more it prunes
    public final static double TEXT_PRUNE_FACTOR = .85;
    public final static double DATE_TIME_PRUNE_FACTOR = .0005;
    public final static double INTEGER_PRUNE_FACTOR = .01;
    public final static double DOUBLE_PRUNE_FACTOR = .01;
    public final static double BOOLEAN_PRUNE_FACTOR = 0;
    public final static double UUID_PRUNE_FACTOR = 0;
    public final static double BINARY_PRUNE_FACTOR = 0;
    public final static double LONG_PRUNE_FACTOR = .0005;
    public final static double TIMEPOINT_PRUNE_FACTOR = 0;

    //upper range for the <b>value</b> of each bucket
    public final static Long TEXT_MAX = Long.MAX_VALUE;
    public final static Long DATE_TIME_MAX = Long.MAX_VALUE / 128;
    public final static Long DOUBLE_MAX = Long.MAX_VALUE;
    public final static Long INTEGER_MAX = new Long(Integer.MAX_VALUE);
    public final static Long BOOLEAN_MAX = 1l;//your options are 0 and 1
    public final static Long UUID_MAX = Long.MAX_VALUE;
    public final static Long BINARY_MAX = 0x7a7a7a7a7a7a7a7al;//zzzzzzzz -- highest base 64 value we could have
    public final static Long LONG_MAX = Long.MAX_VALUE / 128;
    public final static Long TIMEPOINT_MIN = 946684800000l; //1 JAN 2000 -- minimum date we will gen buckets for
    public final static Long TIMEPOINT_MAX = 1893542400000l - TIMEPOINT_MIN;//1 JAN 2030 offset

    //if true, we use negative buckets, if false, we only use positive buckets
    public final static boolean TEXT_NEG = false;
    public final static boolean DATE_NEG = true;
    public final static boolean DOUBLE_NEG = true;
    public final static boolean INTEGER_NEG = true;
    public final static boolean BOOLEAN_NEG = false;
    public final static boolean UUID_NEG = false;
    public final static boolean BINARY_NEG = false;
    public final static boolean LONG_NEG = true;
    public final static boolean TIMEPOINT_NEG = false;

    //end input tweaks
    /**
     * Number of buckets for this run.
     */
    private final int bucketSize;

    /**
     * The prune factor for this run.
     */
    private final double pruneFactor;

    /**
     * Max value of the buckets for this run.
     */
    private final Long bucketMax;

    /**
     * Flag for using negative buckets for this run.
     */
    private final boolean useNegativeBuckets;

    /**
     * Data type for this run.
     */
    private final FieldDataType dataType;

    /**
     * Object constructor.
     *
     * @param args
     */
    public PrimaryIndexBucketGeneratorImpl(String[] args) {
        dataType = parseArgs(args);
        switch (dataType) {
            case TEXT:
                bucketSize = TEXT_BUCKET_SIZE;
                pruneFactor = TEXT_PRUNE_FACTOR;
                bucketMax = TEXT_MAX;
                useNegativeBuckets = TEXT_NEG;
                break;
            case BOOLEAN:
                bucketSize = BOOLEAN_BUCKET_SIZE;
                pruneFactor = BOOLEAN_PRUNE_FACTOR;
                bucketMax = BOOLEAN_MAX;
                useNegativeBuckets = BOOLEAN_NEG;
                break;
            case DATE_TIME:
                bucketSize = DATE_TIME_BUCKET_SIZE;
                pruneFactor = DATE_TIME_PRUNE_FACTOR;
                bucketMax = DATE_TIME_MAX;
                useNegativeBuckets = DATE_NEG;
                break;
            case LONG:
                bucketSize = LONG_BUCKET_SIZE;
                pruneFactor = LONG_PRUNE_FACTOR;
                bucketMax = LONG_MAX;
                useNegativeBuckets = LONG_NEG;
                break;
            case DOUBLE:
                bucketSize = DOUBLE_BUCKET_SIZE;
                pruneFactor = DOUBLE_PRUNE_FACTOR;
                bucketMax = DOUBLE_MAX;
                useNegativeBuckets = DOUBLE_NEG;
                break;
            case INTEGER:
                bucketSize = INTEGER_BUCKET_SIZE;
                pruneFactor = INTEGER_PRUNE_FACTOR;
                bucketMax = INTEGER_MAX;
                useNegativeBuckets = INTEGER_NEG;
                break;
            case UUID:
                bucketSize = UUID_BUCKET_SIZE;
                pruneFactor = UUID_PRUNE_FACTOR;
                bucketMax = UUID_MAX;
                useNegativeBuckets = UUID_NEG;
                break;
            case BINARY:
                bucketSize = BINARY_BUCKET_SIZE;
                pruneFactor = BINARY_PRUNE_FACTOR;
                bucketMax = BINARY_MAX;
                useNegativeBuckets = BINARY_NEG;
                break;
            case TIMEPOINT:
                bucketSize = TIMEPOINT_BUCKET_SIZE;
                pruneFactor = TIMEPOINT_PRUNE_FACTOR;
                bucketMax = TIMEPOINT_MAX;
                useNegativeBuckets = TIMEPOINT_NEG;
                break;
            default:
                throw new UnsupportedOperationException("DataType: " + dataType.toString() + " is not supported. This is probably a bug.");
        }
    }

    /**
     * Object constructor for tests.
     *
     * @param dataType to test
     * @param bucketSize override the default bucketsize
     */
    @Deprecated//test only
    public PrimaryIndexBucketGeneratorImpl(FieldDataType dataType, int bucketSize) throws UnsupportedOperationException {
        this.dataType = dataType;
        if (dataType.equals(FieldDataType.TEXT)) {
            this.bucketSize = bucketSize;
            pruneFactor = TEXT_PRUNE_FACTOR;
            bucketMax = TEXT_MAX;
            useNegativeBuckets = TEXT_NEG;

        } else if (dataType.equals(FieldDataType.BOOLEAN)) {
            this.bucketSize = bucketSize;
            pruneFactor = BOOLEAN_PRUNE_FACTOR;
            bucketMax = BOOLEAN_MAX;
            useNegativeBuckets = BOOLEAN_NEG;

        } else if (dataType.equals(FieldDataType.DATE_TIME)) {
            this.bucketSize = bucketSize;
            pruneFactor = DATE_TIME_PRUNE_FACTOR;
            bucketMax = DATE_TIME_MAX;
            useNegativeBuckets = DATE_NEG;
        } else if (dataType.equals(FieldDataType.LONG)) {
            this.bucketSize = bucketSize;
            pruneFactor = LONG_PRUNE_FACTOR;
            bucketMax = LONG_MAX;
            useNegativeBuckets = LONG_NEG;
        } else if (dataType.equals(FieldDataType.DOUBLE)) {
            this.bucketSize = bucketSize;
            pruneFactor = DOUBLE_PRUNE_FACTOR;
            bucketMax = DOUBLE_MAX;
            useNegativeBuckets = DOUBLE_NEG;
        } else if (dataType.equals(FieldDataType.INTEGER)) {
            this.bucketSize = bucketSize;
            pruneFactor = INTEGER_PRUNE_FACTOR;
            bucketMax = INTEGER_MAX;
            useNegativeBuckets = INTEGER_NEG;
        } else if (dataType.equals(FieldDataType.UUID)) {
            this.bucketSize = bucketSize;
            pruneFactor = UUID_PRUNE_FACTOR;
            bucketMax = UUID_MAX;
            useNegativeBuckets = UUID_NEG;
        } else if (dataType.equals(FieldDataType.BINARY)) {
            this.bucketSize = bucketSize;
            pruneFactor = BINARY_PRUNE_FACTOR;
            bucketMax = BINARY_MAX;
            useNegativeBuckets = BINARY_NEG;
        } else if (dataType.equals(FieldDataType.TIMEPOINT)) {
            this.bucketSize = bucketSize;
            pruneFactor = TIMEPOINT_PRUNE_FACTOR;
            bucketMax = TIMEPOINT_MAX;
            useNegativeBuckets = TIMEPOINT_NEG;
        } else {
            throw new UnsupportedOperationException("DataType: " + dataType.toString() + " is not supported. This is probably a bug.");
        }
    }

    /**
     * Does the actual work.
     */
    public List<Long> generateBuckets() throws FileNotFoundException, UnsupportedEncodingException {
        logger.info("Generating buckets for: " + dataType.toString() + " with prune factor: " + pruneFactor);
        List<Long> buckets = createBuckets();
        System.gc();
        if (pruneFactor != 0) {
            buckets = pruneBucket(buckets, pruneFactor);
            System.gc();
        }
        if (dataType.equals(FieldDataType.DOUBLE)) {
            logger.info("Special case for double; adding a set of low buckets for high precision on lower numbers.");
            for (int i = 0; i < 10000; i++) {
                logger.debug("Adding " + i);
                buckets.add(new Long(i));
            }
            for (int i = 10000; i < 100000; i = i + 2) {
                logger.debug("Adding " + i);
                buckets.add(new Long(i));
            }
            for (int i = 100000; i < 1000000; i = i + 10) {
                logger.debug("Adding " + i);
                buckets.add(new Long(i));
            }
        }
        if (useNegativeBuckets) {
            buckets = addNegativeBuckets(buckets);
            System.gc();
        }
        Collections.sort(buckets);
        return buckets;
    }

    /**
     * Does the work, then writes it out.
     *
     * @throws FileNotFoundException
     * @throws UnsupportedEncodingException
     */
    public List<Long> generateAndWrite() throws FileNotFoundException, UnsupportedEncodingException {
        List<Long> toReturn = generateBuckets();
        writeResults(toReturn);
        return toReturn;
    }

    /**
     * Writes the results as a single lined CSV.
     *
     * @param buckets buckets to write
     * @throws FileNotFoundException
     * @throws UnsupportedEncodingException
     */
    private void writeResults(List<Long> buckets) throws FileNotFoundException, UnsupportedEncodingException {
        StringBuilder contentToWrite = new StringBuilder(bucketSize * 8);//ballpark the size
        for (Long l : buckets) {
            contentToWrite.append(l).append(DE);
        }
        Utils.writeFile(contentToWrite.toString(), bucketSize + "bucketsFor" + dataType.toString() + "withPruneFactor" + pruneFactor + "MaxSize" + bucketMax + ".buckets");
    }

    /**
     * Creates our bucket list.
     *
     * @return
     */
    protected List<Long> createBuckets() {
        List<Long> toReturn = new ArrayList<>(bucketSize);
        for (int bucketPosition = 0; bucketPosition < bucketSize; bucketPosition++)//calculate buckets from 0 to the size of this bucket
        {
            Long bucketValue = initialBucket(bucketSize, bucketPosition);
            toReturn.add(bucketValue);
        }
        return toReturn;
    }

    /**
     * Get a initial bucket.
     */
    private Long initialBucket(int size, int position) {
        Long decValue = 0l;
        if (position != 0) {
            decValue = ((bucketMax / size) * position) - 1;
        }
        return decValue;
    }

    /**
     * Prunes our buckets from the end of the bucket list. Once a pruning start
     * position is established, it prunes chunks of the bucket list based on
     * bucketPruningInteration^2 * 32.
     *
     * @param bucketList Bucket List to prune.
     * @param pruneFactor decimal value (less than 1) that indicates where to
     * start our pruning. Does not impact pruning after a start point is
     * established. Lower values indicates an earlier start point (more
     * pruning), higher values indicate a later start point (less pruning).
     * @return A pruned bucket list.
     */
    private List<Long> pruneBucket(List<Long> bucketList, double pruneFactor) {
        int pruneStartIndex = 0;
        int startSize = bucketList.size();
        int i = 0;
        while (pruneStartIndex < bucketList.size()) {
            if (i == 0) {
                pruneStartIndex = (int) (startSize * pruneFactor) + pruneStartIndex;
            }
            i++;
            int numToPrune = i * i * 32;
            logger.debug("Pruning " + numToPrune + " buckets at position: " + pruneStartIndex + ", bucket list is currently: " + bucketList.size() + " long.");
//--optinal alternative method for pruning that uses more CPU and less memory; havne't checked on this in a while.
//            for (int bucketPosition = 0; bucketPosition <= numToPrune; bucketPosition++)
//            {
//                if (bucketPosition >= bucketList.size())
//                {
//                    pruneStartIndex = bucketList.size();///manually set our end case
//                    break;
//                }
//                bucketList.remove(pruneStartIndex + bucketPosition);
//            }
            List<Long> newBucketList = bucketList.subList(0, pruneStartIndex);
            int endPrune = pruneStartIndex + numToPrune;
            if (endPrune > bucketList.size()) {//we have run out of buckets to prune
                newBucketList.add(bucketList.get(bucketList.size() - 1));//add the last bucket
                pruneStartIndex = bucketList.size();///manually set our end case
            } else {
                newBucketList.addAll(bucketList.subList(pruneStartIndex + numToPrune, bucketList.size()));
            }
            bucketList = newBucketList;
            pruneStartIndex++;
        }
        int numRecordsPruned = startSize - bucketList.size();
        double percentPruned = ((double) numRecordsPruned / (double) startSize) * 100d;
        logger.debug("Pruned " + numRecordsPruned + " buckets out of " + startSize + " for a percent of " + percentPruned);
        //typeBuckets = pruneRecurse(typeBuckets, 0, typeBuckets.size(), 0);
        return bucketList;
    }

    /**
     * Adds a mirrored version of the bucket list, but with negative values.
     *
     * @param bucketList Bucket list to add negative values to.
     * @return New bucket list with negative values.
     */
    private List<Long> addNegativeBuckets(List<Long> bucketList) {
        logger.debug("Adding negative buckets...");
        List<Long> negBuckets = new ArrayList<>(bucketList.size());
        int counter = 0;
        for (Long l : bucketList) {
            if (l.longValue() != 0) {
                negBuckets.add(l * -1);
                if (counter % 1000 == 0) {
                    logger.debug("Added -" + l + " from index: " + counter);
                }
                counter++;
            }
        }
        logger.debug("Negative buckets generated.");
        negBuckets.addAll(bucketList);
        logger.debug("Negative buckets added.");
        //Collections.sort(bucketList);
        logger.debug("Done adding negative buckets...");
        return negBuckets;
    }

    /**
     * Parses our command line arguments into a FielDataType.
     *
     * @param args From the command line.
     * @return A parsed FieldDataType
     */
    private static FieldDataType parseArgs(String[] args) {
        if (args.length != 1) {
            throw new IllegalArgumentException("You must pass in a single FieldDataType");
        }
        return FieldDataType.valueOf(args[0].toUpperCase().trim());
    }
}
