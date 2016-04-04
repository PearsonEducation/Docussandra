package com.pearson.docussandra.bucketmanagement;

import com.pearson.docussandra.ParseUtils;
import com.pearson.docussandra.Utils;
import com.pearson.docussandra.domain.objects.FieldDataType;
import com.pearson.docussandra.exception.IndexParseFieldException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Locates a bucket for an item based on the first few most significant bits of
 * the object.
 */
public class PrimaryIndexBucketLocatorImpl implements BucketLocator
{

    private static Logger logger = LoggerFactory.getLogger(PrimaryIndexBucketLocatorImpl.class);
    private static PrimaryIndexBucketLocatorImpl instance;

    /**
     * Array containing all of our buckets. Outer array is an array of all our
     * buckets by FieldDataType as indexed by
     * FieldDataType.getIndexForDataType(); The inner lists are the actual
     * buckets for each datatype.
     */
    private static List<Long>[] BUCKETS;

    /**
     * Array of our bucket sizes as indexed by
     * FieldDataType.getIndexForDataType();
     */
    //public static int[] BUCKET_SIZES = new int[FieldDataType.getNumberOfDataTypes()];
    /**
     * Method for getting an instance of SimpleIndexBucketLocatorImpl. Will
     * return an existing instance if it exists, will create a new one
     * otherwise. (In other words, it's a singleton.)
     *
     * @return an SimpleIndexBucketLocatorImpl instance.
     */
    public static PrimaryIndexBucketLocatorImpl getInstance()
    {
        if (instance == null)
        {
            instance = new PrimaryIndexBucketLocatorImpl();
        }
        return instance;
    }

    /**
     * Private constructor. For a singleton.
     */
    private PrimaryIndexBucketLocatorImpl()
    {
        BUCKETS = new List[FieldDataType.getNumberOfDataTypes()];
        try
        {
            //read buckets from the FS
            BUCKETS[FieldDataType.TEXT.getIndexForDataType()] = readBucketFromClassPath("/buckets/text.buckets");
            BUCKETS[FieldDataType.DATE_TIME.getIndexForDataType()] = readBucketFromClassPath("/buckets/date_time.buckets");
            BUCKETS[FieldDataType.TIMEPOINT.getIndexForDataType()] = readBucketFromClassPath("/buckets/timepoint.buckets");
            BUCKETS[FieldDataType.DOUBLE.getIndexForDataType()] = readBucketFromClassPath("/buckets/double.buckets");
            BUCKETS[FieldDataType.INTEGER.getIndexForDataType()] = readBucketFromClassPath("/buckets/integer.buckets");
            BUCKETS[FieldDataType.LONG.getIndexForDataType()] = readBucketFromClassPath("/buckets/long.buckets");
            BUCKETS[FieldDataType.BOOLEAN.getIndexForDataType()] = readBucketFromClassPath("/buckets/boolean.buckets");
            BUCKETS[FieldDataType.UUID.getIndexForDataType()] = readBucketFromClassPath("/buckets/uuid.buckets");
            BUCKETS[FieldDataType.BINARY.getIndexForDataType()] = readBucketFromClassPath("/buckets/binary.buckets");
        } catch (IOException e)
        {
            logger.error("Could not read buckets from file system! Docussandra cannot start!", e);
            throw new RuntimeException(e);
        }

    }

    private List<Long> readBucketFromClassPath(String path) throws IOException
    {
        return Utils.traverse1RowFile(path);
    }

    /**
     * Protected constructor. Do not call for anything other than testing of
     * changing bucket sizes. Probably not thread safe!
     *
     */
    @Deprecated
    public PrimaryIndexBucketLocatorImpl(List<Long> textBuckets, List<Long> dateTimeBuckets,
            List<Long> doubleBuckets, List<Long> integerBuckets, List<Long> booleanBuckets,
            List<Long> uuidBuckets, List<Long> binaryBuckets, List<Long> longBuckets, List<Long> timepointBuckets)
    {
        BUCKETS = new List[FieldDataType.getNumberOfDataTypes()];

        BUCKETS[FieldDataType.TEXT.getIndexForDataType()] = textBuckets;
        BUCKETS[FieldDataType.DATE_TIME.getIndexForDataType()] = dateTimeBuckets;
        BUCKETS[FieldDataType.TIMEPOINT.getIndexForDataType()] = timepointBuckets;
        BUCKETS[FieldDataType.DOUBLE.getIndexForDataType()] = doubleBuckets;
        BUCKETS[FieldDataType.INTEGER.getIndexForDataType()] = integerBuckets;
        BUCKETS[FieldDataType.LONG.getIndexForDataType()] = longBuckets;
        BUCKETS[FieldDataType.BOOLEAN.getIndexForDataType()] = booleanBuckets;
        BUCKETS[FieldDataType.UUID.getIndexForDataType()] = uuidBuckets;
        BUCKETS[FieldDataType.BINARY.getIndexForDataType()] = binaryBuckets;

    }

    /**
     * Get the next token in the ring for this big int.
     */
    private Long getClosestBucket(Long bucketingToken, FieldDataType dataType)
    {
        int index = Collections.binarySearch(BUCKETS[dataType.getIndexForDataType()], bucketingToken);

        //make sure the bucket index isn't negative (it will be negative the closest index if it doesn't hit a bucket exactly)
        if (index < 0)
        {
            index = (index + 1) * -1;//if it is, jump the index + 1 and make it positive
        }

        // mod if we need to wrap
        index = index % getBUCKETS()[dataType.getIndexForDataType()].size();

        Long toReturn = BUCKETS[dataType.getIndexForDataType()].get(index);//grab the actual bucket
        return toReturn;
    }

    /**
     * Return the bucket to use for indexing this entity.
     *
     * @param bucketingObject The entity to be indexed/bucketed on.
     * @param dataType the Type of data that we are bucketing on.
     *
     * @return A bucket to use.
     * @throws IndexParseFieldException if the bucketing object cannot be
     * converted to the specified dataType.
     */
    @Override
    public Long getBucket(Object bucketingObject, FieldDataType dataType) throws IndexParseFieldException
    {
        if (bucketingObject == null)
        {
            throw new IllegalArgumentException("Bucketing Token must be not null");
        }

        if (dataType == null)
        {
            throw new IllegalArgumentException("Data type must be not null");
        }
        //if what is being passed in should be of a certian datatype, but has not yet been converted from the string in the json
        Object normalizedObject = bucketingObject;
        if (bucketingObject instanceof String && !(dataType.equals(FieldDataType.TEXT) || dataType.equals(FieldDataType.BINARY)))
        {//if it's a string, but shouldn't be
            normalizedObject = convertStringToOject((String) normalizedObject, dataType);
        }

        return getClosestBucket(convertObjectToBucketingToken(normalizedObject, dataType), dataType);
    }

    /**
     * Converts a string to a proper object of the specified type. Don't use for
     * types that are already Strings.
     *
     * @param s String to convert to an object.
     * @param dataType Data type for the String to be converted to.
     * @return An object that represents the passed in string.
     * @throws IndexParseFieldException If the object cannot be converted to the
     * specified type.
     */
    private Object convertStringToOject(String s, FieldDataType dataType) throws IndexParseFieldException
    {
        if (dataType.equals(FieldDataType.BOOLEAN))
        {
            return ParseUtils.parseStringAsBoolean(s);
        } else if (dataType.equals(FieldDataType.DATE_TIME) || dataType.equals(FieldDataType.TIMEPOINT))
        {
            return ParseUtils.parseStringAsDate(s);
        } else if (dataType.equals(FieldDataType.LONG))
        {
            return ParseUtils.parseStringAsLong(s);
        } else if (dataType.equals(FieldDataType.DOUBLE))
        {
            return ParseUtils.parseStringAsDouble(s);
        } else if (dataType.equals(FieldDataType.INTEGER))
        {
            return ParseUtils.parseStringAsInt(s);
        } else if (dataType.equals(FieldDataType.UUID))
        {
            return ParseUtils.parseStringAsUUID(s);
        } else
        {
            throw new IndexParseFieldException(s);
        }
    }

    /**
     * Wrapper for our type to Long conversion methods.
     *
     * @param toBeConverted Object that needs to be converted to a bucketing
     * token.
     * @param dataType DataType of that object.
     * @return A Long token that can be used by the bucketing algorithm.
     */
    public static Long convertObjectToBucketingToken(Object toBeConverted, FieldDataType dataType)
    {
        //TODO: consider removing redundent checks
        if (dataType == null)
        {
            throw new IllegalArgumentException("Data type must not be null.");
        }
        if (toBeConverted == null)
        {
            //but just in case
            return null;
        }
        if (dataType.equals(FieldDataType.BINARY))
        {
            return convertBinaryBase64ToLong((String) toBeConverted);
        } else if (dataType.equals(FieldDataType.BOOLEAN))
        {
            Boolean temp = (Boolean) toBeConverted;
            if (temp)
            {
                return 1L;
            } else
            {
                return 0L;
            }
        } else if (dataType.equals(FieldDataType.DATE_TIME))
        {
            return convertDateToBucketingLong((Date) toBeConverted);
        } else if (dataType.equals(FieldDataType.TIMEPOINT))
        {
            return convertTimepointToBucketingLong((Date) toBeConverted);
        } else if (dataType.equals(FieldDataType.LONG))
        {
            return (Long) toBeConverted;
        } else if (dataType.equals(FieldDataType.DOUBLE))
        {
            return convertDoubleToLong((Double) toBeConverted);
        } else if (dataType.equals(FieldDataType.INTEGER))
        {
            return convertIntegerToBucketingLong((Integer) toBeConverted);
        } else if (dataType.equals(FieldDataType.TEXT))
        {
            return convertStringToBuckectingLong((String) toBeConverted);
        } else if (dataType.equals(FieldDataType.UUID))
        {
            return convertUuidToLong((UUID) toBeConverted);
        } else
        {
            throw new UnsupportedOperationException(dataType + "  is not a valid data type.");
        }
    }

    /**
     * This method converts the Date value to its corresponding long value, this
     * method assumes that the date format is the same as the format being
     * returned by the ParseUtils.parseStringAsDate
     *
     * @param dateToBeConverted
     * @return Long
     */
    protected static Long convertDateToBucketingLong(Date dateToBeConverted)
    {
        if (dateToBeConverted == null)
        {
            return null;
        }
        return dateToBeConverted.getTime();
    }

    /**
     * This method converts the Timepoint value to its corresponding long value,
     * this method assumes that the date format is the same as the format being
     * returned by the ParseUtils.parseStringAsDate. Note: Any value prior to
     * Y2K (00:00:00 UTC on 1 January 2000) will be bucketed as 0 and any value
     * after 2030 will be bucketed as bucket max. This is done to reduce the
     * number of buckets that are required for this type (by eliminating ranges
     * that are likely to be entirely unused).
     *
     * @param timepointToBeConverted Timeport to convert to a bucketing long.
     * @return Long
     */
    protected static Long convertTimepointToBucketingLong(Date timepointToBeConverted)
    {
        if (timepointToBeConverted == null)
        {
            return null;
        }
        Long toReturn = timepointToBeConverted.getTime() - PrimaryIndexBucketGeneratorImpl.TIMEPOINT_MIN; //offset from y2k (year 2000 gets scaled to look like 1970)
        if (toReturn < 0l)
        {
            return 0l;
        } else if (toReturn > PrimaryIndexBucketGeneratorImpl.TIMEPOINT_MAX)//TIMEPOINT_MAX is already scaled from y2k
        {
            return PrimaryIndexBucketGeneratorImpl.TIMEPOINT_MAX;
        } else
        {
            return toReturn;
        }
    }

//    private static final double DOUBLE_SCALE_FACTOR = Double.MAX_VALUE / new Double(Long.MAX_VALUE);
    /**
     * Converts a Double to a Long for bucketing purposes. Order of input and
     * output values is preserved as long.
     *
     * @param inDoubleValue value to convert to Long.
     * @return a Long value for use in bucket placement.
     */
    protected static Long convertDoubleToLong(Double inDoubleValue)
    {
        if (inDoubleValue == null)
        {
            return null;
        }
//        //need to scale by the same range as longs
//        inDoubleValue = inDoubleValue / DOUBLE_SCALE_FACTOR;
        //then round
        if(inDoubleValue > Long.MAX_VALUE){
            return Long.MAX_VALUE;
        }
        if(inDoubleValue < Long.MIN_VALUE){
            return Long.MIN_VALUE;
        }
        Long outLongValue = Math.round(inDoubleValue);
        return outLongValue;
    }

    /**
     * Converts a binaryBase64 to a Long for bucketing purposes. Order of input
     * and output values is preserved as long.
     *
     * @param binaryBase64 to convert to Long
     * @return a Long value for use in bucket replacement
     */
    protected static Long convertBinaryBase64ToLong(String binaryBase64) throws IllegalArgumentException
    {
        if (binaryBase64 == null)
        {
            return null;
        }
        if (binaryBase64.length() < 4)
        {
            throw new IllegalArgumentException("Input is not a valid BinaryBase64 value, minimum length of BinaryBase64 is 4. Enter a valid value");
        }
        String first8Chars = null;
        if (binaryBase64.length() >= 8)
        {
            first8Chars = binaryBase64.substring(0, 8);
        } else
        {
            first8Chars = binaryBase64;
            //pad with null chars so we get up to 8 bytes
            int toPad = 8 - first8Chars.length();
            for (int i = 0; i < toPad; i++)
            {
                first8Chars += '\u0000';//not the most efficent, but shouldn't happen often or for very many chars
            }
        }
        byte[] bytes = first8Chars.getBytes();

//        byte[] decode = DatatypeConverter.parseBase64Binary(first8Chars);
//        StringBuilder sb = new StringBuilder();
//        for (int i = 0; i < decode.length; i++)
//        {
//            String binChar = Integer.toBinaryString(decode[i]);
//            sb.append(String.format("%s", binChar).replace(" ", "0"));
//        }
//        String binToString = sb.toString();
        //String first8BytesBinary = binToString.substring(0, 32);
        //long outLongFromBin = new BigInteger(bytes, 2).longValue();
        //return outLongFromBin;
        return ByteBuffer.wrap(bytes).getLong();
    }

    /**
     * Converts an Integer to a Long for bucketing purposes. Order of integers
     * will be preserved as longs, but not the actual numeric value. This method
     * should <b>only</b> be used for determining bucket placement.
     *
     * @param integer to convert to a bucketing Long.
     * @return a Long for use in bucket placement.
     */
    protected static Long convertIntegerToBucketingLong(Integer integer)
    {
        if (integer == null)
        {
            return null;
        }
        if (integer == Integer.MIN_VALUE)
        {
            integer++;
        }
        long toReturn = integer.longValue();// * 4294967298L;
        return toReturn;
    }

    /**
     * Converts a string into long values takes the first 8 bytes and uses those
     * to convert them into long, if the string does not contain 8 bytes we pad
     * them with zeros
     *
     * @param stringToBeConverted
     * @return a Long value for the string
     *
     */
    protected static Long convertStringToBuckectingLong(String stringToBeConverted)
    {
        if (stringToBeConverted == null || stringToBeConverted.equals(""))
        {
            return null;
        }
        byte[] byteString = stringToBeConverted.getBytes();
        if (byteString.length < 8)
        {
            //need to pad if the string contains less than 8 chars
            byte[] newByteString = new byte[8];
            for (int i = 0; i < newByteString.length; i++)
            {
                if (i < byteString.length)
                {
                    newByteString[i] = byteString[i];
                } else
                {
                    newByteString[i] = 0;
                }
            }
            byteString = newByteString;
        }
        ByteBuffer bb = ByteBuffer.wrap(byteString);
        long byteMethod = bb.getLong();
        byteMethod = Math.abs(byteMethod);
        return byteMethod;
    }

    /**
     * Converts a UUID to a Long for bucketing purposes. Order of input and
     * output values is preserved as long.
     *
     * @param uuid to convert to Long
     * @return a Long value for use in bucket replacement
     */
    protected static Long convertUuidToLong(UUID uuid)
    {
        if (uuid == null)
        {
            return null;
        }
        Long convertedUuid;
        if (uuid.version() == 1)
        {
            convertedUuid = uuid.timestamp();
        } else
        {
            convertedUuid = uuid.getMostSignificantBits();
        }
        return convertedUuid;
    }

    /**
     * Array containing all of our buckets. Outer array is an array of all our
     * buckets by FieldDataType as indexed by
     * FieldDataType.getIndexForDataType(); The inner lists are the actual
     * buckets for each datatype.
     *
     * @return the BUCKETS
     */
    public static List<Long>[] getBUCKETS()
    {
        return BUCKETS;
    }

}
