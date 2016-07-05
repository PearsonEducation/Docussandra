package com.pearson.docussandra.domain.objects;

/**
 * Enum for types of data that we can index on.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public enum FieldDataType
{

    /**
     * Use when indexing on normal strings.
     */
    TEXT,
    /**
     * Use when indexing on date times that could occur in any century. Date of
     * birth and historical/cosmological events are probably appropriate here.
     * Will support very wide ranges of dates well.
     */
    DATE_TIME,
    /**
     * Use when you need to index on double-style data.
     */
    DOUBLE,
    /**
     * Use when you need to index on integer-style data.
     */
    INTEGER,
    /**
     * Use when you need to index on long-style data.
     */
    LONG,
    /**
     * Use when you need to index on boolean data. Note, this type of index
     * should probably not be used as a primary index, it will not perform well.
     * It should be used as your last compound index if possible.
     */
    BOOLEAN,
    /**
     * Use when you need to index on UUIDs.
     */
    UUID,
    /**
     * Use when you need to index on binary data. Note, this will likely not be
     * your best option unless you have truly binary data. If you can use one of
     * the other data-types, it would be to your advantage in terms of
     * performance.
     */
    BINARY,
    /**
     * Use when you need to index on relatively recent/future dates with a high
     * precision. This index type is appropriate for timestamps with millisecond
     * precision. DATE_TIME will work for these types of dates as well, however,
     * this will perform better on more recent dates.
     */
    TIMEPOINT;

    /**
     * Get the total number of datatypes that this object supports.
     *
     * @return The total number of datatypes that this object supports.
     */
    public static int getNumberOfDataTypes()
    {
        return 9;
    }

    /**
     * Gets a numeric index indicating what type of FieldDataType this is for
     * use in an array or the like.
     *
     * @return An int index for this FieldDataType
     */
    public int getIndexForDataType()
    {
        return this.ordinal();
    }

    /**
     * Maps this type to a Cassandra datatype.
     *
     * @return The Cassandra datatype for this FieldDataType.
     */
    public String mapToCassandaraDataType() //TODO: consider switching this over to a switch statement.
    {
        if (this.equals(TEXT))
        {
            return "varchar";
        } else if (this.equals(DATE_TIME) || this.equals(TIMEPOINT))
        {
            return "timestamp";
        } else if (this.equals(DOUBLE))
        {
            return "double";
        } else if (this.equals(INTEGER))
        {
            return "int";
        } else if (this.equals(BOOLEAN))
        {
            return "boolean";
        } else if (this.equals(UUID))
        {
            return "uuid";
        } else if (this.equals(BINARY))
        {
            return "blob";
        } else if (this.equals(LONG))
        {
            return "bigint";
        } else
        {
            throw new IllegalArgumentException("Type not supported. " + this.toString());//this should never happen
        }
    }

}
