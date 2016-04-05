package com.pearson.docussandra.domain.objects;

/**
 * Enum for types of data that we can index on.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public enum FieldDataType
{

    TEXT,
    DATE_TIME,
    DOUBLE,
    INTEGER,
    LONG,
    BOOLEAN,
    UUID,
    BINARY,
    TIMEPOINT;
    
    /**
     * Get the total number of datatypes that this object supports.
     * @return The total number of datatypes that this object supports.
     */
    public static int getNumberOfDataTypes(){
        return 9;
    }

    /**
     * Gets a numeric index indicating what type of FieldDataType this is for use in an array or the like.
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
