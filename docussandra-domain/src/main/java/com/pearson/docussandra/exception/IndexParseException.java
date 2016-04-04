package com.pearson.docussandra.exception;

import com.pearson.docussandra.domain.objects.IndexField;

/**
 * Exception that indicates that a field that should be indexable is not in the
 * specified format. Contains more meta-data than using an
 * IndexParseFieldException alone.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class IndexParseException extends IndexParseFieldException
{

    /**
     * Field that could not be indexed in the document.
     */
    private IndexField field;
//    /**
//     * Document that could not be indexed.
//     */
//    private Document entity;

    /**
     * Constructor.
     *
     * @param field Field that could not be indexed in the document.
     * @param parent Parent class to use.
     */
    public IndexParseException(IndexField field, IndexParseFieldException parent)
    {
        super("The field: " + field.toString() + " could not be parsed from the document or query, it is not a valid value for the specified datatype.", parent.getFieldValue(), parent.getCause());
        this.field = field;
    }

    /**
     * Field that could not be indexed in the document.
     *
     * @return the field
     */
    public IndexField getField()
    {
        return field;
    }

    @Override
    public String toString()
    {
        return "IndexParseException{" + "field=" + field + ", fieldValue=" + super.getFieldValue() + '}';
    }

}
