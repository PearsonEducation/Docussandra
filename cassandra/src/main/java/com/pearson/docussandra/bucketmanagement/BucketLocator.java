
package com.pearson.docussandra.bucketmanagement;

import com.pearson.docussandra.domain.objects.FieldDataType;
import com.pearson.docussandra.exception.IndexParseFieldException;

/**
 * Interface for locating buckets for data storage and retrieval.
 *
 */
public interface BucketLocator
{

    /**
     * Return the bucket to use for indexing this entity
     *
     * @param bucketingObject object that we are bucketing on
     * @param dataType the Type of data that we are bucketing on.
     *
     * @return A bucket to use.
     * @throws IndexParseFieldException if the bucketing object cannot be converted to the specified dataType.
     */
    public Long getBucket(Object bucketingObject, FieldDataType dataType) throws IndexParseFieldException;

}
