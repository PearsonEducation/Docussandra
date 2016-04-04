package com.pearson.docussandra.domain.objects;

/**
 * @author https://github.com/tfredrich
 * @since Jan 24, 2015
 */
public interface Callback<T>
{

    public void process(T value);
}
