package com.pearson.docussandra.domain;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

/**
 *
 * @author https://github.com/JeffreyDeYoung
 */
public class DomainUtils
{

    public static final String EMPTY_STRING = "";

    public static String join(String delimiter, Collection<? extends Object> objects)
    {
        if (objects == null || objects.isEmpty())
        {
            return EMPTY_STRING;
        }
        Iterator<? extends Object> iterator = objects.iterator();
        StringBuilder builder = new StringBuilder();
        builder.append(iterator.next());
        while (iterator.hasNext())
        {
            builder.append(delimiter).append(iterator.next());
        }
        return builder.toString();
    }

    public static String join(String delimiter, Object... objects)
    {
        return join(delimiter, Arrays.asList(objects));
    }

}
