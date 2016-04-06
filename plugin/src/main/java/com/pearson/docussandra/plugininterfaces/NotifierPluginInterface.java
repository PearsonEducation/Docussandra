package com.pearson.docussandra.plugininterfaces;

import com.pearson.docussandra.domain.objects.Document;

/**
 * Interface that gets called anytime a document gets mutated. We <b>warned</b>:
 * This could happen quite frequently, and if you are not careful you could
 * substantially reduce the performance of Docussandra.
 * 
 * Although this is an abstract class, it should be treated like an interface.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public abstract class NotifierPluginInterface implements Plugin
{

    /**
     * Types of possible mutations.
     */
    public enum MutateType
    {
        CREATE,
        UPDATE,
        DELETE
    }

    /**
     * This method will get called any time a document is mutated. Be careful
     * about the amount of overhead this method produces, as it will be called
     * frequently.
     *
     * @param type Type of mutation that has occured.
     * @param document Updated document for this mutation. Will be null if the
     * mutation was a delete, be sure to check for null.
     */
    public abstract void doNotify(MutateType type, Document document);
}
