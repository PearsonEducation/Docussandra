package com.pearson.docussandra.domain.objects;

import org.restexpress.plugin.hyperexpress.Linkable;

/**
 * Document class that implements Linkable for HAL.
 * @author https://github.com/JeffreyDeYoung
 */
public class LinkableDocument extends Document implements Linkable
{
    public LinkableDocument(Document d){
        super.setUuid(d.getUuid());
        super.table(d.table());
        super.object(d.object());
    }
    
}
