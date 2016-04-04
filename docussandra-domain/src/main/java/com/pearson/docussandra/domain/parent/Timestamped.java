package com.pearson.docussandra.domain.parent;

import java.util.Date;

/**
 * Super class that allows timestamping of objects.
 *
 * @author https://github.com/JeffreyDeYoung
 */
public abstract class Timestamped
{

    private Date createdAt;
    private Date updatedAt;

    public Timestamped()
    {
        //default for now
        createdAt = new Date();
        updatedAt = new Date();
    }

    public Date getCreatedAt()
    {
        return (createdAt == null ? null : new Date(createdAt.getTime()));
    }

    public Date getUpdatedAt()
    {
        return (updatedAt == null ? null : new Date(updatedAt.getTime()));
    }

    public void setCreatedAt(Date date)
    {
        this.createdAt = (date == null ? new Date() : new Date(date.getTime()));
    }

    public void setUpdatedAt(Date date)
    {
        this.updatedAt = (date == null ? new Date() : new Date(date.getTime()));
    }
}
