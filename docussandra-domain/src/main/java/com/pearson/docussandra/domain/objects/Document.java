package com.pearson.docussandra.domain.objects;

import com.mongodb.util.JSON;
import com.pearson.docussandra.domain.parent.Identifiable;
import com.pearson.docussandra.domain.parent.Timestamped;

import java.util.UUID;

import com.strategicgains.syntaxe.annotation.ChildValidation;
import com.strategicgains.syntaxe.annotation.Required;
import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

import java.util.Objects;

import org.bson.BSONObject;

/**
 * This is the domain object that represents a Document in our database. Since
 * this is a document based database, this is the smallest unit of data storage.
 * In other words, this object represents a single record in Docussandra.
 *
 * @author Jeffrey DeYoung
 */
@ApiModel(value = "Document",
        description = "Model that defines a document, including its table and actual data.")
public class Document extends Timestamped implements Identifiable
{
    //TODO: allow something other than UUID as object id.  https://github.com/PearsonEducation/Docussandra/issues/7
    //TODO: add any necessary metadata regarding a document. https://github.com/PearsonEducation/Docussandra/issues/8
    //documents are versioned per transaction via updateAt timestamp.

    /**
     * Unique identifier for this object.
     */
    private UUID id;

    // need a separate version (as opposed to updatedAt)?
//	private long version;
    /**
     * Table that this document resides (or should reside) in.
     */
    @ApiModelProperty(value = "The table's name",
            notes = "This is the table name in which this document is stored.",
            dataType = "String",
            required = true)
    @Required("Table")
    @ChildValidation
    private TableReference table;

    /**
     * The actual JSON document we are storing. Stored as BSON. This is the
     * record itself, with no additional Docussandra metadata.
     */
    private BSONObject object;

    /**
     * Default constructor, needed for serialization.
     */
    public Document()
    {
        super();
    }

    /**
     * Gets the full identifier for this object.
     *
     * @return
     */
    @Override
    public Identifier getId()
    {
        return new Identifier(getDatabaseName(), getTableName(), id, getUpdatedAt());
    }

    /**
     * Gets the unique identifier for this object inside of the given db and
     * table.
     *
     * @return
     */
    public UUID getUuid()
    {
        return id;
    }

    /**
     * Sets the UUID for this object.
     *
     * @param id
     */
    public void setUuid(UUID id)
    {
        this.id = id;
    }

    /**
     * Returns true if there is a table associated with this object. This should
     * nearly always be the case, unless the object was constructed improperly
     * or incompletely. Private for that reason; used internally for null
     * checks.
     *
     * @return
     */
    private boolean hasTable()
    {
        return (table != null);
    }

    /**
     * Gets the table in which this document is stored.
     *
     * @return The table that contains this document.
     */
    public Table getTable()
    {
        return (hasTable() ? table.asObject() : null);
    }

    /**
     * Sets the table that this document is to be stored in.
     *
     * @param database Name of the database that we are storing this document
     * in.
     * @param table Name of the table (inside of the database) that we are
     * storing this document in.
     */
    public void setTable(String database, String table)
    {
        this.table = new TableReference(database, table);
    }

    /**
     * Sets the table that this document is to be stored in.
     *
     * @param table
     */
    public void setTable(Table table)
    {
        this.table = (table != null ? new TableReference(table) : null);
    }

    /**
     * Gets the name of the table that this document is stored in.
     *
     * @return
     */
    public String getTableName()
    {
        return (hasTable() ? table.name() : null);
    }

    /**
     * Gets the name of the database that this document is stored in.
     *
     * @return
     */
    public String getDatabaseName()
    {
        return (hasTable() ? table.database() : null);
    }

    /**
     * Gets the actual document (record) that is stored in Docussandra.
     *
     * @return The stored document as a BSONObject.
     */
    public BSONObject getObject()
    {
        return object;
    }

    /**
     * Sets the actual document (record) that is stored in Docussandra.
     *
     * @param json
     */
    public void setObject(BSONObject json)
    {
        this.object = json;
    }

    /**
     * Gets the object stored in this document object, as a String. Note, this
     * requires deserializing the BSON, so if you can work with the BSON
     * directly, use the getObject() call instead, it will perform better.
     *
     * @return The document as a String.
     */
    public String getObjectAsString()
    {
        return JSON.serialize(object);
    }

    /**
     * Sets the actual document (record) that is stored in Docussandra, as a
     * String. Note this requires serializing the object to BSON, so if you
     * already have the object as BSON, you should call setObject() instead, as it will perform better.
     *
     * @param json
     */
    public void setObjectAsString(String json)
    {
        this.object = (BSONObject) JSON.parse(json);
    }

    /**
     * Returns a string representation of the object.
     * @return 
     */
    @Override
    public String toString()
    {
        return "Document{" + "id=" + id + ", table=" + table + ", object=" + object + '}';
    }

    /**
     * Hash code for this object.
     * @return 
     */
    @Override
    public int hashCode()
    {
        int hash = 7;
        hash = 29 * hash + Objects.hashCode(this.id);
        hash = 29 * hash + Objects.hashCode(this.table);
        hash = 29 * hash + Objects.hashCode(this.object);
        return hash;
    }

    /**
     * Determines if this object is equal to another object.
     * @param obj
     * @return 
     */
    @Override
    public boolean equals(Object obj)
    {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Document other = (Document) obj;
        if (!Objects.equals(this.id, other.id)) {
            return false;
        }
        if (!Objects.equals(this.table, other.table)) {
            return false;
        }
        if (!Objects.equals(this.object, other.object)) {
            return false;
        }
        return true;
    }

}
