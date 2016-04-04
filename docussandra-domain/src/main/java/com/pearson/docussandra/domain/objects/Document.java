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

@ApiModel(value = "Document",
        description = "Model that defines a document, including its table and actual data.")
public class Document extends Timestamped implements Identifiable
{
    //TODO: allow something other than UUID as object id.  https://github.com/PearsonEducation/Docussandra/issues/7
    //TODO: add any necessary metadata regarding a document. https://github.com/PearsonEducation/Docussandra/issues/8
    //documents are versioned per transaction via updateAt timestamp.

    private UUID id;

    // need a separate version (as opposed to updatedAt)?
//	private long version;
    @ApiModelProperty(value = "The table's name",
            notes = "This is the table name in which this document is stored.",
            dataType = "String",
            required = true)
    @Required("Table")
    @ChildValidation
    private TableReference table;

    // The JSON document.
    private BSONObject object;

    public Document()
    {
        super();
    }

    @Override
    public Identifier getId()
    {
        return new Identifier(databaseName(), tableName(), id, getUpdatedAt());
    }

    public UUID getUuid()
    {
        return id;
    }

    public void setUuid(UUID id)
    {
        this.id = id;
    }

    public boolean hasTable()
    {
        return (table != null);
    }

    public Table table()
    {
        return (hasTable() ? table.asObject() : null);
    }

    public void table(String database, String table)
    {
        this.table = new TableReference(database, table);
    }

    public void table(Table table)
    {
        this.table = (table != null ? new TableReference(table) : null);
    }

    public String tableName()
    {
        return (hasTable() ? table.name() : null);
    }

    public String databaseName()
    {
        return (hasTable() ? table.database() : null);
    }

    public BSONObject object()
    {
        return object;
    }

    public void object(BSONObject json)
    {
        this.object = json;
    }

    public String objectAsString()
    {
        return JSON.serialize(object);
    }

    public void objectAsString(String json)
    {
        this.object = (BSONObject) JSON.parse(json);
    }

    @Override
    public String toString()
    {
        return "Document{" + "id=" + id + ", table=" + table + ", object=" + object + '}';
    }

    @Override
    public int hashCode()
    {
        int hash = 7;
        hash = 29 * hash + Objects.hashCode(this.id);
        hash = 29 * hash + Objects.hashCode(this.table);
        hash = 29 * hash + Objects.hashCode(this.object);
        return hash;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        if (getClass() != obj.getClass())
        {
            return false;
        }
        final Document other = (Document) obj;
        if (!Objects.equals(this.id, other.id))
        {
            return false;
        }
        if (!Objects.equals(this.table, other.table))
        {
            return false;
        }
        if (!Objects.equals(this.object, other.object))
        {
            return false;
        }
        return true;
    }

}
