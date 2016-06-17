package com.pearson.docussandra.domain.objects;

import com.pearson.docussandra.domain.Constants;
import com.pearson.docussandra.domain.parent.Timestamped;
import com.pearson.docussandra.domain.parent.Identifiable;
import com.strategicgains.syntaxe.annotation.ChildValidation;
import com.strategicgains.syntaxe.annotation.RegexValidation;
import com.strategicgains.syntaxe.annotation.Required;
import com.strategicgains.syntaxe.annotation.StringValidation;
import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;
import java.util.Objects;
import org.restexpress.plugin.hyperexpress.Linkable;

/**
 * Object that represents a table in Docussandra.
 * @author Jeffrey DeYoung
 */
@ApiModel(value = "Table",
        description = "Model that defines a table including its name and description.")
public class Table
        extends Timestamped implements Linkable, Identifiable
{

    @ApiModelProperty(value = "The database's name",
            notes = "This is the database name associated with this table.",
            dataType = "String",
            required = true)
    @Required("Database")
    @ChildValidation
    private DatabaseReference database;

    @ApiModelProperty(value = "The table's name",
            notes = "This is the tables name",
            dataType = "String",
            required = true)
    @RegexValidation(name = "Table Name", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
    @StringValidation(maxLength = 32)
    private String name;
    private String description;

    //TODO: add consistency & distro metadata here.  https://github.com/PearsonEducation/Docussandra/issues/8
    // How long should this data live?
    private long ttl = 0;

    // After delete or update, how long should the old versions live?
    private long deleteTtl = 0;

    /**
     * Default constructor, needed for serialization.
     */
    public Table()
    {
        super();
    }
    
    /**
     * Constructor.
     * @param databaseName Name of the database that this table is in.
     * @param tableName Name of this table.
     */
    public Table(String databaseName, String tableName){
        this.database = new DatabaseReference(name);
        this.name = tableName;
    }
    
    /**
     * Constructor.
     * @param database Database that this table is in.
     * @param tableName Name of this table.
     */
    public Table(Database database, String tableName){
        this.database = new DatabaseReference(name);
        this.name = tableName;
    }

    /**
     * Returns true if this Table object has a database object associated with
     * it.
     *
     * @return
     */
    public boolean hasDatabase()
    {
        return (database != null);
    }

    /**
     * Gets the database object associated with this Table object.
     *
     * @return
     */
    public Database getDatabase()
    {
        return database.asObject();
    }

    /**
     * Sets the database for this table.
     *
     * @param database
     */
    public void setDatabase(Database database)
    {
        this.database = new DatabaseReference(database);
    }

    /**
     * Sets the database for this table.
     *
     * @param name
     */
    public void setDatabase(String name)
    {
        this.database = new DatabaseReference(name);
    }

    /**
     * Gets the database name associated with this table.
     *
     * @return
     */
    public String getDatabaseName()
    {
        return (hasDatabase() ? database.getName() : null);
    }

    /**
     * Gets the name for this table.
     *
     * @return
     */
    public String getName()
    {
        return name;
    }

    /**
     * Sets the name of this table.
     *
     * @param name
     */
    public void setName(String name)
    {
        this.name = name;
    }

    /**
     * Returns true if there is a description for this table.
     *
     * @return
     */
    public boolean hasDescription()
    {
        return (description != null);
    }

    /**
     * Gets the description for this table.
     *
     * @return
     */
    public String getDescription()
    {
        return description;
    }

    /**
     * Sets the description for this table.
     *
     * @param description
     */
    public void setDescription(String description)
    {
        this.description = description;
    }

    /**
     * Gets the identifier object for this table.
     *
     * @return
     */
    @Override
    public Identifier getId()
    {
        return (hasDatabase() & name != null ? new Identifier(database.getName(), name) : null);
    }

    /**
     * Gets the name of this table as it is referred to by Cassandra itself.
     *
     * @return
     */
    public String toDbTable()
    {
        return database.getName() + "_" + name;
    }

    /**
     * Returns a string representation of this object.
     *
     * @return
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder(name);

        if (hasDescription())
        {
            sb.append(" (");
            sb.append(description);
            sb.append(")");
        }
        return sb.toString();
    }

    /**
     * Hash code.
     *
     * @return
     */
    @Override
    public int hashCode()
    {
        int hash = 5;
        hash = 59 * hash + Objects.hashCode(this.database);
        hash = 59 * hash + Objects.hashCode(this.name);
        hash = 59 * hash + Objects.hashCode(this.description);
        hash = 59 * hash + (int) (this.getTtl() ^ (this.getTtl() >>> 32));
        hash = 59 * hash + (int) (this.getDeleteTtl() ^ (this.getDeleteTtl() >>> 32));
        return hash;
    }

    /**
     * Determines if this object is equal to another object.
     *
     * @param obj
     * @return
     */
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
        final Table other = (Table) obj;
        if (!Objects.equals(this.database, other.database))
        {
            return false;
        }
        if (!Objects.equals(this.name, other.name))
        {
            return false;
        }
        if (!Objects.equals(this.description, other.description))
        {
            return false;
        }
        if (this.getTtl() != other.getTtl())
        {
            return false;
        }
        if (this.getDeleteTtl() != other.getDeleteTtl())
        {
            return false;
        }
        return true;
    }

    /**
     * @return the ttl
     */
    public long getTtl()
    {
        return ttl;
    }

    /**
     * @param ttl the ttl to set
     */
    public void setTtl(long ttl)
    {
        this.ttl = ttl;
    }

    /**
     * @return the deleteTtl
     */
    public long getDeleteTtl()
    {
        return deleteTtl;
    }

    /**
     * @param deleteTtl the deleteTtl to set
     */
    public void setDeleteTtl(long deleteTtl)
    {
        this.deleteTtl = deleteTtl;
    }

}
