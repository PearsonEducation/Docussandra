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

    public Table()
    {
        super();
    }

    public boolean hasDatabase()
    {
        return (database != null);
    }

    public Database database()
    {
        return database.asObject();
    }

    public void database(Database database)
    {
        this.database = new DatabaseReference(database);
    }

    public void database(String name)
    {
        this.database = new DatabaseReference(name);
    }

    public String databaseName()
    {
        return (hasDatabase() ? database.name() : null);
    }

    public boolean hasName()
    {
        return (name != null);
    }

    public String name()
    {
        return name;
    }

    public void name(String name)
    {
        this.name = name;
    }

    public boolean hasDescription()
    {
        return (description != null);
    }

    public String description()
    {
        return description;
    }

    public void description(String description)
    {
        this.description = description;
    }

    @Override
    public Identifier getId()
    {
        return (hasDatabase() & hasName() ? new Identifier(database.name(), name) : null);
    }

    public String toDbTable()
    {
        return database.name() + "_" + name;
    }

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
