package com.pearson.docussandra.domain.objects;

import com.pearson.docussandra.domain.Constants;
import com.strategicgains.syntaxe.annotation.RegexValidation;
import java.util.Objects;

/**
 * @author https://github.com/tfredrich
 * @since Jan 30, 2015
 */
public class TableReference
{

    @RegexValidation(name = "Database Name", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
    private String database;

    @RegexValidation(name = "Table Name", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
    private String name;

    /**
     * Default constructor for Jackson parsing only.
     */
    public TableReference()
    {
    }        

    public TableReference(String database, String table)
    {
        this.database = database;
        this.name = table;
    }

    public TableReference(Table table)
    {
        this(table.databaseName(), table.name());
    }

    public String database()
    {
        return database;
    }

    public String name()
    {
        return name;
    }

    public Table asObject()
    {
        Table t = new Table();
        t.database(database);
        t.name(name);
        return t;
    }

    @Override
    public int hashCode()
    {
        int hash = 5;
        hash = 59 * hash + Objects.hashCode(this.database);
        hash = 59 * hash + Objects.hashCode(this.name);
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
        final TableReference other = (TableReference) obj;
        if (!Objects.equals(this.database, other.database))
        {
            return false;
        }
        if (!Objects.equals(this.name, other.name))
        {
            return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        return "TableReference{" + "database=" + database + ", name=" + name + '}';
    }

}
