package com.pearson.docussandra.domain.objects;

import com.pearson.docussandra.domain.Constants;
import com.strategicgains.syntaxe.annotation.RegexValidation;
import java.util.Objects;

/**
 * @author https://github.com/tfredrich
 * @since Jan 25, 2015
 */
public class DatabaseReference
{

    @RegexValidation(name = "Database Name", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
    private String name;

    public DatabaseReference()
    {
        super();
    }

    public DatabaseReference(String name)
    {
        this.name = name;
    }

    public DatabaseReference(Database database)
    {
        this(database.name());
    }

    public String name()
    {
        return name;
    }

    public Database asObject()
    {
        return new Database(name);
    }

    @Override
    public int hashCode()
    {
        int hash = 7;
        hash = 83 * hash + Objects.hashCode(this.name);
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
        final DatabaseReference other = (DatabaseReference) obj;
        if (!Objects.equals(this.name, other.name))
        {
            return false;
        }
        return true;
    }

}
