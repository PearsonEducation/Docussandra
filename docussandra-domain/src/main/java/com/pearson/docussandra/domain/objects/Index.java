package com.pearson.docussandra.domain.objects;

import com.pearson.docussandra.domain.Constants;
import com.pearson.docussandra.domain.parent.Timestamped;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import com.pearson.docussandra.domain.parent.Identifiable;
import com.strategicgains.syntaxe.Validatable;
import com.strategicgains.syntaxe.ValidationException;
import com.strategicgains.syntaxe.annotation.ChildValidation;
import com.strategicgains.syntaxe.annotation.RegexValidation;
import com.strategicgains.syntaxe.annotation.Required;
import com.strategicgains.syntaxe.annotation.StringValidation;
import java.io.Serializable;
import java.util.Objects;
import org.restexpress.plugin.hyperexpress.Linkable;

public class Index
        extends Timestamped
        implements Validatable, Serializable, Linkable, Identifiable
{

    @Required("Table")
    @ChildValidation
    private TableReference table;

    @RegexValidation(name = "Index Name", nullable = false, pattern = Constants.NAME_PATTERN, message = Constants.NAME_MESSAGE)
    @StringValidation(maxLength = 32)
    private String name;

    private boolean isUnique = false;

    /**
     * The list of getFields, in order, that are being indexed. Prefixing a
     * field with a dash ('-') means it's order in descending order.
     * https://github.com/PearsonEducation/Docussandra/issues/9
     */
    @Required("Fields")
    //@ChildValidation
    private List<IndexField> fields;

    //Note: not currently supported -- https://github.com/PearsonEducation/Docussandra/issues/10
    /**
     * Consider the index is only concerned with only a partial dataset. In this
     * case, instead of storing the entire BSON payload, we store only a
     * subset--those listed in setIncludeOnly.
     * https://github.com/PearsonEducation/Docussandra/issues/10
     */
    private List<String> includeOnly;

    /**
     * Field indicating if this index should be presently considered active.
     */
    private boolean active;

    public Index()
    {
        active = false;
    }

    public Index(String name)
    {
        this();
        setName(name);
        active = false;
    }

    public boolean hasTable()
    {
        return (table != null);
    }

    public Table getTable()
    {
        return (hasTable() ? table.asObject() : null);
    }

    public void setTable(String database, String table)
    {
        this.table = new TableReference(database, table);
    }

    public void setTable(Table table)
    {
        this.table = (table != null ? new TableReference(table) : null);
    }

    public String getTableName()
    {
        return (hasTable() ? table.name() : null);
    }

    public String getDatabaseName()
    {
        return (hasTable() ? table.database() : null);
    }

    public String getName()
    {
        return name;
    }

    public Index setName(String name)
    {
        this.name = name;
        return this;
    }

    public boolean isUnique()
    {
        return isUnique;
    }

    public Index isUnique(boolean value)
    {
        this.isUnique = value;
        return this;
    }

    public void setFields(List<IndexField> props)
    {
        this.fields = new ArrayList<>(props);
    }

    public List<IndexField> getFields()
    {
        return (fields == null ? Collections.<IndexField>emptyList() : Collections.unmodifiableList(fields));
    }

    public List<String> getFieldsValues()
    {
        if (fields == null)
        {
            return Collections.<String>emptyList();
        } else
        {
            ArrayList<String> toReturn = new ArrayList<>();
            for (IndexField i : fields)
            {
                toReturn.add(i.getField());
            }
            return Collections.unmodifiableList(toReturn);
        }
    }

    public List<String> getFieldsTypes()
    {
        if (fields == null)
        {
            return Collections.<String>emptyList();
        } else
        {
            ArrayList<String> toReturn = new ArrayList<>();
            for (IndexField i : fields)
            {
                toReturn.add(i.getType().name());
            }
            return Collections.unmodifiableList(toReturn);
        }
    }

    public void setIncludeOnly(List<String> props)
    {
        if (props != null && !props.isEmpty())
        {
            this.includeOnly = new ArrayList<>(props);
        }
    }

    public List<String> getIncludeOnly()
    {
        return (includeOnly == null ? Collections.<String>emptyList() : Collections.unmodifiableList(includeOnly));
    }

    @Override
    public Identifier getId()
    {
        return new Identifier(getDatabaseName(), getTableName(), name);
    }

//    public void iterateFields(Callback<IndexField> callback)
//    {
//        for (IndexField field : getFields)
//        {
//            callback.process(field);
//        }
//    }
    /**
     * Field indicating if this index should be presently considered active.
     *
     * @return the active
     */
    public boolean isActive()
    {
        return active;
    }

    /**
     * Field indicating if this index should be presently considered active.
     *
     * @param isActive the active to set
     */
    public void setActive(boolean isActive)
    {
        this.active = isActive;
    }

    @Override
    public void validate()
    {
        final List<String> errors = new ArrayList<>();

        if (fields == null || fields.isEmpty())
        {
            errors.add("Fields is required.");
        } else
        {
            for (IndexField i : fields)
            {
                if (i.getField() == null)
                {
                    errors.add("Field name is required.");
                }
            }
        }

        Pattern includePattern = Pattern.compile("^\\w+");

        if (includeOnly != null)
        {
            if (includeOnly.isEmpty())
            {
                errors.add("'includeOnly' cannot be empty, if included.");
            }

            for (String field : includeOnly)
            {
                if (!includePattern.matcher(field).matches())
                {
                    errors.add("Invalid 'includeOnly' field name: " + field);
                }
            }
        }

        if (!errors.isEmpty())
        {
            throw new ValidationException(errors);
        }
    }

    @Override
    public int hashCode()
    {
        int hash = 7;
        hash = 97 * hash + Objects.hashCode(this.table);
        hash = 97 * hash + Objects.hashCode(this.name);
        hash = 97 * hash + (this.isUnique ? 1 : 0);
        hash = 97 * hash + Objects.hashCode(this.fields);
        hash = 97 * hash + Objects.hashCode(this.includeOnly);
        hash = 97 * hash + (this.active ? 1 : 0);
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
        final Index other = (Index) obj;
        if (!Objects.equals(this.table, other.table))
        {
            return false;
        }
        if (!Objects.equals(this.name, other.name))
        {
            return false;
        }
        if (this.isUnique != other.isUnique)
        {
            return false;
        }
        if (!Objects.equals(this.fields, other.fields))
        {
            return false;
        }
        if (!Objects.equals(this.includeOnly, other.includeOnly))
        {
            return false;
        }
        if (this.active != other.active)
        {
            return false;
        }
        return true;
    }

    @Override
    public String toString()
    {
        return "Index{" + "table=" + table + ", indexName=" + name + ", isUnique=" + isUnique + ", fields=" + fields + ", includeOnly=" + includeOnly + ", active=" + active + '}';
    }

}
