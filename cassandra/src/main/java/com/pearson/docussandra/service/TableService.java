package com.pearson.docussandra.service;

import java.util.List;

import com.pearson.docussandra.domain.objects.Identifier;
import com.pearson.docussandra.domain.objects.Table;
import com.pearson.docussandra.exception.DuplicateItemException;
import com.pearson.docussandra.exception.ItemNotFoundException;
import com.pearson.docussandra.persistence.DatabaseRepository;
import com.pearson.docussandra.persistence.TableRepository;
import com.strategicgains.syntaxe.ValidationEngine;

public class TableService
{

    private TableRepository tables;
    private DatabaseRepository databases;

    public TableService(DatabaseRepository databaseRepository, TableRepository tableRepository)
    {
        super();
        this.databases = databaseRepository;
        this.tables = tableRepository;
    }

    public Table create(Table entity)
    {
        if (!databases.exists(entity.getDatabase().getId()))
        {
            throw new ItemNotFoundException("Database not found: " + entity.getDatabase());
        }
        //check if the table exits
        if (tables.exists(entity.getId()))
        {
            throw new DuplicateItemException("Table name already exists");
        }

        ValidationEngine.validateAndThrow(entity);
        return tables.create(entity);
    }

    public Table read(String database, String table)
    {
        Identifier id = new Identifier(database, table);
        Table t = tables.read(id);

        if (t == null)
        {
            throw new ItemNotFoundException("Table not found: " + id.toString());
        }

        return t;
    }

    public List<Table> readAll(String database)
    {
        Identifier id = new Identifier(database);
        if (!databases.exists(id))
        {
            throw new ItemNotFoundException("Database not found: " + database);
        }

        return tables.readAll(id);
    }

    public void update(Table entity)
    {
        ValidationEngine.validateAndThrow(entity);
        tables.update(entity);
    }

    public void delete(Identifier id)
    {
        tables.delete(id);
    }
}
