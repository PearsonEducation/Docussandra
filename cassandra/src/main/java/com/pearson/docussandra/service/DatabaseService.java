package com.pearson.docussandra.service;

import java.util.List;

import com.pearson.docussandra.domain.objects.Database;
import com.pearson.docussandra.domain.objects.Identifier;
import com.pearson.docussandra.exception.DuplicateItemException;
import com.pearson.docussandra.exception.ItemNotFoundException;
import com.pearson.docussandra.persistence.DatabaseRepository;
import com.strategicgains.syntaxe.ValidationEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DatabaseService
{

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private DatabaseRepository databases;

    public DatabaseService(DatabaseRepository databaseRepository)
    {
        super();
        this.databases = databaseRepository;
    }

    public Database create(Database entity)
    {
        logger.info("Attempting to create database: " + entity);
        //check if database name exists
        if(databases.exists(entity.getId())) {
            throw new DuplicateItemException("Database name already exists");
        }
        else{
            ValidationEngine.validateAndThrow(entity);
            return databases.create(entity);
        }
    }

    public Database read(String name)
    {
        Database n = databases.read(new Identifier(name));

        if (n == null)
        {
            throw new ItemNotFoundException("Database not found: " + name);
        }

        return n;
    }

    public List<Database> readAll()
    {
        return databases.readAll();
    }

    public void update(Database entity)
    {
        logger.info("Attempting to update database: " + entity.name());
        ValidationEngine.validateAndThrow(entity);
        databases.update(entity);
    }

    public void delete(String name)
    {
        logger.info("Attempting to delete database: " + name);
        databases.delete(new Identifier(name));
    }
}
