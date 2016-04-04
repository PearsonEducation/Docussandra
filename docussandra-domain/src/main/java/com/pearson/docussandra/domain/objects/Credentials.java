package com.pearson.docussandra.domain.objects;

import com.strategicgains.syntaxe.annotation.Required;

/**
 * @author https://github.com/tfredrich
 * @since Jan 25, 2015
 */
public class Credentials
{

    @Required("Database")
    private DatabaseReference database;

    @Required("Username")
    private String username;

    @Required("Password")
    private String password;
}
