# Docussandra: A REST-based, Document-Oriented Database with Cassandra as a Backend
---

**Docussandra** marries MongoDB's simple data storage model with the horizontal scaling of Cassandra. It enables developers to store arbitrary payloads as
BSON, as they would with MongoDB, in a Cassandra cluster. It supports indexing, filtering, sorting, querying and pagination
(via familiar limit and offset semantics), all at a blazing speed. Simple json document storage with effortless scaling exposed as a service - **that's Docussandra**!

## Five Minute Quick Start
Let's begin by starting a server and beginning to store data. 

### Prerequisites 
A running Cassandra instance and maven (for building Java packages). 

### Setting Up the Cassandra Datastore
Before we can start the Docussandra API server that talks to Cassandra we have to create the keyspace where data will be stored. We do this by loading the schema found in the **rest/src/main/resources/docussandra.cql** directory. On the command line this looks like:

> cqlsh -f rest/src/main/resources/docussandra.cql

### Building the Project
To build the project, type: (Disclaimer: we have negative tests that run, so the output might look scary, it's probably normal.)

> mvn clean install

### Starting the Docussandra API

Next we need to build and run the API server. First, if using a command line, navigate to the **api** project folder in the root project. Once there, type:

> mvn exec:java -Dexec.mainClass="com.pearson.docussandra.Main"

If successful, the Docussandra will now be listening to requests on **127.0.0.1:8081**.

### Creating a Database
Once the server has been started we can begin to interact with it. The first step is a create a database in which to store data. This can be done by making a **POST** request to:
> http://localhost:8081/databases/*{databaseName}*

The *{databaseName}* variable mentioned above can be whatever the user would like *as long as it has not been previously used*. Names must be **globally unique** and **in lower case**. Otherwise an error will result.

### Creating a Table
To create a logical grouping of similar data we'll create a table. We can do this by making another **POST** request to:
> http://localhost:8081/databases/*{databaseName}*/tables/*{tablename}*

Naming restrictions on the table name are similar to the database: unique *to the database* and in lower case. 

### Storing and Retrieving Data
Storing information to Docussandra is simple. To add a JSON document to a table that you've created, POST the document to the table.  
> http://localhost:8081/databases/*{databaseName}*/tables/*{tablename}*/documents

Suppose that we're interested in storing information about the appearances of various superhero characters in comics. First, we need to create the database with Docussandra:

> POST http://localhost:8081/databases/comics

With our comic database created, we now need to create a table for JSON documents that describe our superhero (and villian) appearances.

> POST http://localhost:8081/databases/comics/tables/appearances

What does that appearance JSON document look like? Perhaps we want to store something like:

> {

		"name":"Wolverine",
		"date":"1974-11-01",
		"comic":"Incredible Hulk, The",
		"title":"And Now... the Wolverine!",
		"number":"181"
}

That JSON document contains a number of important pieces of information: the **name** of the character, the **date** of this appearance, the name of the **comic**, the **title** and **number** of the issue. Good stuff, to be sure. But in our use case we are most concerned with returning information *primarily* by the **character name**, followed by the **date** of the appearance. To do that, we need to create an index. We do this by posting to the reserved word **indexes** at our endpoint, followed by the name we're giving that index. 

> POST http://localhost:8081/databases/comics/tables/appearances/indexes/namedate

In the body of the POST, we include the fields within each JSON document that we want Docussandra to index the information by:

> { "fields":["name","date"] }

With the index created, we can now add our data. 

> POST http://localhost:8081/databases/comics/tables/appearances/

with a body of:

{
		"name":"Wolverine",
		"date":"1974-11-01",
		"comic":"Incredible Hulk, The",
		"title":"And Now... the Wolverine!",
		"number":"181"
}
  
*Note*: While the endpoint of "http://localhost:8081/databases/comics/tables/appearances" was used to create the table, data is written to be written to "http://localhost:8081/databases/comics/tables/appearances/documents".

We can retrieve all our appearances by making a GET request to our table:

> GET http://localhost:8081/databases/comics/tables/appearances/documents

## Plugins
Docussandra now supports plugin hooks that allow for custom code to be run as part of Docussandra to support security and notifications of document changes.

### Using Plugins:
To implement and use your own plugins in Docussandra, you must:
1. Create a new Java Project for your plugin/plugins.
2. Include the Docussandra plugins project as a dependency:
```xml
       <dependency>
            <groupId>com.pearson.docussandra</groupId>
            <artifactId>docussandra-plugin</artifactId>
            <version>1.1-SNAPSHOT</version>
        </dependency>
```
3. Implement one or both of the provided interfaces (technically abstract classes): ```NotifierPlugin``` or ```SecurityPlugin```.
4. Build your project with any dependencies into a jar.
5. Rename the jar so that it starts with "plugin"; ensure it keeps it's .jar file extension.
6. Drop the jar (or jars) into the home directory of the user that is running the Docussandra service/application. (Please note, order of execution of plugins is arbitrary, don't expect any ordering unless you provide this yourself. Also note that these plugins are executed synchronously as part of Docussandra's data flow, be cautious and mindful of performance.)

### SecurityPlugins
SecurityPlugins are executed prior to the processing of any (every) HTTP request. You will be provided a ```HashMap``` of HTTP headers, the requested path, and the HttpMethod that is being requested. You should throw a ```PermissionDeniedException``` if the user/client should receive an permissions error, be sure to catch and handle all other exceptions gracefully. Reminder be mindful of performance; some sort of caching is recommended. 
```java
    /**
     * Method that performs a validation based on the passed in headers. Should
     * throw a PermissionDeniedException if the client is not authorized.
     *
     * @param headers Map of the headers.
     * @param requestedPath Path that is being requested.
     * @param method Type of Http method that was requested.
     * @throws PermissionDeniedException If the client is not authorized to make
     * this call.
     */
    public abstract void doValidate(HashMap<String, List<String>> headers, String requestedPath, HttpMethod method) throws PermissionDeniedException;
```
### NotifierPlugins
NotiferPlugins are executed immediately after any document is mutated (created, updated, or deleted). You will be provided with a type of mutation (CREATE, UPDATE, or DELETE) and the Document that has changed. Note, that the Document could be ```null``` (especially in the case of a delete) and you should always check it for ```null```. Depending on the application, these type of notifications could occur *very* frequently, be mindful of performance and resource usage. Be sure to catch all exceptions and handle them gracefully.
```java
    /**
     * This method will get called any time a document is mutated. Be careful
     * about the amount of overhead this method produces, as it will be called
     * frequently.
     *
     * @param type Type of mutation that has occurred.
     * @param document Updated document for this mutation. Will be null if the
     * mutation was a delete, be sure to check for null.
     */
    public abstract void doNotify(MutateType type, Document document);
```

## Credits
### People:
In chronological order by contribution:
* [Todd Fredrich](https://github.com/tfredrich) -- Architect and Concept of Operations
* [Jeffrey DeYoung](https://github.com/JeffreyDeYoung) -- Lead Developer
* [Matthew Rienbold](https://github.com/MatthewReinbold) -- Architect
* [Chandan Kumar](https://github.com/cckumarr) -- Software Engineer
* Anusha Vidap -- Software Development Intern
* [Hillary Moore](https://github.com/umoorhi) -- Project Owner

### Inspiration: 

* Apache Usergrid (http://usergrid.apache.org/)

## Liscense/Copyright
Copyright 2016 Pearson Education, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

