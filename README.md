Siafu
=====

Siafu is a simple distributed database management system powered by pyparsing, sqlalchemy and twisted.

It's a daemon that runs on port 7890 and allows you to configure and use a distributed database.
*See Usage below.*

Siafu uses a local sqlite database called `siafu.db` to maintain state - **don't delete**. This can
be changed by modifying the **DATABASE** variable in [siafu/settings.py](siafu/settings.py).

It features:
* fragmentation support (simple)
* namespacing of distributed databases
* distributed query processing - select only (simple with very little optimization)

Installing
-----------

1. Clone this repo to your machine, eg. `$ git clone https://github.com/caninemwenja/siafu.git`
2. Go into the repo directory, `$ cd siafu/`
3. Install the required libraries (probably wise if you use virtualenv), `$ pip install -r requirements.txt`
4. Run the server, `$ python server.py` *some logging will appear here*
5. Connect to the server from another terminal, `$ telnet localhost 7890`

Example
-------

Run the following before the code below (assuming you've installed **siafu** and **sqlite3** on your system):

    $ sqlite3 frag1.db
    $ sqlite> CREATE TABLE my_table(id, name);
    $ sqlite> INSERT INTO my_table VALUES(1, 'Riri');
    $ sqlite> INSERT INTO my_table VALUES(2, 'Nexos');
    $ sqlite> .quit

    $ sqlite3 frag2.db
    $ sqlite> CREATE TABLE my_table(id, gender);
    $ sqlite> INSERT INTO my_table VALUES(1, 'female');
    $ sqlite> INSERT INTO my_table VALUES(2, 'male');
    $ sqlite> .quit

    $ sqlite3 frag3.db
    $ sqlite> CREATE TABLE my_table2(id, fee);
    $ sqlite> INSERT INTO my_table2 VALUES(1, 7000);
    $ sqlite> .quit

    $ sqlite3 frag4.db
    $ sqlite> CREATE TABLE my_table2(id, fee);
    $ sqlite> INSERT INTO my_table2 VALUES(2, 3000);
    $ sqlite> .quit

Run the following statements after connecting to the server (step 5 in Installing).

*In the following example, leave out the sections starting with a **#** (they're just helpful notes for the example).*


    CREATE DATABASE my_database; # create a distributed database
    SHOW DATABASES; # to see whether your database was created (you can skip this)
    USE my_database; # select the database for use
    CREATE TABLE my_table; # create a table to fragment
    SHOW TABLES; # to see whether your was created (you can skip this as well)


### Vertical Fragmentation

* **all fragments should have the same table name (but internally fragmented, columnwise)**
* **all columns have to specified in this format: table.column**


    CREATE FRAGMENT fragment_1 sqlite:///frag1.db ON my_table (id, name);
    CREATE FRAGMENT fragment_2 sqlite:///frag2.db ON my_table (id, gender); # for VF to work you need a unifying attributed eg id here

    # this next step assumes you've already created the sqlite databases frag1.db and frag2.db, and the table my_table in each of them and filled them with some test data
    SELECT my_table.id, my_table.name, my_table.gender FROM my_table; # you have to specify the columns in this format exactly: table.column

### Horizontal Fragmentation

* **all fragments should have the same table name (but internally fragmented, datawise)**
* **all columns have to specified in this format: table.column**


    CREATE TABLE my_table2; # create a new table to play with

    CREATE FRAGMENT fragment_1 sqlite:///frag3.db ON my_table2 (*);
    CREATE FRAGMENT fragment_2 sqlite:///frag4.db ON my_table2 (*);

    # this assumes the fragments are already created and pre-filled with fragmented data
    SELECT my_table2.id, my_table2.fee FROM my_table2;

### Joins on Fragmented table

    SELECT my_table.id, my_table.name, my_table2.fee FROM my_table JOIN my_table2 ON my_table.id = my_table2.id;


Usage
-----

Siafu is used through SQL-like queries. Below is a highlight of some (most) of them:

* `CREATE DATABASE <database name>;` - creates a "distributed database" which is basically a namespace for defining your
distributed database's objects.
* `DROP DATABASE <database name>;` - destroys the whole namespace (database, tables, fragments) and deselects it
* `USE <database name>` - changes the current namespace to the one named after the database, this allows you to create
tables and fragments under this database
* `CREATE TABLE <table name>;` - creates a table under the current "distributed database". This is the table that acts as
 the unifier of all the fragments.
* `DROP TABLE <table name>;` - destroys the table and all the fragments associated with it
* `CREATE FRAGMENT <fragment name> <location> ON <table> (<column>, <column>, ..);` - creates a fragment on the table specified,
**it doesn't create the actual fragment** but creates a local representation of it. That is left to the user (See Example
bove). The location field is an sqlalchemy engine configuration string eg `mysql://localhost/mydb`. The user can specify
which columns are in the fragment in the brackets. If all columns are found in the target fragment then the user can
instead use `*`. eg `CREATE FRAGMENT my_fragment mysql://localhost/mydb ON my_table (*);`
* `DROP FRAGMENT <fragment name>;` - removes the fragment from the table (ie its reference that)
* `SHOW DATABASE;` - shows all the distributed databases
* `SHOW TABLES;` - shows all the tables in the selected distributed database namespace
* `SHOW FRAGMENTS ON <table>` - show all the fragments associtated with a table
* `SELECT <table.column, table2.column, ...> FROM table [JOIN table2 ON table.column = table2.column] [WHERE table.column ...]` - simple select statement.
All column references have to be in the form **table.column**. All joins have to be in the form **FROM table1 JOIN table2 ON table1.column = table2.column**.
The select statement will resolve the fragmentation and return the requested data.


Authors
-------
* [caninemwenja](http://github.com/caninemwenja)
