__author__ = 'kmwenja'

import sqlalchemy

import grammar

from pyparsing import ParseException

from random import randint
from sys import maxsize
from base64 import b64decode, b64encode

SIAFU_SQL = """
CREATE TABLE IF NOT EXISTS siafu_databases (
    ID integer not null primary key,
    NAME varchar(100) not null,
    CREATED timestamp default CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS siafu_tables (
    ID integer not null primary key,
    NAME varchar(100) not null,
    DB varchar(100) not null,
    CREATED timestamp default CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS siafu_fragments(
  ID integer not null primary key,
  NAME varchar(100) not null,
  TBL varchar(100) not null,
  DB varchar(100) not null,
  LOC varchar(200) not null,
  COLS varchar(500) not null,
  CREATED timestamp default CURRENT_TIMESTAMP
);
"""


class Select(object):

    def __init__(self, parse_results):
        self.parsed = parse_results

    def tables(self):
        tbls = []
        tbls.append(self.parsed['main_table'])

        try:
            for j in self.parsed['joins']:
                tbls.append(j[1][0])
        except KeyError:
            pass

        return set(tbls)

    def columns(self, table=None):
        cols = self.projection(table)

        try:
            for j in self.parsed['joins']:
                if table:
                    for c in j[1][1]:
                        if c[0] == table:
                            cols.append(c[1])

                if not table:
                    for c in j[1][1]:
                        cols.append(c[1])
        except KeyError:
            pass

        try:
            for s in self.parsed['selection'][0]:
                if type(s) == type(self.parsed):
                    if s[0][0] == table:
                        cols.append(s[0][1])

                    if s[2][0] == table:
                        cols.append(s[2][1])

                    if not table:
                        if type(s[0]) == type(s):
                            cols.append(s[0][1])
                        if type(s[2]) == type(s):
                            cols.append(s[2][1])
        except KeyError:
            pass

        return set(cols)

    def joins(self):
        js = ""
        js += self.parsed['main_table']

        try:
            for j in self.parsed['joins']:
                js += " " + j[0] + " " + j[1][0] + " on " + ".".join(j[1][1][0]) + "=" + ".".join(j[1][1][1])
        except KeyError:
            pass

        return js

    def projection(self, table=None):
        cols = []
        for column in self.parsed['columns']:
            if table and column[0] == table:
                cols.append(column[1])

            if not table:
                cols.append(column[1])

        return cols

    def selection(self):
        sel = ""

        try:
            for s in self.parsed['selection'][0]:
                if type(s) == type(self.parsed):
                    if type(s[0]) == type(self.parsed):
                        sel += " " + s[0][0] + "." +s[0][1]
                    else:
                        sel += " " + s[0]
                    sel += " " + s[1]
                    if type(s[2]) == type(self.parsed):
                        sel += " " + s[2][0] + "." + s[2][1]
                    else:
                        sel += " " + s[2]
                else:
                    sel += " " + s
        except KeyError:
            pass

        return sel

    def process(self, siafu):
        # for each table select and retrieve relevant data
        for table in self.tables():

            link = None

            if not siafu.table_exists(table):
                raise SiafuError(
                "SELECT ... FROM {0}".format(table),
                "Table does not exist")

            columns = self.columns(table)
            # retrieve table fragments
            rows = siafu.get_fragments(table)

            data = []
            cols = []

            for row in rows:
                location = b64decode(row['LOC'])
                connection = sqlalchemy.create_engine(location).connect()

                if row['COLS'] == '*':
                    query = "SELECT {0} FROM {1}".format(
                        ",".join(columns),
                        table)
                    frag_rows = connection.execute(query).fetchall()
                    data.extend(frag_rows)

                    connection.close()
                else:
                    query = "SELECT {0} FROM {1}".format(
                        row['COLS'], table)
                    frag_rows = connection.execute(query).fetchall()

                    cols.append(row['COLS'].split(","))

                    data.append(frag_rows)
                    print data

                connection.close()

            if len(cols) > 0:
                print cols
                central = set(cols[0])
                for c in cols:
                    central = set(c).intersection(central)

                link = list(central)[0]
                part_tables = []

                for j in range(len(data)):
                    part = data[j]
                    part_table_name = table+"_part_"+repr(j)
                    part_cols = cols[j]

                    query = "DROP TABLE IF EXISTS {0}".format(part_table_name)
                    siafu.connection.execute(query)

                    query = "CREATE TEMP TABLE {0} ({1})"

                    part_tables.append(part_table_name)
                    siafu.connection.execute(query.format(
                        part_table_name, ",".join(part_cols)))
                    query = "INSERT INTO {0}({1}) VALUES({2})"
                    for row in part:
                        vals = ""
                        values = row.values()
                        for i in range(len(values)):
                            if type(u"") == type(values[i]):
                                vals += "\'"+values[i]+"\'"
                            else:
                                vals += repr(values[i])
                            if i < (len(values) - 1):
                                vals += ","


                        # insert_cols = [part_table_name+"."+x for x in part_cols]

                        siafu.connection.execute(query.format(
                            part_table_name, ",".join(part_cols), vals))

                query = "SELECT {0} FROM {1} JOIN {2}"
                join_str = ""
                prev_table = part_tables[0]
                for i in range(1, len(part_tables)):
                    part_table = part_tables[i]
                    join_str += part_table + " ON " + part_table + "." + link + "=" + prev_table + "." + link
                    prev_table = part_table

                cols_without_link = set(columns).difference(central)
                columns1 = list(cols_without_link)
                columns1.append(part_tables[0]+"."+link)

                query = query.format(",".join(columns1), part_tables[0], join_str)
                print query
                data = siafu.connection.execute(query).fetchall()

            query = "DROP TABLE IF EXISTS {0}".format(table)
            siafu.connection.execute(query)

            create_cols = list(columns)
            if link and link not in create_cols:
                create_cols.append(link)

            query = "CREATE TEMP TABLE {0} ({1})"
            siafu.connection.execute(query.format(
                table, ",".join(create_cols)))

            query = "INSERT INTO {0}({1}) VALUES({2})"
            for row in data:
                vals = ""
                values = row.values()
                for i in range(len(values)):
                    if type(1L) == type(values[i]):
                        vals += str(values[i])
                    elif type(u"") == type(values[i]):
                        vals += "\'"+values[i]+"\'"
                    else:
                        vals += repr(values[i])
                    if i < (len(values) - 1):
                        vals += ","

                siafu.connection.execute(query.format(
                    table, ",".join(create_cols), vals))

        query = "select {0} from {1} where {2}" if len(self.selection()) > 0 else "select {0} from {1}"
        results = siafu.connection.execute(query.format(
            ",".join(self.projection()), self.joins(), self.selection()))
        rows = results.fetchall()

        return rows


class SiafuError(Exception):

    def __init__(self, operation, reason):
        self.operation = operation
        self.reason = reason

    def __unicode__(self):
        return "{0} on {1}".format(self.reason, self.operation)


class SiafuSyntaxError(Exception):

    def __init__(self, sql, reason, position=None):
        self.sql = sql
        self.reason = reason
        self.position = position

    def __unicode__(self):
        return "{0} in {1} at {2}".format(
            self.reason,
            self.sql,
            self.position
        )


class Siafu(object):

    def __init__(self, db_conn_string):
        # setup database connection
        self.engine = sqlalchemy.create_engine(db_conn_string)
        self.connection = self.engine.connect()
        self.current_database = None

        # initialize tables if non existent
        sql_statements = SIAFU_SQL.split(";")
        for statement in sql_statements:
            self.connection.execute(statement)

    def process_sql(self, sql):
        try:
            # sql_string = sql.lower()
            sql_string = sql
            res = grammar.sql.parseString(sql_string)

            cmd = res['db_cmd']

            if cmd == 'use':
                self.use_database(res['db_name'])
                return "USE {0}".format(res['db_name'])

            if cmd == 'create database':
                self.create_database(res['db_name'])
                return "CREATE DB {0}".format(res['db_name'])

            if cmd == 'drop database':
                self.drop_database(res['db_name'])
                return "DROP DB {0}".format(res['db_name'])

            if cmd == 'create table':
                self.create_table(res['table_name'])
                return "CREATE TABLE {0}".format(res['table_name'])

            if cmd == 'drop table':
                self.drop_table(res['table_name'])
                return "DROP TABLE {0}".format(res['table_name'])

            if cmd == 'create fragment':
                self.create_fragment(res['fragment_name'], res['table_name'], res['location'], res['columns'])
                return "CREATE FRAGMENT {0}".format(res['fragment_name'])

            if cmd == 'drop fragment':
                self.drop_fragment(res['fragment_name'], res['table_name'])
                return "DROP FRAGMENT {0}".format(res['fragment_name'])

            if cmd == 'show databases':
                return str(self.show_databases())

            if cmd == 'show tables':
                return str(self.show_tables())

            if cmd == 'show fragments':
                return str(self.show_fragments(res['table_name']))

            if cmd == 'select':
                sel = Select(res)
                return str(sel.process(self))

            raise SiafuError("SQL Parsing: {0}".format(sql_string),
                             "SQL not supported")
        except ParseException, pe:
            raise SiafuSyntaxError(
                sql, "Could not parse sql: {0}".format(pe.msg), pe.loc)

    def create_fragment(self, name, table, location, columns):
        current_op = "CREATE FRAGMENT {0}".format(name)

        if not self.current_database:
            raise SiafuError(
                current_op,
                "No database selected")

        if not self.table_exists(table):
            raise SiafuError(
                current_op,
                "Table does not exist")

        query = "INSERT INTO siafu_fragments(ID, NAME, TBL, DB, LOC, COLS) " \
                "VALUES({0},'{1}','{2}','{3}','{4}', '{5}')"

        cols = '*'

        if columns[0] != '*':
            cols = ",".join([c['col_name'] for c in columns])

        self.connection.execute(query.format(
            self.random(),
            name,
            table,
            self.current_database,
            b64encode(location),
            cols))

    def drop_fragment(self, name, table):
        current_op = "DROP PHF {0}".format(name)

        if not self.current_database:
            raise SiafuError(
                current_op,
                "No database selected")

        if not self.table_exists(table):
            raise SiafuError(
                current_op,
                "Table does not exist")

        query = "DELETE FROM siafu_fragments WHERE NAME = '{0}' AND TBL='{1}' AND DB='{2}'"
        self.connection.execute(query.format(
            name,
            table,
            self.current_database))

    def table_exists(self, name):
        query = "SELECT * FROM siafu_tables WHERE NAME = '{0}' AND DB = '{1}'"
        res = self.connection.execute(query.format(name, self.current_database))
        rows = res.fetchall()

        return rows is not None and len(rows) > 0

    def random(self):
        return randint(0, maxsize)

    def create_table(self, name):
        current_op = "CREATE TABLE {0}".format(name)

        if not self.current_database:
            raise SiafuError(
                current_op,
                "No database selected")

        if self.table_exists(name):
            raise SiafuError(
                current_op,
                "Table already exists")

        query = "INSERT INTO siafu_tables(ID, NAME, DB) " \
                "VALUES({0}, '{1}', '{2}')"
        self.connection.execute(query.format(
            self.random(),
            name,
            self.current_database))

    def drop_table(self, name):
        current_op = "DROP TABLE {0}".format(name)

        if not self.current_database:
            raise SiafuError(
                current_op,
                "No database selected")

        if not self.table_exists(name):
            raise SiafuError(
                current_op,
                "Table does not exist")

        query = "DELETE FROM siafu_tables WHERE NAME='{0}' AND DB = '{1}'"
        self.connection.execute(query.format(name, self.current_database))

        query = "DELETE FROM siafu_fragments WHERE TBL='{0}' AND DB = '{1}'"
        self.connection.execute(query.format(name, self.current_database))

    def create_database(self, name):
        if self.database_exists(name):
            raise SiafuError(
                "CREATE DATABASE {0}".format(name),
                "Database already exists")

        query = "INSERT INTO siafu_databases(ID, NAME) " \
                "VALUES({0}, '{1}')"
        self.connection.execute(query.format(
            self.random(), name))

    def database_exists(self, name):
        query = "SELECT * FROM siafu_databases WHERE NAME = '{0}'"
        res = self.connection.execute(query.format(name))
        rows = res.fetchall()

        return rows is not None and len(rows) > 0

    def use_database(self, name):
        if not self.database_exists(name):
            raise SiafuError(
                "USE {0}".format(name),
                "No database by that name")

        self.current_database = name

    def drop_database(self, name):
        if not self.database_exists(name):
            raise SiafuError(
                "DROP DATABASE {0}".format(name),
                "No database by that name")

        query = "DELETE FROM siafu_databases WHERE NAME='{0}'"
        self.connection.execute(query.format(name))

        query = "DELETE FROM siafu_tables WHERE DB='{0}'"
        self.connection.execute(query.format(name))

        query = "DELETE FROM siafu_fragments WHERE DB='{0}'"
        self.connection.execute(query.format(name))

        if self.current_database == name:
            self.current_database = None

    def show_databases(self):
        query = "SELECT * FROM siafu_databases"
        rows = self.connection.execute(query).fetchall()

        print_out = "DATABASES\n"
        print_out += "----------\n"
        for row in rows:
            print_out += row['NAME'] + "\n"

        return print_out

    def show_tables(self):
        if not self.current_database:
            raise SiafuError(
                "SHOW TABLES",
                "No database selected")

        query = "SELECT * FROM siafu_tables WHERE DB='{0}'"
        rows = self.connection.execute(query.format(
            self.current_database)).fetchall()

        print_out = "TABLES\n"
        print_out += "-------\n"
        for row in rows:
            print_out += row['NAME'] + "\n"

        return print_out

    def show_fragments(self, table):
        current_op = "SHOW FRAGMENTS ON {0}".format(table)

        if not self.current_database:
            raise SiafuError(
                "SHOW TABLES",
                "No database selected")

        if not self.table_exists(table):
            raise SiafuError(
                current_op,
                "Table does not exist")

        rows = self.get_fragments(table)

        print_out = "FRAGMENTS ON {0}\n".format(table)
        print_out += "-----------------------\n"
        print_out += "NAME | LOC | COLS\n"
        print_out += "-----------------------\n"
        for row in rows:
            print_out += row['NAME'] + " | " + b64decode(row['LOC']) + " | " + row["COLS"] + "\n"

        return print_out

    def get_fragments(self, table):
        query = "SELECT * FROM siafu_fragments WHERE DB='{0}' AND TBL='{1}'"
        rows = self.connection.execute(query.format(
            self.current_database, table)).fetchall()

        return rows
