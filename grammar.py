__author__ = 'caninemwenja'

from pyparsing import Word, alphanums, alphas, Literal, Suppress, ZeroOrMore, Optional, Group, oneOf

opening_bracket = Suppress("(")
closing_bracket = Suppress(")")
semicolon = Suppress(";")
comma = Suppress(",")
star = Literal("*")

identifier = Word(alphas, alphanums + "_")
values = Word(alphanums+"_"+"'")

cd = Literal("create database")
dd = Literal("drop database")
ud = Literal("use")

ct = Literal("create table")
dt = Literal("drop table")

cf = Literal("create fragment")
df = Literal("drop fragment")

sd = Literal("show databases")
st = Literal("show tables")
sf = Literal("show fragments")

sel = Literal("select")

fr = Suppress("from")
whr = Suppress("where")

location = Word(alphas, alphanums+"_"+"%"+":"+"/"+"+"+"."+"@"+"?"+"=")

column = Group(Optional(identifier+Suppress(".")) + identifier.setResultsName("col_name"))

join_header = Optional(Literal("left") | Literal("right")) + (Literal("join") | Literal("inner join") |
                                                              Literal("outer join"))

join_tail = Group(identifier.setResultsName("table") + Suppress("on") +
                  Group(column + Suppress("=") + column).setResultsName("join_link"))

columns = Group(star | (ZeroOrMore(column + comma) + column))

join = Group(join_header.setResultsName("join_type") + join_tail)

whr_column = Group(column + oneOf([">", "<", ">=", "<=", "=", "!="]) + (values | column))

whrs = whr + Group(ZeroOrMore(whr_column + oneOf(["and", "or"]))+ whr_column)

cds = cd.setResultsName("db_cmd") + identifier.setResultsName("db_name")
dds = dd.setResultsName("db_cmd") + identifier.setResultsName("db_name")
uds = ud.setResultsName("db_cmd") + identifier.setResultsName("db_name")

cts = ct.setResultsName("db_cmd") + identifier.setResultsName("table_name")
dts = dt.setResultsName("db_cmd") + identifier.setResultsName("table_name")

cfs = cf.setResultsName("db_cmd") + identifier.setResultsName("fragment_name") + \
      location.setResultsName("location") + Suppress("on") + identifier.setResultsName("table_name") + \
      opening_bracket + columns.setResultsName("columns") + closing_bracket

dfs = df.setResultsName("db_cmd") + identifier.setResultsName("fragment_name") + Suppress("on") + \
      identifier.setResultsName("table_name")

sds = sd.setResultsName("db_cmd")
sts = st.setResultsName("db_cmd")
sfs = sf.setResultsName("db_cmd") + Suppress("on") + identifier.setResultsName("table_name")

sels = sel.setResultsName("db_cmd") + columns.setResultsName("columns") + fr + identifier.setResultsName("main_table") + \
       ZeroOrMore(join).setResultsName("joins") + Optional(whrs).setResultsName("selection")

sql = (cds | dds | uds | cts | dts | cfs | dfs | sds | sts | sfs | sels) + semicolon
