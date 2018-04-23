## 1. Introduction ##

import psycopg2
conn = psycopg2.connect(dbname='dq',user='hud_admin',password='eRqg123EEkl')
print(conn)

## 2. Investigating the Tables ##

conn = psycopg2.connect(dbname="dq", user="hud_admin", password="eRqg123EEkl")
cur = conn.cursor()
cur.execute('SELECT table_name FROM information_schema.tables ORDER BY table_name')
table_names = cur.fetchall()
for name in table_names:
    print(name)

## 3. Working with Schemas ##

conn = psycopg2.connect(dbname="dq", user="hud_admin", password="eRqg123EEkl")
cur = conn.cursor()
conn = psycopg2.connect(dbname="dq", user="hud_admin", password="eRqg123EEkl")
cur = conn.cursor()

cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name")
for table_name in cur.fetchall():
    name = table_name[0]
    print(name)

## 4. Describing the Tables ##

from psycopg2.extensions import AsIs
conn = psycopg2.connect(dbname="dq", user="hud_admin", password="eRqg123EEkl")
cur = conn.cursor()

cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
for table in cur.fetchall():
    haha_query = cur.mogrify('SELECT * FROM %s LIMIT 0',[AsIs(table[0])])
    cur.execute(haha_query)
    print(cur.description)
    print(' ')

## 5. Type Code Mappings ##

conn = psycopg2.connect(dbname="dq", user="hud_admin", password="eRqg123EEkl")
cur = conn.cursor()
cur.execute('SELECT oid, typname FROM pg_catalog.pg_type')
haha = cur.fetchall()
type_mappings = dict()
for line in haha:
    type_mappings[line[0]] = line[1]

## 6. Readable Description Types ##

from psycopg2.extensions import AsIs
conn = psycopg2.connect(dbname="dq", user="hud_admin", password="eRqg123EEkl")
cur = conn.cursor()
readable_description = dict()
for table in table_names:
    cur.execute('SELECT * FROM %s LIMIT 0',[AsIs(table)])
    readable_description[table] = dict(columns=[dict(name=col.name,type=type_mappings[col.type_code],length=col.internal_size) for col in cur.description])

## 7. Number of Rows ##

from psycopg2.extensions import AsIs
conn = psycopg2.connect(dbname="dq", user="hud_admin", password="eRqg123EEkl")
cur = conn.cursor() 
for table in table_names:
    cur.execute('SELECT COUNT(*) FROM %s',[AsIs(table)])
    readable_description[table]['total'] = cur.fetchone()[0]
print(readable_description)

## 8. Sample Rows ##

from psycopg2.extensions import AsIs
conn = psycopg2.connect(dbname="dq", user="hud_admin", password="eRqg123EEkl")
cur = conn.cursor()
for table in table_names:
    cur.execute('SELECT * FROM %s LIMIT 100',[AsIs(table)])
    readable_description[table]['sample_rows'] = cur.fetchall()
print(readable_description)        