## 1. Querying with Multiple Filters ##

conn = psycopg2.connect(dbname="dq", user="hud_admin", password="abc123")
cur = conn.cursor()
query = "CREATE INDEX state_idx ON homeless_by_coc(state)"
cur.execute(query)
conn.commit()
query = "EXPLAIN (format json) SELECT * FROM homeless_by_coc WHERE state='CA' AND year > '1991-01-01'"
cur.execute(query)
pp.pprint(cur.fetchall())

## 3. Adding Another Index ##

conn = psycopg2.connect(dbname="dq", user="hud_admin", password="abc123")
cur = conn.cursor()
cur.execute("CREATE INDEX state_idx ON homeless_by_coc(state)")
conn.commit()
cur.execute("EXPLAIN (ANALYZE,format json) SELECT * FROM homeless_by_coc WHERE state='CA' AND  year > '1991-01-01'")
pp.pprint(cur.fetchall())
cur.execute("DROP INDEX IF EXISTS state_idx")
conn.commit()
cur.execute("CREATE INDEX state_year_idx ON homeless_by_coc(state,year)")
conn.commit()
cur.execute("EXPLAIN (ANALYZE,format json) SELECT * FROM homeless_by_coc WHERE state='CA' AND  year > '1991-01-01'")
pp.pprint(cur.fetchall())

## 4. Multiple Indexes ##

conn = psycopg2.connect(dbname="dq", user="hud_admin", password="abc123")
cur = conn.cursor()
cur.execute("CREATE INDEX state_year_coc_number_idx ON homeless_by_coc(state,year,coc_number)")
conn.commit()

## 5. The Tradeoff of Using Indexes ##

import time
conn = psycopg2.connect(dbname="dq", user="hud_admin", password="abc123")
cur = conn.cursor()
filename = 'homeless_by_coc.csv'

start_time = time.time()
with open(filename) as f:
    statement = cur.mogrify('COPY %s FROM STDIN WITH CSV HEADER', (AsIs(filename.split('.')[0]), ))
    cur.copy_expert(statement, f)
print(time.time() - start_time)

cur.execute('DELETE FROM homeless_by_coc')
cur.execute('CREATE INDEX state_year_idx ON homeless_by_coc(state, year)')

start_time = time.time()
with open(filename) as f:    
    statement = cur.mogrify('COPY %s FROM STDIN WITH CSV HEADER', (AsIs(filename.split('.')[0]), ))
    cur.copy_expert(statement, f)
print(time.time() - start_time)


## 6. Order By Index ##

conn = psycopg2.connect(dbname="dq", user="hud_admin", password="abc123")
cur = conn.cursor()
cur.execute('DROP INDEX haha_idx')
conn.commit()
cur.execute('CREATE INDEX haha_idx ON homeless_by_coc(state,year ASC)')
conn.commit()
cur.execute("SELECT DISTINCT year FROM homeless_by_coc WHERE state='CA' AND year > '1991-01-01'")
ordered_years = cur.fetchall()
pp.pprint(ordered_years)

## 7. Index on Expressions ##

conn = psycopg2.connect(dbname="dq", user="hud_admin", password="abc123")
cur = conn.cursor()
cur.execute('DROP INDEX m_idx')
conn.commit()
cur.execute('CREATE INDEX m_idx ON homeless_by_coc(lower(measures))')
conn.commit()
cur.execute("SELECT * FROM homeless_by_coc WHERE lower(measures) = 'unsheltered homeless people in families' LIMIT 1")
unsheltered_row=cur.fetchone()

## 8. Partial Indexes ##

conn = psycopg2.connect(dbname="dq", user="hud_admin", password="abc123")
cur = conn.cursor()
cur.execute("DROP INDEX state_idx")
conn.commit()
cur.execute("CREATE INDEX state_idx ON homeless_by_coc(state) WHERE count > 0")
conn.commit()
cur.execute("EXPLAIN ANALYZE SELECT * FROM homeless_by_coc WHERE state='CA' AND count > 0")
pp.pprint(cur.fetchall())

## 9. Building a Multi-Column Index ##

conn = psycopg2.connect(dbname="dq", user="hud_admin", password="abc123")
cur = conn.cursor()
cur.execute("CREATE INDEX year_count ON homeless_by_coc(year,count)")
conn.commit()
query = "EXPLAIN (ANALYZE, format json) SELECT hbc.year,si.name,hbc.count FROM homeless_by_coc hbc, state_info si WHERE hbc.state=si.postal AND hbc.year>'2007-01-01' AND hbc.measures != 'total homeless'"
cur.execute(query)
pp.pprint(cur.fetchall())