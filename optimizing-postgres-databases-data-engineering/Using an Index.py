## 1. Alternate Table Scans ##

conn = psycopg2.connect(dbname="dq", user="hud_admin", password="abc123")
cur = conn.cursor()
query = "EXPLAIN (format json) SELECT * FROM homeless_by_coc WHERE id=10"
cur.execute(query)
pp.pprint(cur.fetchall())

## 2. Index Scan ##

conn = psycopg2.connect(dbname="dq", user="hud_admin", password="abc123")
cur = conn.cursor()
query = "EXPLAIN (format json) SELECT * FROM homeless_by_coc WHERE id=5"
cur.execute(query)
homeless_query_plan = cur.fetchall()
query = "EXPLAIN (format json) SELECT * FROM state_info WHERE name = 'Alabama'"
cur.execute(query)
state_query_plan = cur.fetchall()
query = "EXPLAIN (format json) SELECT * FROM state_household_incomes WHERE state = 'Georgia'"
cur.execute(query)
incomes_query_plan = cur.fetchall()
pp.pprint(homeless_query_plan)
pp.pprint(state_query_plan)
pp.pprint(incomes_query_plan)

## 4. Indexing ##

conn = psycopg2.connect(dbname="dq", user="hud_admin", password="abc123")
cur = conn.cursor()
query = "CREATE TABLE state_idx(state VARCHAR(2),homeless_id INTEGER,PRIMARY KEY(state,homeless_id))"
cur.execute(query)
query = "INSERT INTO state_idx (state,homeless_id) SELECT state, id FROM homeless_by_coc"
cur.execute(query)
conn.commit()
query = "SELECT hbc.state,hbc.year,hbc.coc_number FROM homeless_by_coc as hbc, state_idx as si WHERE si.homeless_id = hbc.id AND si.state = 'CA'"
cur.execute(query)
pp.pprint(cur.fetchall())

## 5. Comparing the Queries ##

conn = psycopg2.connect(dbname="dq", user="hud_admin", password="abc123")
cur = conn.cursor()
cur.execute("""
SELECT hbc.id, hbc.year, hbc.coc_number FROM homeless_by_coc hbc, state_idx
WHERE state_idx.state = 'CA' AND state_idx.homeless_id = hbc.id
""")
conn = psycopg2.connect(dbname="dq", user="hud_admin", password="abc123")
cur = conn.cursor()
cur.execute("""
EXPLAIN (ANALYZE, format json) SELECT hbc.id, hbc.year, hbc.coc_number FROM homeless_by_coc hbc, state_idx
WHERE state_idx.state = 'CA' AND state_idx.homeless_id = hbc.id
""")
pp.pprint(cur.fetchall())
cur.execute("""
EXPLAIN (ANALYZE, format json) SELECT id, year, coc_number FROM homeless_by_coc WHERE state='CA'
""")
pp.pprint(cur.fetchall())

## 6. Create an Index ##

conn = psycopg2.connect(dbname="dq", user="hud_admin", password="abc123")
cur = conn.cursor()
conn = psycopg2.connect(dbname="dq", user="hud_admin", password="abc123")
cur = conn.cursor()
cur.execute("CREATE INDEX state_idx ON homeless_by_coc(state)")
conn.commit()
cur.execute("EXPLAIN (ANALYZE, format json) SELECT * FROM homeless_by_coc WHERE state='CA'")
pp.pprint(cur.fetchall())

## 7. Dropping Indexes ##

conn = psycopg2.connect(dbname="dq", user="hud_admin", password="abc123")
cur = conn.cursor()
#cur.execute("CREATE INDEX state_idx ON homeless_by_coc(state)")
#conn.commit()
#cur.execute("EXPLAIN (ANALYZE, format json) SELECT * FROM homeless_by_coc WHERE state='CA'")
#pp.pprint(cur.fetchall())
cur.execute("DROP INDEX IF EXISTS state_idx")
conn.commit()
cur.execute("EXPLAIN (ANALYZE,format json) SELECT * FROM homeless_by_coc WHERE state = 'CA'")
pp.pprint(cur.fetchall())

## 8. Index Performance on Joins ##

conn = psycopg2.connect(dbname="dq", user="hud_admin", password="abc123")
cur = conn.cursor()
#cur.execute("CREATE INDEX state_idx ON homeless_by_coc(state)")
#conn.commit()
#query = "CREATE INDEX state_idx ON homeless_by_coc(state)"
#cur.execute(query)
#conn.commit()
query = "EXPLAIN ANALYZE SELECT hbc.state, hbc.coc_number, hbc.coc_name, si.name FROM homeless_by_coc as hbc, state_info as si WHERE hbc.state = si.postal"
cur.execute(query)
pp.pprint(cur.fetchall())

query = "DROP INDEX IF EXISTS state_idx"
cur.execute(query)
conn.commit()
query = "EXPLAIN ANALYZE SELECT hbc.state, hbc.coc_number, hbc.coc_name, si.name FROM homeless_by_coc as hbc, state_info as si WHERE hbc.state = si.postal"
cur.execute(query)
pp.pprint(cur.fetchall())