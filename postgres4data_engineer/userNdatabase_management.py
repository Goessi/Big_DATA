## 1. Connection String ##

import psycopg2
conn = psycopg2.connect(dbname = 'dq',user='postgres',password = 'abc123')
print(conn)

## 2. Creating a User ##

conn = psycopg2.connect(dbname='dq',user='postgres',password='password')
cur = conn.cursor()
query = "CREATE USER data_viewer WITH PASSWORD 'somepassword'"
cur.execute(query)
conn.commit()

## 3. User Privileges ##

conn = psycopg2.connect(dbname="dq", user="dq")
cur = conn.cursor()
query = 'REVOKE ALL ON user_accounts FROM data_viewer'
cur.execute(query)

## 4. Granting Privileges ##

conn = psycopg2.connect(dbname="dq", user="dq")
cur = conn.cursor()
query = 'GRANT SELECT ON user_accounts TO data_viewer'
cur.execute(query)
conn.commit()

## 5. Postgres Groups ##

conn = psycopg2.connect(dbname="dq", user="dq")
cur = conn.cursor()
query = 'CREATE GROUP readonly NOLOGIN'
cur.execute(query)
query = 'REVOKE ALL ON user_accounts FROM readonly'
cur.execute(query)
query = 'GRANT SELECT ON user_accounts TO readonly'
cur.execute(query)
query = 'GRANT readonly TO data_viewer'
cur.execute(query)
conn.commit()

## 6. Creating a database ##

conn = psycopg2.connect(dbname="dq", user="dq")
conn.autocommit = True
cur = conn.cursor()
query = 'CREATE DATABASE user_accounts OWNER data_viewer'
cur.execute(query)

## 7. Putting It All Together ##

conn = psycopg2.connect(dbname="dq", user="dq")
conn.autocommit = True
cur = conn.cursor()
cur.execute('''SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = 'top_secret' AND pid <> pg_backend_pid()''')
cur.execute('DROP DATABASE IF EXISTS top_secret')
cur.execute('''SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = 'top_secret' AND pid <> pg_backend_pid()''')
cur.execute("CREATE DATABASE top_secret OWNER dq")
conn = psycopg2.connect(dbname="top_secret", user="dq")
cur = conn.cursor()
cur.execute("""
CREATE TABLE documents(id INT, info TEXT);
DROP GROUP IF EXISTS spies;
CREATE GROUP spies NOLOGIN;
REVOKE ALL ON documents FROM spies;
GRANT SELECT, INSERT, UPDATE ON documents TO spies;
DROP USER IF EXISTS double_o_7;
CREATE USER double_o_7 WITH CREATEDB PASSWORD 'shakennotstirred' IN GROUP spies;
""")
conn.commit()
conn_007 = psycopg2.connect(dbname='top_secret', user='double_o_7', password='shakennotstirred')
