import psycopg2

conn = psycopg2.connect("dbname=jarodmeng user=jarodmeng")
cur = conn.cursor()
cur.execute("CREATE TABLE tweets (tweet_id text, text text, user_id text, user_screen_name text, user_name text, created_at timestamp, retweets integer, favorites integer);")
conn.commit()
cur.close()
conn.close()
