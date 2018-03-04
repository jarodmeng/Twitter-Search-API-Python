import datetime
import TwitterScraper as t
import psycopg2

__author__ = "Jarod Meng"

class TwitterDB(t.TwitterSearch):
    """
    Store tweets into database
    """
    def __init__(self, rate_delay, error_delay, max_tweets, conn):
        super(TwitterDB, self).__init__(rate_delay, error_delay)
        self.max_tweets = max_tweets
        self.counter = 0
        self.conn = conn

    def replace_created_at(self, t):
        if t["created_at"] is not None:
            t["created_at"] = datetime.datetime.fromtimestamp(t["created_at"]/1000)

        return t

    def pop_query(self, t, query):
        t["query"] = query

        return t

    def save_tweets(self, tweets, query):
        """
        Save tweets to postgresql
        :return: True always
        """
        cur = self.conn.cursor()
        replaced_tweets = [self.replace_created_at(t) for t in tweets if t["created_at"] is not None]
        pop_tweets = [self.pop_query(t, query) for t in replaced_tweets]
        cur.executemany("""INSERT INTO tweets(tweet_id,text,user_id,user_screen_name,user_name,created_at,retweets,favorites,query) VALUES(%(tweet_id)s, %(text)s, %(user_id)s, %(user_screen_name)s, %(user_name)s, %(created_at)s, %(retweets)s, %(favorites)s, %(query)s)""", pop_tweets)
        self.conn.commit()
        cur.close()

        self.counter += len(replaced_tweets)
        if self.max_tweets is not None and self.counter >= self.max_tweets:
            return False

        return True

if __name__ == '__main__':
    search_query = '#elsagate'
    rate_delay_seconds = 0
    error_delay_seconds = 10
    max_tweets = 20000

    conn = psycopg2.connect('dbname=jarodmeng user=jarodmeng')
    twitDB = TwitterDB(rate_delay_seconds, error_delay_seconds, max_tweets, conn)
    twitDB.search(search_query)
    conn.close()
