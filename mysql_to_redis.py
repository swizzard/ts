# coding=utf8
__author__ = 'Sam Raker'

from os import getenv

import MySQLdb
from MySQLdb import cursors
from MySQLdb import MySQLError
import redis


class DBToRedis(object):
    def __init__(self):
        self.r = redis.StrictRedis(host='192.168.1.3')

        self.r.set("user_alias_counter", 0)
        self.r.set("tweet_alias_counter", 0)
        self.r.set("domain_counter", 0)
        self.curr_table = None
        self.curr_id = 0
        self.tables = ("hashtag", "hashtag_to_tweet", "tag_word",
                       "tag_word_to_hashtag", "tweet", "url",
                       "url_to_tweet", "user", "user_to_tweet",
                       "word", "word_to_tweet")

    @staticmethod
    def get_conn():
        conn = None
        while conn is None:
            try:
                conn = MySQLdb.connect(cursorclass=cursors.SSDictCursor,
                                       host='24.186.113.22', port=3306,
                                       user='samuelraker', db='twitter',
                                       passwd=getenv('TWEETS_DB_PASSWORD'))
            except MySQLError as e:
                print e
        else:
            return conn

    def row_gen(self, table, _id):
        self.curr_table = table
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT * FROM {} WHERE id >= %s".format(table), (_id,))
        rows = cur.fetchall()
        num_rows = len(rows)
        i = 0
        while True:
            if i < num_rows:
                row = rows[i]
                self.curr_id = row.get('id')
                yield rows[i]
                i += 1
            else:
                self.curr_id = 0
                raise StopIteration

    @staticmethod
    def gen_key(prefix, _id):
        return "{}:{}".format(prefix, _id)

    def insert(self, prefix, _id, val):
        self.r.set(self.gen_key(prefix, _id), val)

    def set_insert(self, ref, cross, _id, val):
        """
        DBToRedis().set_insert("tweet", "users", 123, 456)
        """
        key = self.gen_key("{}_{}".format(ref, cross), _id)
        self.r.sadd(key, val)

    def get_alias(self, prefix):
        return self.r.incr("{}_alias_counter".format(prefix))

    def handle_domain(self, domain):
        if self.r.sadd("all_domains", domain):
            self.insert("domain", self.r.incr("domain_counter"), domain)

    def parse_hashtag(self, hashtag_row):
        if hashtag_row.get('english') == 1:
            self.insert("hashtag", hashtag_row.get("id"), hashtag_row.get("str"))

    def parse_hashtag_to_tweet(self, htt_row):
        self.set_insert("hashtag", "tweets", htt_row.get('hashtag_id'), htt_row.get('tweet_id'))

    def parse_tag_word(self, tag_word_row):
        if tag_word_row.get('english') == 1:
            self.insert("tag_word", tag_word_row.get("id"), tag_word_row.get("str"))

    def parse_tag_word_to_hashtag(self, httw_row):
        self.set_insert("hashtag", "words", httw_row.get("hashtag_id"), httw_row.get("tag_word_id"))

    def parse_tweet(self, tweet_row):
        self.r.hmset(self.gen_key("tweet", tweet_row.pop("id")), dict(alias=self.get_alias("tweet"), **tweet_row))

    def parse_url(self, url_row):
        _id = url_row.get("id")
        domain = url_row.get("domain")
        _str = url_row.get("str")
        self.handle_domain(domain)
        self.insert("all_urls", _id, _str)

    def parse_url_to_tweet(self, urltt_row):
        self.set_insert("tweet", "urls", urltt_row.get("tweet_id"), urltt_row.get("url_id"))

    def parse_user(self, user_row):
        self.r.hmset(self.gen_key("user", user_row.pop("id")), dict(alias=self.get_alias("user"), **user_row))

    def parse_user_to_tweet(self, utt_row):
        user_id = utt_row.get("user_id")
        tweet_id = utt_row.get("tweet_id")
        cross = "mention" if utt_row.get("is_mention") else "author"
        self.set_insert("user", cross, tweet_id, user_id)

    def parse_word(self, word_row):
        if word_row.get('english') == 1:
            self.insert("word", word_row.get("id"), word_row.get("str"))

    def parse_word_to_tweet(self, wtt_row):
        self.set_insert("tweet", "words", wtt_row.get('tweet_id'), wtt_row.get('word_id'))

    @staticmethod
    def print_parse(f):
        def dec(*args, **kwargs):
            print args[0]
            return f(*args, **kwargs)

        return dec

    def parse_all(self, table):
        parser_func = self.print_parse(self.__getattribute__("parse_{}".format(table)))
        for row in self.row_gen(table, self.curr_id):
            parser_func(row)

