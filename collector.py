#!/usr/bin/env python3
# coding: utf-8

from datetime import timedelta, timezone, tzinfo
from dateutil.parser import parse
from urllib.parse import parse_qs
import configparser
import csv
import logging
import pprint
import sys
import time
import twitter


CONFIG_FILE = 'collector.conf'
DEFAULT_QUERY = 'q=頭痛%%20OR%%20ずつう%%20OR%%20頭が痛い%%20OR%%20頭がいたい%%20OR%%20あたまが痛い%%20OR%%20頭いたい%%20OR%%20あたま痛い%%20OR%%20あたまいたい%%20&locale=ja&result_type=recent&count=100'
DEFAULT_DATA_FILE = 'data.csv'
DEFAULT_SLEEP_INTERVAL = 5


class Collector(object):

    def __init__(self,
            consumer_key=None,
            consumer_secret=None,
            access_token_key=None,
            access_token_secret=None,
            most_recent_id=None,
            base_query=None,
            data_file=None,
            sleep_interval=None):

        self.pp = pprint.PrettyPrinter(indent=2)
        self._api = twitter.Api(consumer_key=consumer_key,
                consumer_secret=consumer_secret,
                access_token_key=access_token_key,
                access_token_secret=access_token_secret)
        self._base_query = base_query
        self._data_file = data_file
        self._sleep_interval = sleep_interval
        self._endpoint_rate_limit = None
        self._remaining_count = 0
        self._latest_id = None
        self._since_id = most_recent_id

        # initial GetSearch and CheckRateLimit
        # this should get tweets since most_recent_id
        self._GetSearch()


    def _CheckRateLimit(self):
        while True:
            try:
                self._endpoint_rate_limit = self._api.rate_limit.get_limit('/search/tweets')
            except Exception:
                logging.debug(e)
                time.sleep(self._sleep_interval)
                continue
            self._remaining_count = int(self._endpoint_rate_limit.remaining)
            self.pp.pprint([time.strftime('%d/%m/%Y_%T'), self._endpoint_rate_limit])
            break


    def _GetSearch(self, max_id=None):
        while self._endpoint_rate_limit is None:
            self._CheckRateLimit()

        query = ''
        # 1. there is no history; get everything since as old as possible
        if self._since_id == 0:
            query = self._base_query
        # 2. get everything since a specific tweet
        elif max_id is None:
            query = self._base_query + '&since_id=%d' % (self._since_id)
        # 3. get tweets in a specific range
        else:
            query = self._base_query + '&since_id=%d&max_id=%d' % (self._since_id, max_id)

        while True:
            # we know we consued all the query counts; wait for remaining_count to be recovered
            if self._remaining_count == 0:
                while int(self._endpoint_rate_limit.reset) == 0:
                    logging.debug('retrieving the current rate limit...')
                    self._CheckRateLimit()
                    time.sleep(self._sleep_interval)
                if self._remaining_count != 0:
                    continue
                pause_duration = int(self._endpoint_rate_limit.reset) - int(time.time()) + 1
                if pause_duration > 0:
                    self.pp.pprint([time.strftime('%d/%m/%Y_%T'), 'wait %d seconds before making the request' % pause_duration])
                    time.sleep(pause_duration)

            # subtract 1 from remaining_count
            self._remaining_count -= 1
            try:
                self._current_result = self._api.GetSearch(
                            raw_query=query,
                            include_entities=False,
                            return_json=True)
            # coudn't get tweets: something is wrong
            except Exception as e:
                logging.warning(e)
                time.sleep(self._sleep_interval)
                continue
            logging.debug('query: ' + query)
            break

        # set latest_id if None and there are any tweets
        # if there's no tweets, keep last_id as None
        if self._latest_id is None and len(self._current_result['statuses']) != 0:
            try:
                self._latest_id = self._current_result['statuses'][0]['id']
                # something is really wrong
            except Exception as e:
                logging.warning(e)
                sys.exit(2)


    def _WriteToCSV(self, tweet):
        if tweet['text'].startswith('RT @'):
            return
        with open(self._data_file, 'a') as f:
            w = csv.writer(f)
            time = utc_to_jst(parse(tweet['time']))
            text = tweet['text'].replace('\n',' ')
            row = None
            if tweet['coord'] is not None and tweet['coord']['type'] == 'Point':
                row = [time,tweet['id'],
                    tweet['user'],
                    text,
                    tweet['geo_enabled'],
                    tweet['coord']['coordinates'][0],
                    tweet['coord']['coordinates'][1]]
            else:
                row = [time,
                    tweet['id'],
                    tweet['user'],
                    text,
                    tweet['geo_enabled'],
                    None,
                    None]
            w.writerow(row)


    def RunForever(self):
        # initialize new_max_id
        new_max_id = None
        while True:
            # there is no tweet in the current iteration; wait and retry
            if len(self._current_result['statuses']) == 0:
                self.pp.pprint([time.strftime('%d/%m/%Y_%T'), 'sleeping 12 hours before the next cycle :-)'])
                time.sleep(60*60*12)
                self._GetSearch(max_id)
                continue

            # from here on process the current iteration
            try:
                # this is the oldest tweet's id in the current iteration
                new_max_id = self._current_result['statuses'][-1]['id'] - 1
                # stdout the current new_max_id
                self.pp.pprint([time.strftime('%d/%m/%Y_%T'), {'new_max_id': new_max_id}])
            # something is really wrong
            except Exception as e:
                logging.warning(e)
                sys.exit(2)

            # save the current result
            for st in self._current_result['statuses']:
                tweet = {'time': st['created_at'], 'id': st['id_str'], 'user': st['user']['screen_name'], 'text': st['text'], 'coord': st['coordinates'], 'geo_enabled': st['user']['geo_enabled']}
                self._WriteToCSV(tweet)

            # check the number of tweets in this iteration
            number_of_tweets_in_this_cycle = len(self._current_result['statuses'])
            logging.info('%d tweets collected' % number_of_tweets_in_this_cycle)
            # less tweets than requested; reached the oldest
            if number_of_tweets_in_this_cycle < int(parse_qs(self._base_query)['count'][0]):
                # set since_id to the latest id we know
                self._since_id = self._latest_id
                # new_since_id should be cleared
                new_max_id = None
                # reset latest_id
                self._latest_id = None
                logging.info('reached the oldest tweets available')

            # proceed to the next cycle after 2 seconds
            self.pp.pprint([time.strftime('%d/%m/%Y_%T'), 'wait %d seconds before fetching the next result' % self._sleep_interval])

            time.sleep(self._sleep_interval)
            self._GetSearch(new_max_id)


class JST(tzinfo):
    def utcoffset(self, dt):
        return timedelta(hours=9)
    def tzname(self, dt):
        return "JST"
    def dst(self, dt):
        return timedelta(0)


def utc_to_jst(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=JST())


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config['Log'] = {}
    config['Twitter'] = {'most_recent_id': '0', 'base_query': DEFAULT_QUERY, 'data_file': DEFAULT_DATA_FILE, 'sleep_interval': DEFAULT_SLEEP_INTERVAL}
    config.read(CONFIG_FILE)

    if 'filename' in config['Log']:
        logging.basicConfig(filename=config['Log']['filename'], format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)
    else:
        logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

    try:
        collector = Collector(consumer_key=config['Twitter']['consumer_key'],
            consumer_secret=config['Twitter']['consumer_secret'],
            access_token_key=config['Twitter']['access_token_key'],
            access_token_secret=config['Twitter']['access_token_secret'],
            most_recent_id=int(config['Twitter']['most_recent_id']),
            base_query=config['Twitter']['base_query'],
            data_file=config['Twitter']['data_file'],
            sleep_interval=int(config['Twitter']['sleep_interval']))
    except KeyError as e:
        print("Error: %s parameter was not defined in %s" % (e, CONFIG_FILE))
        sys.exit(1)

    collector.RunForever()
