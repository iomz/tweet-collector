#!/usr/bin/env python3
# coding: utf-8

from datetime import timedelta, timezone, tzinfo
from dateutil.parser import parse
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
DEFAULT_INTERVAL = 3

class Collector(object):

    def __init__(self,
            consumer_key=None,
            consumer_secret=None,
            access_token_key=None,
            access_token_secret=None,
            most_recent_id=None,
            base_query=None,
            data_file=None):

        self.pp = pprint.PrettyPrinter(indent=2)
        self._api = twitter.Api(consumer_key=consumer_key,
                consumer_secret=consumer_secret,
                access_token_key=access_token_key,
                access_token_secret=access_token_secret)
        self._base_query = base_query
        self._data_file = data_file
        self._endpoint_rate_limit = None
        self._remaining_count = 0
        self._first_id = ''
        self._since_id = most_recent_id
        
        # initial GetSearch and CheckRateLimit
        self._GetSearch()
        self._first_id = self._current_result['statuses'][0]['id']

    def _CheckRateLimit(self):
        while True:
            try:
                self._endpoint_rate_limit = self._api.rate_limit.get_limit('/search/tweets')
            except Exception:
                logging.debug(e)
                time.sleep(DEFAULT_INTERVAL)
                continue
            self._remaining_count = int(self._endpoint_rate_limit.remaining)
            self.pp.pprint([time.strftime('%d/%m/%Y_%T'), self._endpoint_rate_limit])
            break

    def _GetSearch(self, max_id=None):
        while self._endpoint_rate_limit is None:
            self._CheckRateLimit()
       
        query = ''
        if self._since_id == 0:
            query = self._base_query
        elif max_id is None:
            query = self._base_query + '&since_id=%d' % (self._since_id)
        else:
            query = self._base_query + '&since_id=%d&max_id=%d' % (self._since_id, max_id)

        while True:
            if self._remaining_count == 0:
                while int(self._endpoint_rate_limit.reset) == 0:
                    logging.debug('retrieving the current rate limit...')
                    self._CheckRateLimit()
                    time.sleep(DEFAULT_INTERVAL)
                if self._remaining_count != 0:
                    continue
                pause_duration = int(self._endpoint_rate_limit.reset) - int(time.time()) + 1
                if pause_duration > 0:
                    logging.debug('wait %d seconds before making the request' % pause_duration)
                    time.sleep(pause_duration)
            self._remaining_count -= 1
            try:
                self._current_result = self._api.GetSearch(
                            raw_query=query,
                            include_entities=False,
                            return_json=True)
            except Exception as e:
                logging.debug(e)
                time.sleep(DEFAULT_INTERVAL)
                continue
            logging.info('query: ' + query)
            break

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
            logging.debug(row)

    def RunForever(self):
        while True:
            # save the current result
            for st in self._current_result['statuses']:
                tweet = {'time': st['created_at'], 'id': st['id_str'], 'user': st['user']['screen_name'], 'text': st['text'], 'coord': st['coordinates'], 'geo_enabled': st['user']['geo_enabled']}
                self._WriteToCSV(tweet)
            
            # check the next result
            try:
                max_id = self._current_result['statuses'][-1]['id'] - 1
            except Exception as e:
                logging.info(e)
                max_id = 0
                
            self.pp.pprint([time.strftime('%d/%m/%Y_%T'), {'max_id': max_id}])
            # if covered everything after the last cycle
            if max_id <= self._since_id:
                self._since_id = self._first_id
                logging.info('sleeping 12 hours before the next cycle :-)')
                time.sleep(60*60*12)

            # proceed to the next cycle after 2 seconds
            logging.debug('wait %d seconds before fetching the next result' % DEFAULT_INTERVAL)
            time.sleep(DEFAULT_INTERVAL)
            self._GetSearch(max_id)


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
    config['Twitter'] = {'most_recent_id': '0', 'base_query': DEFAULT_QUERY, 'data_file': DEFAULT_DATA_FILE}
    config.read(CONFIG_FILE)

    if 'filename' in config['Log']:
        logging.basicConfig(filename=config['Log']['filename'], format='%(levelname)s:%(message)s', level=logging.DEBUG)
    else:
        logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
    
    try:
        collector = Collector(consumer_key=config['Twitter']['consumer_key'],
            consumer_secret=config['Twitter']['consumer_secret'],
            access_token_key=config['Twitter']['access_token_key'],
            access_token_secret=config['Twitter']['access_token_secret'],
            most_recent_id=int(config['Twitter']['most_recent_id']),
            base_query=config['Twitter']['base_query'],
            data_file=config['Twitter']['data_file'])
    except KeyError as e:
        print("Error: %s parameter was not defined in %s" % (e, CONFIG_FILE))
        sys.exit(1)

    collector.RunForever()
