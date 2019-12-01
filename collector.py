#!/usr/bin/env python3
# coding: utf-8

from datetime import datetime, timedelta, timezone, tzinfo
from dateutil.parser import parse
from natsort import natsorted
from urllib.parse import parse_qs
import configparser
import csv
import logging
import pprint
import os
import sys
import time
import twitter


CONFIG_FILE = 'collector.conf'
DEFAULT_QUERY = 'q=hello%%20&locale=ja&result_type=recent&count=100'
DEFAULT_DATA_FILE = 'data.csv'
DEFAULT_DATA_DIR = 'data'
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
        self._max_id = None
        self._since_id = most_recent_id
        self._current_month = get_current_month()

        # stdout the config
        self.pp.pprint([time.strftime('%d/%m/%Y_%T'), 'hatc is initialized'])
        self.pp.pprint([time.strftime('%d/%m/%Y_%T'), {'base_query': self._base_query}])

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


    def _GetSearch(self):
        while self._endpoint_rate_limit is None:
            self._CheckRateLimit()

        query = ''
        # 1. there is no history; get everything since as old as possible
        if self._since_id == 0:
            query = self._base_query
        # 2. get everything since a specific tweet
        elif self._max_id is None:
            query = self._base_query + '&since_id={}'.format(self._since_id)
        # 3. get tweets in a specific range
        else:
            query = self._base_query + '&since_id={}&max_id={}'.format(self._since_id, self._max_id)

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
                    logging.debug('wait {} seconds before making the request'.format(pause_duration))
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


    def _RotateCSV(self):
        '''
        Do the following:
        - dump the data from last month to the file
        - backup the current data_file
        - extract the new month data and make it as the new data_file
        - update self._current_month
        '''
        # Prepare the target dir and the filename for rotation
        self.pp.pprint([time.strftime('%d/%m/%Y_%T'), 'preparing the data rotation dir'])
        if not os.path.exists(DEFAULT_DATA_DIR):
            os.makedirs(DEFAULT_DATA_DIR)
        last_month_data_file = os.path.join(DEFAULT_DATA_DIR, self._current_month + ".csv")

        # Read the data
        self.pp.pprint([time.strftime('%d/%m/%Y_%T'), 'reading the data file'])
        last_month_data = set()
        new_data = []
        next_month = get_current_month()
        with open(self._data_file, 'r') as f:
            for l in f.readlines():
                if l.startswith(self._current_month):
                    last_month_data.add(l)
                elif l.startswith(next_month):
                    new_data.append(l)

        # Dump the data from last month to the file
        self.pp.pprint([time.strftime('%d/%m/%Y_%T'), 'cleaning up the data from the last month...'])
        total = len(last_month_data)
        count = 0
        percents = [(x,int(total/100*x)) for x in range(10,100,10)]
        with open(last_month_data_file, 'a') as f:
            for l in natsorted(last_month_data):
                count += 1
                f.write(l)
                if count in percents:
                    self.pp.pprint([time.strftime('%d/%m/%Y_%T'), '{} % saved'.format(percents[count])])

        # Backup the old data file
        self.pp.pprint([time.strftime('%d/%m/%Y_%T'), 'performing the data backup'])
        os.rename(self._data_file, self._data_file + ".bak")

        # Write out the data for the current_month
        self.pp.pprint([time.strftime('%d/%m/%Y_%T'), 'updating the data'])
        with open(self._data_file, 'a') as f:
            for l in new_data:
                f.write(l)

        # Update the current_month
        self.pp.pprint([time.strftime('%d/%m/%Y_%T'), 'rotated the data file {} -> {}'.format(self._current_month, next_month)])
        logging.debug('rotated the data file {} -> {}'.format(self._current_month, next_month))
        self.pp.pprint([time.strftime('%d/%m/%Y_%T'), 'updating the current_month'])
        self._current_month = next_month


    def RunForever(self):
        while True:
            # there are no tweets in the current iteration; sleep or rotate
            if len(self._current_result['statuses']) == 0:
                # up-to-date; sleep
                if self._latest_id is None:
                    if get_current_month() != self._current_month:
                        self.pp.pprint([time.strftime('%d/%m/%Y_%T'), 'month changed; rotating the data'])
                        self._RotateCSV()

                    self.pp.pprint([time.strftime('%d/%m/%Y_%T'), 'sleeping 12 hours before the next cycle :-)'])
                    logging.debug('sleeping 12 hours before the next cycle :-)')
                    time.sleep(60*60*12)
                    self._GetSearch()
                    continue

                self.pp.pprint([time.strftime('%d/%m/%Y_%T'), 'reached the oldest tweets available; rotating the target range'])
                # set since_id to the latest id we know
                self._since_id = self._latest_id
                logging.info('new since_id: {}'.format(self._since_id))
                # max_id should be cleared
                self._max_id = None
                # latest_id should be cleared
                self._latest_id = None
                # proceed to the next cycle
                logging.debug('wait {} seconds before fetching the next result'.format(self._sleep_interval))
                time.sleep(self._sleep_interval)
                self._GetSearch()
                continue

            # from here on process the current iteration
            try:
                # this is the oldest tweet's id in the current iteration
                self._max_id = self._current_result['statuses'][-1]['id'] - 1
                # stdout the current max_id
                self.pp.pprint([time.strftime('%d/%m/%Y_%T'), {'new max_id': self._max_id}])
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
            logging.info('{} tweets collected'.format(number_of_tweets_in_this_cycle))
            self.pp.pprint([time.strftime('%d/%m/%Y_%T'), '{} tweets collected'.format(number_of_tweets_in_this_cycle)])

            # proceed to the next cycle
            logging.debug('wait {} seconds before fetching the next result'.format(self._sleep_interval))
            time.sleep(self._sleep_interval)
            self._GetSearch()


class JST(tzinfo):
    def utcoffset(self, dt):
        return timedelta(hours=9)
    def tzname(self, dt):
        return "JST"
    def dst(self, dt):
        return timedelta(0)

def get_current_month():
    return datetime.now(tz=JST()).strftime('%Y-%m')

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
        print("Error: {} parameter was not defined in {}".format(e, CONFIG_FILE))
        sys.exit(1)

    collector.RunForever()
