#!/usr/bin/env python3
# coding: utf-8

import configparser
import csv
import logging
import os
import sys
import time

from datetime import datetime, timedelta, timezone, tzinfo
from dateutil.parser import parse
from natsort import natsorted

import twitter

CONFIG_FILE = 'collector.conf'
DEFAULT_QUERY = 'q=hello%%20&locale=ja&result_type=recent&count=100'
DEFAULT_DATA_FILE = 'data.csv'
DEFAULT_DATA_DIR = 'data'
DEFAULT_SLEEP_INTERVAL = 5

# pylint: disable=R0902
class Collector():

    # pylint: disable=R0913
    def __init__(self,
            consumer_key=None,
            consumer_secret=None,
            access_token_key=None,
            access_token_secret=None,
            most_recent_id=None,
            base_query=None,
            data_file=None,
            sleep_interval=None,
            current_month=None,
            data_rotate=None):

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
        self._current_month = current_month
        self._rotate_pending = False
        self._current_result = None
        self._data_rotate = data_rotate

        # stdout the config
        logging.info('hatc is initialized')
        logging.info('base_query: %s', self._base_query)

        # initial GetSearch and CheckRateLimit
        # this should get tweets since most_recent_id
        self._get_search()


    def _get_search(self):
        while self._endpoint_rate_limit is None:
            self.check_rate_limit()

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
            # we know we consumed all the query counts; wait for remaining_count to be recovered
            if self._remaining_count == 0:
                while int(self._endpoint_rate_limit.reset) == 0:
                    logging.debug('retrieving the current rate limit...')
                    self.check_rate_limit()
                    time.sleep(self._sleep_interval)
                if self._remaining_count != 0:
                    continue
                pause_duration = int(self._endpoint_rate_limit.reset) - int(time.time()) + 1
                if pause_duration > 0:
                    logging.debug('wait %d seconds before making the request', pause_duration)
                    time.sleep(pause_duration)

            # subtract 1 from remaining_count
            self._remaining_count -= 1
            try:
                self._current_result = self._api.GetSearch(
                            raw_query=query,
                            include_entities=False,
                            return_json=True)
            # coudn't get tweets: something is wrong
            # pylint: disable=W0703
            except Exception as exception:
                logging.warning(exception)
                time.sleep(self._sleep_interval)
                continue
            logging.debug('query: %s', query)
            break

        # set latest_id if None and there are any tweets
        # if there's no tweets, keep last_id as None
        if self._latest_id is None and len(self._current_result['statuses']) != 0:
            try:
                self._latest_id = self._current_result['statuses'][0]['id']
                # something is really wrong
            # pylint: disable=W0703
            except Exception as exception:
                logging.warning(exception)
                sys.exit(2)


    def _write_to_csv(self, tweet):
        if tweet['text'].startswith('RT @'):
            return
        with open(self._data_file, 'a') as data_file:
            csv_writer = csv.writer(data_file)
            posted_time = utc_to_jst(parse(tweet['time']))
            text = tweet['text'].replace('\n',' ')
            row = None
            if tweet['coord'] is not None and tweet['coord']['type'] == 'Point':
                row = [
                    posted_time,
                    tweet['id'],
                    tweet['user'],
                    text,
                    tweet['geo_enabled'],
                    tweet['coord']['coordinates'][0],
                    tweet['coord']['coordinates'][1]]
            else:
                row = [
                    posted_time,
                    tweet['id'],
                    tweet['user'],
                    text,
                    tweet['geo_enabled'],
                    None,
                    None]
            csv_writer.writerow(row)


    def _rotate_csv(self):
        '''
        Do the following:
        - dump the data from last month to the file
        - backup the current data_file
        - extract the new month data and make it as the new data_file
        - update self._current_month
        '''
        # Prepare the target dir and the filename for rotation
        logging.info('preparing the data rotation dir')
        if not os.path.exists(DEFAULT_DATA_DIR):
            os.makedirs(DEFAULT_DATA_DIR)
        last_month_data_file = os.path.join(DEFAULT_DATA_DIR, self._current_month + ".csv")

        # Read the data
        logging.info('reading the data file')
        last_month_data = set()
        new_data = []
        next_month = get_current_month()
        with open(self._data_file, 'r') as data_file:
            for line in data_file.readlines():
                if line.startswith(self._current_month):
                    last_month_data.add(line)
                elif line.startswith(next_month):
                    new_data.append(line)

        # Dump the data from last month to the file
        logging.info('cleaning up the data from the last month...')
        total = len(last_month_data)
        count = 0
        percents = [(x,int(total/100*x)) for x in range(10,100,10)]

        ## Check if the file alredy exists
        if os.path.exists(last_month_data_file):
            # Backup the file
            os.rename(last_month_data_file, last_month_data_file + ".bak")

        with open(last_month_data_file, 'a') as lmdf:
            for line in natsorted(last_month_data):
                count += 1
                lmdf.write(line)
                if count in percents:
                    logging.debug('%d %% saved', percents[count])

        # Backup the old data file
        logging.info('performing the data backup')
        os.rename(self._data_file, self._data_file + ".bak")

        # Write out the data for the current_month
        logging.info('updating the data')
        with open(self._data_file, 'a') as data_file:
            for line in new_data:
                data_file.write(line)

        # Update the current_month
        logging.info('rotated the data file %s -> %s', self._current_month, next_month)
        logging.info('updating the current_month')
        self._current_month = next_month


    def check_rate_limit(self):
        while True:
            try:
                self._endpoint_rate_limit = self._api.rate_limit.get_limit('/search/tweets')
            # pylint: disable=W0703
            except Exception as exception:
                logging.debug(exception)
                time.sleep(self._sleep_interval)
                continue
            self._remaining_count = int(self._endpoint_rate_limit.remaining)
            logging.debug(self._endpoint_rate_limit)
            break


    def run_forever(self):
        while True:
            # there are no tweets in the current iteration; sleep or rotate
            if len(self._current_result['statuses']) == 0:
                # up-to-date; sleep
                if self._latest_id is None:
                    # check the ratation before sleeping
                    if self._rotate_pending:
                        if not self._data_rotate:
                            self._rotate_pending = False
                        else:
                            logging.info('month changed; rotating the data')
                            self._rotate_csv()
                            self._rotate_pending = False

                    # sleep for 12 hours
                    logging.info('sleeping 12 hours before the next cycle :-)')
                    time.sleep(60*60*12)

                    # check the current month
                    if get_current_month() != self._current_month:
                        # if the month changed, the next cycle should cover
                        # the end of the previous month
                        self._rotate_pending = True

                    self._get_search()
                    continue

                logging.info('reached the oldest tweets available; rotating the target range')
                # set since_id to the latest id we know
                self._since_id = self._latest_id
                logging.info('new since_id: %d', self._since_id)
                # max_id should be cleared
                self._max_id = None
                # latest_id should be cleared
                self._latest_id = None
                # proceed to the next cycle
                logging.debug(
                        'wait %d seconds before fetching the next result',
                        self._sleep_interval)
                time.sleep(self._sleep_interval)
                self._get_search()
                continue

            # from here on process the current iteration
            try:
                # this is the oldest tweet's id in the current iteration
                self._max_id = self._current_result['statuses'][-1]['id'] - 1
                # stdout the current max_id
                logging.debug('new max_id: %d', self._max_id)
            # something is really wrong
            # pylint: disable=W0703
            except Exception as exception:
                logging.warning(exception)
                sys.exit(2)

            # save the current result
            for status in self._current_result['statuses']:
                tweet = {
                        'time': status['created_at'],
                        'id': status['id_str'],
                        'user': status['user']['screen_name'],
                        'text': status['text'],
                        'coord': status['coordinates'],
                        'geo_enabled': status['user']['geo_enabled']}
                self._write_to_csv(tweet)
                ## check the current date from the tweets
                ## this doesn't mean we can alreayd rotate the data...
                #if st['created_at'][:7] != self._current_month:
                #    self._rotate_pending = True

            # check the number of tweets in this iteration
            number_of_tweets_in_this_cycle = len(self._current_result['statuses'])
            logging.debug('%d tweets collected', number_of_tweets_in_this_cycle)

            # proceed to the next cycle
            logging.debug('wait %d seconds before fetching the next result', self._sleep_interval)
            time.sleep(self._sleep_interval)
            self._get_search()


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
    config['Twitter'] = {
            'most_recent_id': '0',
            'base_query': DEFAULT_QUERY,
            'data_file': DEFAULT_DATA_FILE,
            'sleep_interval': DEFAULT_SLEEP_INTERVAL,
            'current_month': get_current_month(),
            'data_rotate': True}
    config.read(CONFIG_FILE)

    if 'filename' in config['Log']:
        logging.basicConfig(
                filename=config['Log']['filename'],
                format='%(asctime)s %(levelname)s:%(message)s',
                level=logging.INFO)
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
            sleep_interval=int(config['Twitter']['sleep_interval']),
            current_month=config['Twitter']['current_month'],
            data_rotate=bool(config['Twitter']['data_rotate']))
    except KeyError as key_error:
        print("Error: {} parameter was not defined in {}".format(key_error, CONFIG_FILE))
        sys.exit(1)

    collector.run_forever()
