# encoding: utf-8
"""
redis_counter.py

Created by Matt Nunogawa on 2010-03-25.
Copyright (c) 2010 Matt Nunogawa. All rights reserved.

This class take a 'counter_key'

We design this for frequent writes and infrequent reads

in the DB it uses the following fields:
'RedisCounter:counter_key' (string type, total number of counts)

#recent 
(list type, value is unix timestamp of the count in question)
'RedisCounter:counter_key:last_five_seconds' # only used for testing
'RedisCounter:counter_key:last_hour'
'RedisCounter:counter_key:last_day'
'RedisCounter:counter_key:last_week'
'RedisCounter:counter_key:last_month' # really, this is just the last 30 days

#historical
'RedisCounter:counter_key:historical_hour:<YYYYMMDDHH>'
'RedisCounter:counter_key:historical_hours_list'
'RedisCounter:counter_key:historical_day:<YYYYMMDD>' (string type, the count for that day)
'RedisCounter:counter_key:historical_days_list' (list type, list of all YYYYMMDD stamps that have a day count)
'RedisCounter:counter_key:historical_week:<YYYYMMDD>'
'RedisCounter:counter_key:historical_weeks_list'
'RedisCounter:counter_key:historical_month:<YYYYMM>'
'RedisCounter:counter_key:historical_months_list'

"""
__author__ = "Matt Nunogawa"
__version__ = "0.0.1"
__description__ = "Utility class to measure counts over different spans of time."

KEY_PREFIX = 'RedisCounter:'

RECENT_COUNTS_SUFFIXES = ['last_five_seconds', 'last_hour', 'last_day', 'last_week', 'last_month']
RECENT_COUNTS_TIME_INTERVALS = [5, 60*60, 60*60*24, 60*60*24*7, 60*60*24*30]

PERIODIC_COUNTS_SUFFIXES = ['historical_hour','historical_day', 'historical_week', 'historical_month'];
PERIODIC_COUNTS_LIST_SUFFIXES = ['historical_hours_list','historical_days_list', 'historical_weeks_list', 'historical_months_list'];
PERIODIC_COUNTS_TIMESTAMP_FORMATS = ['%Y%m%d%H',"%Y%m%d", "%Y%m%d", "%Y%m"];

import redis
import time


class RedisCounter:
    def __init__(self, partial_counter_key, redis_instance):
        # since we override getattr and setattr to hit the redis store, 
        # we use the internal __dict__ to save ivars
        self.counter_key = KEY_PREFIX + partial_counter_key
        self.redistore = redis_instance
        self._initCounterIfNecessary(self.counter_key)            
    
    def _initCounterIfNecessary(self, key):
        ""
        does_exist = self.redistore.exists(key)
        if does_exist == False:
            # it doesn't exist, create it
            self.redistore.set(key, '0')
    
    def current_count(self):
        return self.redistore.get(self.counter_key)

    def increment_counter(self):
        now = time.time()
        # increment recent counts, (in the last hour, day, week, month)
        for key in self._recent_counts_keys():
            self.redistore.rpush(key, now)
        # increment the periodic counts (hourly, daily, weekly, monthly)
        for i in range(len(PERIODIC_COUNTS_SUFFIXES)):
            key_prefix = self._periodic_counts_key_prefixes()[i]
            periodic_key = key_prefix + self._timestamp_for_format(time.time(), PERIODIC_COUNTS_TIMESTAMP_FORMATS[i])
            self._initCounterIfNecessary(periodic_key)
            self.redistore.incr(periodic_key)
            #if necessary, add this periodic key to the list of periodic keys
            periodic_list_key = self._periodic_list_keys()[i]
            if self.redistore.lindex(periodic_list_key, -1) != periodic_key:
                self.redistore.rpush(periodic_list_key, periodic_key)
        
        # finally, imcrement to total count and return it
        return self.redistore.incr(self.counter_key)

    def reset_counter(self):
        self.delete_counter()
        self.redistore.set(self.counter_key, '0')
            
    def delete_counter(self):
        self.redistore.delete(self.counter_key)
        for key in self._recent_counts_keys():
            self.redistore.delete(key)
        for list_key in self._periodic_list_keys():
            key = self.redistore.lpop(list_key)
            while key:
                self.redistore.delete(key)
                key = self.redistore.lpop(list_key)
                

    ####################################################
    #
    # Recent Count functions

    def _recent_counts_keys(self):
        return [self.counter_key+':'+suffix for suffix in RECENT_COUNTS_SUFFIXES]        
        
    def _counts_in_recent_count_index(self, recent_count_index):
        """calcs one of the counts in RECENT_COUNTS_SUFFIXES
        O(n) where n is the number of counts since the last time we called this beast
        Since reads are infrequent, this should be okay
        """
        
        time_interval = RECENT_COUNTS_TIME_INTERVALS[recent_count_index]
        list_key = self._recent_counts_keys()[recent_count_index]
        some_time_ago = time.time() - time_interval
        
        # we do an O(n) operation here.  This is probably okay as reads should be infrequent
        head_time = self.redistore.lpop(list_key)

        try:
            float(head_time)
        except:
            return self.redistore.llen(list_key)
        
        while float(head_time) < some_time_ago:
            head_time = self.redistore.lpop(list_key)
            try:
                float(head_time)
            except:
                return self.redistore.llen(list_key)
        
        self.redistore.lpush(list_key, head_time)
        
        return self.redistore.llen(list_key)

    def counts_in_last_five_seconds(self):
        return self._counts_in_recent_count_index(0)
    def counts_in_last_hour(self):
        return self._counts_in_recent_count_index(1)
    def counts_in_last_day(self):
        return self._counts_in_recent_count_index(2)
    def counts_in_last_week(self):
        return self._counts_in_recent_count_index(3)
    def counts_in_last_month(self):
        return self._counts_in_recent_count_index(4)

    ####################################################
    #
    # Periodic Count functions
    def _periodic_counts_key_prefixes(self):
        return [self.counter_key+':'+suffix+':' for suffix in PERIODIC_COUNTS_SUFFIXES]        
    def _periodic_list_keys(self):
        return [self.counter_key+':'+suffix for suffix in PERIODIC_COUNTS_LIST_SUFFIXES]        

    def _timestamp_for_format(self, seconds_since_epoch, format):
        return time.strftime(format, time.localtime(seconds_since_epoch))
        
    def _periodic_counts_for_timestamp(self, seconds_since_epoch):
        list_to_return = []
        for i in range(len(PERIODIC_COUNTS_SUFFIXES)):
            key_prefix = self._periodic_counts_key_prefixes()[i]
            periodic_key = key_prefix + self._timestamp_for_format(seconds_since_epoch, PERIODIC_COUNTS_TIMESTAMP_FORMATS[i])
            value = self.redistore.get(periodic_key)
            if value == None:
                list_to_return.append('0')
            else:
                list_to_return.append(value)
        return list_to_return
            
    def counts_for_hour(self, seconds_since_epoch):
        return self._periodic_counts_for_timestamp(seconds_since_epoch)[0]
    def counts_for_day(self, seconds_since_epoch):
        return self._periodic_counts_for_timestamp(seconds_since_epoch)[1]
    def counts_for_week(self, seconds_since_epoch):
        return self._periodic_counts_for_timestamp(seconds_since_epoch)[2]
    def counts_for_month(self, seconds_since_epoch):
        return self._periodic_counts_for_timestamp(seconds_since_epoch)[3]

        
