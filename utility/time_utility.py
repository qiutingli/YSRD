# coding = utf-8

import datetime
import calendar
import pandas as pd
import time


def first_day_of_a_month(time):
    first_day_str = '{}-{}-01 00:00:00'.format(time.year, time.month)
    first_day = time_str_to_datetime(first_day_str)
    return first_day_str, first_day


def last_day_of_a_month(time):
    last_date = calendar.monthrange(time.year, time.month)[1]
    last_day_str = '{}-{}-{} 23:59:59'.format(time.year, time.month, last_date)
    last_day = time_str_to_datetime(last_day_str)
    return last_day_str, last_day


def time_str_to_datetime(time):
    """
    给定一个'%Y-%m-%d %H:%M:%S'格式的时间字符串，转换为datetime类型
    """
    datetime_time = datetime.datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
    return datetime_time


def if_same_month(time1, time2):
    """
    判断两个datetime对象是否在同一个月，目前代码只处理跨一个月的情况，跨多个月不考虑
    """
    if abs(time1.month - time2.month) > 1:
        print('本代码不处理跨两个以上月份的情况')

    return time1.month == time2.month


def time_str_differences(start_time_str, end_time_str):
    end_time = datetime.datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S')
    start_time = datetime.datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')

    return end_time.timestamp() - start_time.timestamp()


def gen_full_time_stamp(start_time_str, end_time_str, interval_unit, interval):
    """
    给定开始时间和结束时间，类型timestamp，以及时间间隔，产生按照interval为步长的data frame

    Args:
        start_time: str, 开始时间
        end_time: str, 结束时间
        interval_unit: 步长单位，str, 处理hour, minute, second
        interval: 步长
    """
    start_time = time_str_to_datetime(start_time_str)
    end_time = time_str_to_datetime(end_time_str)

    time_df = pd.DataFrame(columns=['FULL_TIME_DATETIME', 'FULL_TIME_STR'])

    if interval_unit == 'hour':
        time_delta = datetime.timedelta(hours=interval)
    elif interval_unit == 'minute':
        time_delta = datetime.timedelta(minutes=interval)
    elif interval_unit == 'second':
        time_delta = datetime.timedelta(seconds=interval)

    time_df = time_df.append({'FULL_TIME_DATETIME': start_time}, ignore_index=True)

    tmp_time = start_time

    while tmp_time + time_delta <= end_time:
        tmp_time = tmp_time + time_delta
        time_df = time_df.append({'FULL_TIME_DATETIME':tmp_time}, ignore_index=True)

    time_df['FULL_TIME_STR'] = time_df.apply(lambda x: datetime.datetime.strftime(x.FULL_TIME_DATETIME, '%Y-%m-%d %H:%M:%S'), axis=1)

    return time_df
    

def datetime_to_string(date_time):
    """
        datetime 转换成字符串

        """
    return date_time.strftime("%Y-%m-%d %H:%M:%S")


def string_to_timestamp(time_string):
    """
   字符串转换成时间戳

   """
    return time.mktime(time.strptime(time_string, "%Y-%m-%d %H:%M:%S"))


def timestamp_to_string(timestamp):
    """
        时间戳转换成字符串

        """
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))


def datetime_to_timestamp(date_time):
    """
           datetime 转换成时间戳

    """
    return time.mktime(date_time.timetuple())


def datetime_n_days_before_string(date_time, n):
    """
    给定一个datatime 和 n ,返回
    """
    return (date_time - datetime.timedelta(days=n)).strftime('%Y-%m-%d %H:%M:%S')


def datetime_n_days_after_string(date_time, n):
    """
    给定一个datatime 和 n ,返回
    """
    return (date_time + datetime.timedelta(days=n)).strftime('%Y-%m-%d %H:%M:%S')

def datetime_to_int_hour(date_time):
    '''
    给定一个date_time，调节到整点，并且小时数加1。例如'2018-09-01 12:35:06'调节为'2018-09-01 13:00:00'
    '''
    return date_time - datetime.timedelta(hours=-1,minutes=date_time.minute, seconds=date_time.second)

def str_datetime_to_int_hour(date_time):
    '''
    给定一个时间字符串，调节到整点的datetime类型。例如'2018-09-01 12:35:06'调节为'2018-09-01 12:35:06'
    '''
    date_time = time_str_to_datetime(date_time)
    return datetime_to_int_hour(date_time)

def get_seconds_given_time_str(start_time_str, end_time_str):
    return string_to_timestamp(end_time_str) - string_to_timestamp(start_time_str)
