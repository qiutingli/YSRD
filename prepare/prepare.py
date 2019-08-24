#coding=utf-8

import datetime as dt
import pandas as pd
import os
import math

import common
import config.json_parser as json_parser
from dao import read_data
import utility.time_utility as tu
from utility.file_utility import FileUtility


class Prepare():
    def __init__(self):
        # 初始化static_parser和dynamic parser
        dynamic_path = "%s/input/qe_config_dynamic.JSON" % os.path.abspath(os.pardir)
        static_path = "%s/input/qe_config_static.JSON" % os.path.abspath(os.pardir)

        self.dynamic_parser = json_parser.ReadJson(dynamic_path)
        self.static_parser = json_parser.ReadJson(static_path)

        # 初始化老化的起始时间和结束时间
        self.start_time = self.dynamic_parser.get_first_layer("START_TIME")
        self.end_time = self.dynamic_parser.get_first_layer("END_TIME")

        # 初始化聚合数据的time interval，单位秒
        # 我们会用每个CAP_TIME除以time_interval，获得一个time_index，将连续的时间离散化
        self.time_interval = self.static_parser.get_first_layer("AGGREGATION_INTERVAL")

        # 有时候我们不关心过高量程上的值是否准确，例如PM25>500，或者PM10>600
        self.max_exp_val = self.static_parser.get_first_layer("MAX_EXP_VAL")

        # file_utility对象负责新建输出目录，以及获得数据、图片、输出文件保存的地方
        self.file_utility = FileUtility()

        # 由于我们对每一台需要质保的设备分开处理，这里记录当前处理的参数是什么，以及当前处理的是否为该参数下的第一台设备，主要是为了输出数据而建立的flag
        self.first_qe_device = True
        self.current_var = ''

    def drop_duplicate_rows(self, data):
        '''
        将数据按时间去重
        :param data: 需去重数据
        :return: 
            去重后的数据
        '''
        result = data.drop_duplicates(['CAP_TIME', 'DEV_ID'])
        return result

    def get_index_for_time_align(self, data):
        '''
        依据时间对齐起始时间和时间间隔 获取数据的时间索引
        :param data: 需添加时间索引的数据
        :param time_interval: 时间间隔(单位：秒) 在此间隔内索引相同
        :return: 时间索引
        '''
        d0 = dt.datetime.strptime(self.start_time, '%Y-%m-%d %H:%M:%S')
        data['TIME_INDEX'] = data.apply(lambda x: self.cal_time_index(x.CAP_TIME), axis=1)

        return data

    def cal_time_index(self, cap_time_str):
        total_seconds = tu.get_seconds_given_time_str(self.start_time, cap_time_str)
        return round(total_seconds/self.time_interval)

    def aggregate_data(self, time_indexed_data, column_names):
        '''
        将数据的行按相同时间索引进行聚合
        :param time_indexed_data: 已添加时间索引的数据
        :return: 按相同时间索引进行聚合后的数据
        '''
        result = time_indexed_data[column_names].groupby([time_indexed_data['DEV_ID'], time_indexed_data['CHANNEL'], time_indexed_data['TIME_INDEX']], observed=False).mean()

        result.reset_index(inplace=True)
        return result

    def drop_abnormal(self, data, var):
        '''
        对给定的通道，查出其对应的参数的error code并做排除
        '''
        error_code_list = self.static_parser.get_second_layer("ERROR_CODE", var)
        num_bf = data.shape[0]
        data = data[~data[var].isin(error_code_list)]
        num_af = data.shape[0]
        return data, num_bf - num_af

    def drop_hop(self, data, var, col_name):
        num_bf = data.shape[0]

        thres = self.static_parser.get_third_layer("PARA", var, "hop_threshold")
        data_upload_interval = self.dynamic_parser.get_first_layer("QE_TIME_INTERVAL")

        data.sort_values(by=['CAP_TIME'], inplace=True)
        data['HOP_DELTA'] = data[col_name].diff(periods=1)
        data['PREV_CAP_TIME'] = data['CAP_TIME'].shift(periods=1)

        data.dropna(inplace=True)

        if data.empty:
            return data, num_bf

        data['CAP_TIME_DELTA'] = data.apply(lambda x: tu.time_str_differences(x.PREV_CAP_TIME, x.CAP_TIME), axis=1)

        data1 = data[(abs(data['HOP_DELTA']) < thres) & (data['CAP_TIME_DELTA'] < 2*data_upload_interval)].copy()
        data2 = data[data['CAP_TIME_DELTA'] > 2*data_upload_interval].copy()

        data = pd.concat([data1, data2], axis=0, sort=False)

        num_af = data.shape[0]
        return data, num_bf - num_af

    def get_data_interpolated(self, data, interpolation_channels):
        '''
        Args:
            data: 需要被插值的数据
            interpolation_channels: 需要被插值的通道名称
        '''
        for channel in interpolation_channels:
            data[channel].interpolate(inplace=True)
        return data

    def gen_full_index(self):
        '''
        产生聚合所需要的index最完备的时间序列，这个在处理基准设备组的时候非常重要，因为基准设备组需要对其测量值取均值，但是由于不同的基准设备上传数据的时间不同，有可能在某个时间点上直接取均值只能handle一个设备的subset，因此我们需要先插值，方便后面取均值。
        本函数则是为了插值先准备需要被插值的完整的时间点TIME_INDEX列
        '''
        # 获得老化时间长度，单位秒
        total_seconds = tu.get_seconds_given_time_str(self.start_time, self.end_time)

        # 获得产生TIME_INDEX的time_interval
        time_interval = self.static_parser.get_first_layer("AGGREGATION_INTERVAL")
        # 获得最大的index
        max_index = math.ceil(total_seconds / time_interval)

        # 从1到最大的index
        full_index = pd.DataFrame([i for i in range(1, max_index)], columns=['TIME_INDEX'])
        full_index[['TIME_INDEX']] = full_index[['TIME_INDEX']].astype(int)
        return full_index

    def get_indexed_and_cleaned_base_data(self, base_data, var):
        # base数据去重
        cur_base_data = base_data.copy()
        cur_base_data = self.drop_duplicate_rows(cur_base_data)

        # 对给定的参数，base数据去error code
        cur_base_data = self.get_de_channeled_base_data(cur_base_data, var)
        cur_base_data, _ = self.drop_abnormal(cur_base_data, var)
        cur_base_data = cur_base_data.rename(columns={var:'BASE_{}'.format(var)})

        cur_base_data = cur_base_data[cur_base_data['BASE_{}'.format(var)] < self.max_exp_val[var]].copy()

        # 加time_index
        cur_base_data = self.get_index_for_time_align(cur_base_data)
        cur_base_data = self.attach_full_index(cur_base_data)

        # 按给定的时间aggregate
        cur_base_data = self.aggregate_data(cur_base_data,['BASE_{}'.format(var)])

        # 插值
        # 对每一台设备的数据进行插值
        cur_base_data = self.interpolate_base(cur_base_data, var)

        # 取平均
        cur_base_data = self.get_mean_of_committee(cur_base_data, var)
        cur_base_data = cur_base_data[cur_base_data['BASE_CHANNEL_COUNT'] == self.get_num_committee_channels(var)]

        return cur_base_data

    def get_mean_of_committee(self, cur_base_data, var):
        '''
        获得基准设备组的均值

        Args:
            cur_base_data: 包含基准设备组在var上测量值的的dataframe
            var: 需要处理的参数
        '''
        proc_base_data = cur_base_data[['TIME_INDEX', 'DEV_ID', 'CHANNEL', 'BASE_{}'.format(var)]].copy()

        # 对测量值求均值
        cur_base_data_mean = cur_base_data[['BASE_{}'.format(var)]].groupby(cur_base_data['TIME_INDEX']).mean()

        # 计算在该离散时间点上有插值数据的通道数量
        cur_base_data_cnt = cur_base_data[['BASE_{}'.format(var)]].groupby(cur_base_data['TIME_INDEX']).count()

        # 重命名避免出现合并时pandas自动给出的列名
        cur_base_data_cnt.rename(columns={'BASE_{}'.format(var):'BASE_CHANNEL_COUNT'}, inplace=True)

        # 合并均值，以及产生均值的设备数
        cur_base_data_combine = cur_base_data_mean.merge(cur_base_data_cnt, left_index=True, right_index=True)

        cur_base_data_combine.reset_index(inplace=True)

        # 只取出来基准设备组均值对应的行
        proc_mean = cur_base_data_combine[['TIME_INDEX', 'BASE_{}'.format(var)]].copy()
        proc_mean['DEV_ID'] = proc_mean.apply(lambda x: 'MEAN_DEV', axis=1)
        proc_mean['CHANNEL_ID'] = proc_mean.apply(lambda x: 'MEAN_CHANNEL', axis=1)

        # 与基准设备组中每一个基准通道的出数拼成一个文件，方便后面画图
        proc_mean = pd.concat([proc_base_data, proc_mean], axis=0, sort=False)
        proc_mean.to_csv(self.file_utility.get_proc_base_data_path(var, is_write=True))
        return cur_base_data_combine

    def get_indexed_and_cleaned_qe_data(self, qe_data, var, channel, criteria_objs):
        '''
        Args:
            qe_data: 某台设备某个通道的质保数据
        '''

        # 去重后检查原始数据完备率
        qe_data['CHANNEL'] = qe_data.apply(lambda x: channel, axis=1)

        qe_data = self.drop_duplicate_rows(qe_data)

        for criterion in criteria_objs:
            criterion.num_orig_entry_check(qe_data.shape[0])

        if qe_data.empty:
            return qe_data, criteria_objs

        qe_data.rename(columns={channel:var}, inplace=True)

        # 去error code后检查原始数据是否合理
        qe_data, num_dropped = self.drop_abnormal(qe_data, var)
        for criterion in criteria_objs:
            criterion.num_error_entry_check(num_dropped)

        if qe_data.empty:
            return qe_data, criteria_objs

        # 去跳变
        qe_data, num_hopped = self.drop_hop(qe_data, var, var)
        for criterion in criteria_objs:
            criterion.num_hop_entry_check(num_hopped)

        if qe_data.empty:
            return qe_data, criteria_objs

        # 增加time_index
        qe_data = self.get_index_for_time_align(qe_data)

        # 做聚合
        qe_data = self.aggregate_data(qe_data, [var])

        full_index = self.gen_full_index()
        qe_data = full_index.merge(qe_data, on='TIME_INDEX', how='left')

        qe_data = qe_data[qe_data[var] <= self.max_exp_val[var]]

        if var != self.current_var:
            self.first_qe_device = True

        if self.first_qe_device:
            qe_data.to_csv(self.file_utility.get_proc_qe_data_path(var, is_write=True))
            self.first_qe_device = False
            self.current_var = var
        else:
            qe_data.to_csv(self.file_utility.get_proc_qe_data_path(var, is_write=True), mode='a', header=False)

        return qe_data, criteria_objs

    def get_merged_and_indexed_base_and_qe_data(self, indexed_base_data, qe_data, var, channel, criteria_objs):

        indexed_qe_data, criteria_objs = self.get_indexed_and_cleaned_qe_data(qe_data, var, channel, criteria_objs)

        if indexed_qe_data.empty:
            return indexed_qe_data, criteria_objs

        indexed_merge_data = indexed_base_data.merge(indexed_qe_data, on='TIME_INDEX', how='inner')

        indexed_merge_data.dropna(inplace=True)

        return indexed_merge_data, criteria_objs

    def interpolate_base(self, data, var):
        cur_data = pd.DataFrame(columns=data.columns)
        dev_list = data['DEV_ID'].unique()

        base_type = self.dynamic_parser.get_third_layer("BASE_DEV_FOR_QE", var, "WAY_TO_QE")

        if base_type[0] == 'SINGLE_BASE':
            data = self.attach_full_index(data)
            data['BASE_{}'.format(var)].interpolate(inplace=True)
            return data
        else:
            base_committee = self.static_parser.get_second_layer("BASE_DEVICE_COMMITTEE", var)
            base_data = pd.DataFrame(columns=['CAP_TIME', 'CHANNEL', 'TIME_INDEX', 'BASE_{}'.format(var)])
            for dev, channels in base_committee.items():
                for channel in channels:
                    tmp_data = data[(data['DEV_ID'] == dev) & (data['CHANNEL'] == channel)].copy()
                    tmp_data = self.attach_full_index(tmp_data)
                    tmp_data['DEV_ID'] = tmp_data.apply(lambda x: dev, axis=1)
                    tmp_data['CHANNEL'] = tmp_data.apply(lambda x: channel, axis=1)
                    tmp_data['BASE_{}'.format(var)].interpolate(inplace=True)
                    base_data = pd.concat([base_data, tmp_data], axis=0, sort=False)
            return base_data

    def attach_full_index(self, data):
        full_index = self.gen_full_index()
        data[['TIME_INDEX']] = data[['TIME_INDEX']].astype(int)
        data = full_index.merge(data, on='TIME_INDEX', how='left')
        return data

    def get_de_channeled_base_data(self, base_data, var):
        '''
        将数据格式整理为['CAP_TIME', 'DEV_ID', 'CHANNELS', 'VAR']的形式
        '''
        base_type = self.dynamic_parser.get_third_layer("BASE_DEV_FOR_QE", var, "WAY_TO_QE")

        if base_type[0] == "SINGLE_BASE":

            base_channel = self.dynamic_parser.get_third_layer("BASE_DEV_FOR_QE", var, "BASE_CHANNEL")[0]
            base_dev = self.dynamic_parser.get_third_layer("BASE_DEV_FOR_QE", var, "BASE_DEV_ID")[0]
            cur_base_data = base_data[['CAP_TIME', 'DEV_ID', base_channel]].copy()
            cur_base_data.rename(columns={base_channel:var}, inplace=True)
            cur_base_data['CHANNEL'] = cur_base_data.apply(lambda x: base_channel, axis=1)
        else:
            cur_base_data = self.process_committee_base(base_data, var)
        return cur_base_data

    def process_committee_base(self, base_data, var):
        cur_base_data = pd.DataFrame(columns=['CAP_TIME', 'DEV_ID', 'CHANNEL', var])
        base_committee = self.static_parser.get_second_layer("BASE_DEVICE_COMMITTEE", var)

        for dev, channels in base_committee.items():
            for channel in channels:
                cur_data = base_data[(base_data['DEV_ID'] == dev)].copy()
                cur_data = cur_data[['CAP_TIME', 'DEV_ID', channel]].copy()
                cur_data.rename(columns={channel:var}, inplace=True)
                cur_data['CHANNEL'] = cur_data.apply(lambda x: channel, axis=1)
                cur_base_data = pd.concat([cur_base_data, cur_data], axis=0, sort=False)
        return cur_base_data

    def get_num_committee_channels(self, var):
        num_channels = 0
        base_type = self.dynamic_parser.get_third_layer("BASE_DEV_FOR_QE", var, "WAY_TO_QE")
        if base_type[0] == 'SINGLE_BASE':
            return 1
        else:
            base_committee = self.static_parser.get_second_layer("BASE_DEVICE_COMMITTEE", var)

            for dev, channels in base_committee.items():
                for channel in channels:
                    num_channels += 1
            return num_channels
