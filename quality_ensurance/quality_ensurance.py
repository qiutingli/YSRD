# coding=utf-8
import time
import os
from sklearn.linear_model import LinearRegression
from sklearn.externals import joblib
import pandas as pd
import statsmodels.api as sml
import numpy as np
import time
import datetime as dt

import common

import prepare.prepare as prepare
import config.json_parser as json_parser
from criteria.device_qe_result import DeviceCriteria
from prepare.prepare import Prepare
from dao.read_data import GetData
from utility.file_utility import FileUtility
from plot.plotter import Plotter

pd.set_option('max_columns', 50)


class QualityEnsurance():
    def __init__(self):
        # 初始化static_parser和dynamic parser
        self.dynamic_path = "%s/input/qe_config_dynamic.JSON" % os.path.abspath(os.pardir)
        self.static_path = "%s/input/qe_config_static.JSON" % os.path.abspath(os.pardir)

        self.dynamic_parser = json_parser.ReadJson(self.dynamic_path)
        self.static_parser = json_parser.ReadJson(self.static_path)
        self.file_utility = FileUtility()
        self.file_utility.mkdirs()

        # # 获得相关数据并读取
        self.load_data_from_db()
        self.qe_data = pd.read_csv(self.file_utility.get_orig_qe_data_path(), sep=',')
        self.should_qe_dev = list(pd.read_csv(self.file_utility.get_should_qe_dev_list_path(), header=None)[0])
        print(self.should_qe_dev)
        print(self.should_qe_dev)
        self.qe_dev_list = self.qe_data['DEV_ID'].unique()
        self.device_criterion_res = {}
        self.init_device_criterion_obj()

        # 初始化质保标准相关的对象
        # key: dev_id
        # value: DeviceCriteria对象

        # 由dynamic parser获得通道
        # key: variable, e.g., PM25, PM10
        # value: channel list
        self.qe_channels = self.dynamic_parser.get_first_layer("CHANNELS_NEED_TO_BE_QUALITY_ENSURED")

        self.qe_vars = self.qe_channels.keys()
        self.prepare = Prepare()
        self.plotter = Plotter()

        self.statistics = {}
        self.hourly_entry = {}

    def init_device_criterion_obj(self):
        for dev in self.should_qe_dev:
            self.device_criterion_res[dev] = DeviceCriteria(self.static_parser, self.dynamic_parser)

    def load_data_from_db(self):
        device_id_list_path = "%s/input/qe_dev_ids.csv" % os.path.abspath(os.pardir)
        vars_to_qe = self.dynamic_parser.get_first_layer("CHANNELS_NEED_TO_BE_QUALITY_ENSURED")
        vars_to_qe = vars_to_qe.keys()
        getd = GetData()

        for var in vars_to_qe:
            base_res = getd.get_base_data(self.dynamic_path, var, self.static_path)
            base_res.to_csv(self.file_utility.get_orig_base_data_path(var, is_write=True))

        qe_res = getd.get_qe_data(self.dynamic_path, device_id_list_path)

        if qe_res is None:
            print('!!!被质保的数据读取数据为空，很可能是配置的sql有问题或者capture表不对，请检查动态配置文件！')

        qe_res.to_csv(self.file_utility.get_orig_qe_data_path(is_write=True))

    def compose_qe_result(self):
        cri_df = self.compose_qe_criteria_header()
        for dev in self.device_criterion_res.keys():
            tmp_cri = self.device_criterion_res[dev]
            tmp_cri.check_device_pass()
            tmp_dict = {}
            tmp_dict['DEV_ID'] = dev
            for var in self.qe_vars:
                for channel in self.qe_channels[var]:
                    tmp_dict['{}_LEVEL_I_PASS'.format(channel)] = tmp_cri.is_pass_by_channel[channel][0]
                    tmp_dict['{}_LEVEL_II_PASS'.format(channel)] = tmp_cri.is_pass_by_channel[channel][1]
                    tmp_dict['{}_LEVEL_I_DESC'.format(channel)] = tmp_cri.get_channel_assessment_by_level(channel, 1) 
                    tmp_dict['{}_LEVEL_II_DESC'.format(channel)] = tmp_cri.get_channel_assessment_by_level(channel, 2)
            cri_df = cri_df.append(tmp_dict, ignore_index=True)
        cri_df.to_csv(self.file_utility.get_assessmemt_file_path(), encoding="utf_8_sig", float_format='%.3f',index=False)

    def compose_qe_criteria_header(self):
        criteria_cols = ['DEV_ID']
        for var in self.qe_vars:
            print(self.qe_channels[var])
            for channel in self.qe_channels[var]:
                print(channel)
                criteria_cols = criteria_cols + ['{}_LEVEL_I_PASS'.format(channel)]
                print(criteria_cols)
                criteria_cols = criteria_cols + ['{}_LEVEL_II_PASS'.format(channel)]
                criteria_cols = criteria_cols + ['{}_LEVEL_I_DESC'.format(channel)]
                criteria_cols = criteria_cols + ['{}_LEVEL_II_DESC'.format(channel)]
        cri_df = pd.DataFrame(columns=criteria_cols)
        return cri_df

    def batch_perform_merge_df_check(self, merge_df, dev, channel):
        # 一个channel对应两个标准
        for cri_obj in self.device_criterion_res[dev].criteria_object_dict[channel]:
            cri_obj.lower_range_mae_check(merge_df, self.static_parser)
            cri_obj.upper_range_mae_percent_check(merge_df, self.static_parser)
            cri_obj.correlation_check(merge_df)
            cri_obj.mae_check(merge_df, self.static_parser)
            cri_obj.maep_check(merge_df, self.static_parser)

    def get_consistency_models(self, merge_df, dev, var, channel, res_df):
        coef, mae, rsquared, mae_percent = self.regress(merge_df[var], merge_df['BASE_{}'.format(var)], var)
        if var == 'TVOC':
            intercept = coef['const']
            print("intercept: ", intercept)
            slope = coef[var]
            print("slope: ", slope)
            res_dict = {'DEV_ID':dev, 'CHANNEL':channel, 'SLOPE': slope, 'INTERCEPT': intercept,
                        'R-SQAURED':rsquared, 'MAE':mae, 'MAE%':mae_percent, 'NUM_POINTS':merge_df.shape[0]}
        else:
            slope = coef[var]
            res_dict = {'DEV_ID': dev, 'CHANNEL': channel, 'SLOPE': slope, 'INTERCEPT': 0,
                        'R-SQAURED': rsquared, 'MAE': mae, 'MAE%': mae_percent, 'NUM_POINTS': merge_df.shape[0]}
        res_df = res_df.append(res_dict, ignore_index=True)

        for cri_obj in self.device_criterion_res[dev].criteria_object_dict[channel]:
            cri_obj.slope_floor_check(slope)
            cri_obj.slope_ceil_check(slope)
            cri_obj.post_reg_mae_percent_check(mae_percent)
        return res_df

    def regress(self, X, y, var):
        print(y.shape[0])
        if var == 'TVOC':
            X_new = sml.add_constant(X)
            results = sml.OLS(y, X_new).fit()
        else:
            results = sml.OLS(y, X).fit()
        mae = np.mean(abs(y.values - results.fittedvalues))
        mae_percent = np.mean(abs(y.values - results.fittedvalues)/(y.values + 1))
        return results.params, mae, results.rsquared, mae_percent

    def init_res_df(self):
        return pd.DataFrame(columns=['DEV_ID', 'CHANNEL', 'SLOPE', 'INTERCEPT', 'R-SQAURED', 'MAE', 'MAE%', 'NUM_POINTS'])

    def make_plots(self):
        vars_to_qe = self.dynamic_parser.get_first_layer("CHANNELS_NEED_TO_BE_QUALITY_ENSURED").keys()

        for var in vars_to_qe:
            qe_type = self.dynamic_parser.get_second_layer("BASE_DEV_FOR_QE", var)
            if qe_type['WAY_TO_QE'] == "SINGLE_BASE":
                base_dev_dict = {self.dynamic_parser.get_second_layer("BASE_DEV_FOR_QE", var)['BASE_DEV_ID']:[self.dynamic_parser.get_second_layer("BASE_DEV_FOR_QE", var)['BASE_CHANNEL']]}
            else:
                base_dev_dict = self.static_parser.get_second_layer("BASE_DEVICE_COMMITTEE", var)

            qe_channel_list = self.dynamic_parser.get_second_layer("CHANNELS_NEED_TO_BE_QUALITY_ENSURED", var)

            self.plotter.plot_overall_original_data(var, base_dev_dict, qe_channel_list)
            self.plotter.plot_overall_processed_data(var, base_dev_dict, qe_channel_list)


    def get_hourly_time_index(self, data, start_time):
        d0 = dt.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        index = []
        cap_time = data['CAP_TIME']
        for i in cap_time.index:
            d = dt.datetime.strptime(cap_time[i], '%Y-%m-%d %H:%M:%S')
            delta = 3600
            ind = (d - d0).total_seconds() / delta
            ind = int(np.ceil(ind))
            index.append(ind)
        return index

    def check_effective_rate(self, nrow):
        if nrow/12 >= 0.9:
            return 1
        else:
            return 0

    def handle_num_orig_hourly_entry(self, cur_qe_data):
        start_time = self.dynamic_parser.get_first_layer("START_TIME")
        time_index = self.get_hourly_time_index(cur_qe_data, start_time)
        indexed_data = cur_qe_data.assign(TIME_INDEX=time_index)
        for i in np.unique(time_index):
            num = self.check_effective_rate(indexed_data[indexed_data["TIME_INDEX"] == i].shape[0])
            try:
                self.hourly_entry["HOUR_{}".format(str(i))] += num
            except:
                self.hourly_entry["HOUR_{}".format(str(i))] = num


    def main_routine(self):
        res_df = self.init_res_df()
        # 对每个参数的每个通道
        # for each device，
        for var in self.qe_vars:
            print('正在处理{}...'.format(var))
            self.base_data = pd.read_csv(self.file_utility.get_orig_base_data_path(var), sep=',')

            indexed_base_data = self.prepare.get_indexed_and_cleaned_base_data(self.base_data, var)

            for channel in self.qe_channels[var]:
                print("CHANNEL: ", channel)
                # 对存在于qe_dev_ids.csv但是实际从数据库中查询出来的数据为0的情况直接标注为有效数据为0
                self.statistics[channel] = {}
                for dev in self.should_qe_dev:
                    if not dev in self.qe_dev_list:
                        print ('dev {} has no data'.format(dev))
                        for cri_obj in self.device_criterion_res[dev].criteria_object_dict[channel]:
                                cri_obj.num_orig_entry_check(0)

                    # 处理从数据库中能够查询出来数据的情况
                    else:
                        print ('dev {} has data'.format(dev))
                        cur_qe_data = self.qe_data[(self.qe_data['DEV_ID'] == dev)][['CAP_TIME', 'DEV_ID', channel]].copy()
                        indexed_merge_df, criteria_objs = self.prepare.get_merged_and_indexed_base_and_qe_data(indexed_base_data, cur_qe_data, var, channel, self.device_criterion_res[dev].criteria_object_dict[channel])

                        # 如果经过各种处理之后无数据了，则直接continue，不再进行下面的检查
                        if indexed_merge_df.empty:
                            continue

                        self.device_criterion_res[dev].criteria_object_dict[channel] = criteria_objs
                        # TODO: 取消注释
                        self.plotter.make_dev_level_plots(var, indexed_merge_df, dev, channel)
                        if indexed_merge_df.shape[0] >= 20:
                            res_df = self.get_consistency_models(indexed_merge_df, dev, var, channel, res_df)
                        else:
                            print('Too few entries ({}) for regression of variable {}!'.format(indexed_merge_df.shape[0], var))
                        self.batch_perform_merge_df_check(indexed_merge_df, dev, channel)

                        # TODO: 将以下用于处理统计量的代码放到合适的位置。注意：因需要获得统计量增加了静态配置文件里的 CRITERIA_ITEM_BY_VAR
                        stats = self.device_criterion_res[dev].criteria_object_dict[channel][0].get_statistics()
                        self.statistics[channel][dev] = stats
                        if channel == 'TSP':
                            self.handle_num_orig_hourly_entry(cur_qe_data)

            print('{}处理完毕！'.format(var))
        res_df.to_csv(self.file_utility.get_reg_file_path())
        self.compose_qe_result()

        try:
            os.makedirs('../output/statistics')
        except Exception as e:
            print(e)
        for i in ['TSP', 'TEMPERATURE', 'HUMIDITY']:
            pd.DataFrame(self.statistics[i]).T.to_csv('../output/statistics/{}_statistics.csv'.format(i))
        hourly_entry_df = pd.DataFrame([self.hourly_entry]).T
        hourly_entry_df.columns = ['NUM_DEVS']
        pd.DataFrame(hourly_entry_df['NUM_DEVS']/len(self.qe_dev_list)).to_csv('../output/statistics/hourly_entry.csv')

def test():
    start_time = time.time()

    qe = QualityEnsurance()
    qe.main_routine()
    qe.make_plots()

    end_time = time.time()
    print('Total execution time is {} seconds'.format(end_time - start_time))

test()


