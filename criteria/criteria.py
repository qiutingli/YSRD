# coding=utf-8
import math
import numpy as np
import pandas as pd

import common

import config.json_parser as json_parser
import utility.time_utility as tu



class Criteria():
    '''
    标准类，用于记录标准的关键参数，以及check给定的数据是否符合标准
    '''
    def __init__(self, var, level, static_parser, dynamic_parser):
        '''
        1. 给定质保设备出数的间隔，质保起始时间和质保结束时间，计算完美情况下的数据条数
        2. 根据json parser初始化各种标准

        Args:
            var: 该标准对应的参数
            level: 该标准对应的标准级别
            qe_interval_in_sec: 以秒衡量的质保设备出数间隔
            start_time: 质保（老化）开始时间
            end_time: 质保（老化）结束时间
        '''
        self. min_mae_check_entries = 5
        self.var = var
        self.level = level
        if self.level == 1:
            self.session = "CRITERIA_1"
        elif self.level == 2:
            self.session = "CRITERIA_2"
        else:
            print("The specified criteria level is not supported")

        self.start_time = dynamic_parser.get_first_layer("START_TIME")
        self.end_time = dynamic_parser.get_first_layer("END_TIME")
        self.qe_interval_in_sec = dynamic_parser.get_first_layer("QE_TIME_INTERVAL")

        self.criteria_key_list = ['upper_range_mae_perc_ceil', 'lower_range_mae_ceil', 'post_reg_maep', 'maep', 'mae', 'corr_floor', 'slope_floor', 'slope_ceil', 'orig_entry_rate_floor', 'error_rate_ceil', 'hop_rate_ceil']

        self.criteria_ch = {
         'upper_range_mae_perc_ceil': '高量程相对偏差最大值',
         'lower_range_mae_ceil': '低量程绝对偏差最大值',
         'post_reg_maep': '一致性模型后平均相对偏差',
         'maep': '相对偏差',
         'mae': '绝对偏差',
         'corr_floor': '相关系数最小值',
         'corr_ceil': '相关系数最大值',
         'slope_floor': '斜率最小值',
         'slope_ceil': '斜率最大值',
         'orig_entry_rate_floor': '原始数据缺数率',
         'error_rate_ceil': '错误码比例',
         'hop_rate_ceil': '跳变比例'
        }

        self.num_complete_entris = math.floor(tu.get_seconds_given_time_str(self.start_time, self.end_time)/self.qe_interval_in_sec)

        self.init_criteria(var, static_parser)
        self.init_qe_msg(var)

        self.is_pass = True

        # 加载每个参数评估哪些标准
        self.criteria_by_var = static_parser.get_second_layer("CRITERIA_ITEM_BY_VAR", self.var)

        # 用于记录计算过的统计量
        self.statistics_recorder = {}

    def init_criteria(self, var, static_json_parser):
        self.criteria = {}
        for criterion in self.criteria_key_list:
            self.criteria[criterion] = static_json_parser.get_third_layer(self.session, self.var, criterion)[0]

    def init_qe_msg(self, var):
        self.msg = {}
        for criterion in self.criteria_key_list:
            self.msg[criterion] = '未检查'

    def  num_orig_entry_check(self, num_orig_entries):
        '''
        检查原始数据的条数是否高于规定比例
        '''
        criterion_key = 'orig_entry_rate_floor'

        if not criterion_key in self.criteria_by_var:
            return

        entry_rate = num_orig_entries/self.num_complete_entris
        self.statistics_recorder["EFFECTIVE_RATE"] = entry_rate

        if entry_rate < self.criteria[criterion_key]:
            self.is_pass = False
            self.msg[criterion_key] = '原始数据有效条数 {}, 占比 {:.4f}, 低于标准有效率 {:.4f}, 原始数据完整率检测不通过~~~'.format(num_orig_entries, entry_rate, self.criteria[criterion_key])
        else:
            self.msg[criterion_key] = '原始数据数据完整率检测通过'

    def num_orig_hourly_entry_check(self, data):
        pass

    def num_error_entry_check(self, num_error_code_entries):
        '''
        检查含错误码的比例是否低于规定比例
        '''
        criterion_key = 'error_rate_ceil'

        if not criterion_key in self.criteria_by_var:
            return

        error_rate = num_error_code_entries/self.num_complete_entris
        self.statistics_recorder["ERROR_RATE"] = error_rate

        if error_rate > self.criteria[criterion_key]:
            self.is_pass = False
            self.msg[criterion_key] = '错误码条数 {}, 占比 {:.4f}, 高于错误码比例阈值 {:.4f}, 检测不通过~~~'.format(num_error_code_entries, error_rate, self.criteria[criterion_key])
        else:
            self.msg[criterion_key] = '错误码占比为{:.4f}, 检测通过'.format(error_rate)

    def num_hop_entry_check(self, num_hops):
        '''
        检查跳变的条数是否低于规定比例
        '''
        criterion_key = 'hop_rate_ceil'

        if not criterion_key in self.criteria_by_var:
            return

        hop_rate = num_hops/self.num_complete_entris
        self.statistics_recorder["HOP_RATE"] = hop_rate

        if hop_rate > self.criteria[criterion_key]:
            self.is_pass = False
            self.msg[criterion_key] = '跳变条数 {}, 占比 {:.4f}, 高于错误码比例阈值 {:.4f}，跳变检测不通过~~~'.format(num_hops, hop_rate, self.criteria[criterion_key])
        else:
            self.msg[criterion_key] = '跳变检测通过'

    def lower_range_mae_check(self, merge_df, static_json_parser):
        '''
        检查在低量程的时候平均偏差是否小于规定的值
        '''
        criterion_key = 'lower_range_mae_ceil'
        if not criterion_key in self.criteria_by_var:
            return

        cutting_point = static_json_parser.get_second_layer("PARA", self.var)['scale_cutting_point']

        # 检查低量程
        low_df = merge_df[(merge_df['BASE_{}'.format(self.var)] < cutting_point) & (merge_df[self.var] < cutting_point)].copy()

        mae = np.mean(abs(low_df['BASE_{}'.format(self.var)] - low_df[self.var]))

        if low_df.empty:
            self.msg[criterion_key] = '低量程(< {})数据缺失，低量程绝对偏差检测未执行*****'.format(cutting_point)
        elif low_df.shape[0] <= self. min_mae_check_entries:
            self.msg[criterion_key] = '低量程(< {}) {}条少于{}条，低量程绝对偏差检测未执行*****'.format(cutting_point, low_df.shape[0], self. min_mae_check_entries)
        elif mae > self.criteria[criterion_key]:
            self.is_pass = False
            self.msg[criterion_key] = '低量程(< {})绝对偏差为{:.4f}, 高于标准{:.4f}， 低量程绝对偏差检测不通过~~~'.format(cutting_point, mae, self.criteria[criterion_key])
        else:
            self.msg[criterion_key] = '低量程(< {})绝对偏差为{:.4f}, 低量程绝对偏差检测通过'.format(cutting_point, mae)

        del low_df

    def upper_range_mae_percent_check(self, merge_df, static_json_parser):
        # 检查高量程
        criterion_key = 'upper_range_mae_perc_ceil'
        if not criterion_key in self.criteria_by_var:
            return

        cutting_point = static_json_parser.get_second_layer("PARA", self.var)['scale_cutting_point']
        max_exp_val = static_json_parser.get_second_layer("MAX_EXP_VAL", self.var)

        high_df = merge_df[(merge_df[self.var] <= max_exp_val) & (merge_df[self.var] >= cutting_point)].copy()

        mae_percent = np.mean(abs(high_df['BASE_{}'.format(self.var)] - high_df[self.var]) / high_df['BASE_{}'.format(self.var)])

        if high_df.empty:
            self.msg[criterion_key] = '高量程({}-{})数据缺失，高量程相对偏差检测未执行*****'.format(cutting_point, max_exp_val)
        elif high_df.shape[0] <= self. min_mae_check_entries:
            self.msg[criterion_key] = '高量程({}-{}) {}条数据少于{}条，高量程相对偏差检测未执行*****'.format(cutting_point, max_exp_val, high_df.shape[0], self. min_mae_check_entries)
        elif mae_percent > self.criteria[criterion_key]:
            self.is_pass = False
            self.msg[criterion_key] = '高量程({}-{})相对偏差为{:.4f}, 高于标准{:.4f}，高量程相对偏差检测不通过~~~'.format(cutting_point, max_exp_val, mae_percent, self.criteria[criterion_key])
        else:
            self.msg[criterion_key] = '高量程({}-{})相对偏差为{:4f}, 相对偏差检测通过'.format(cutting_point, max_exp_val, mae_percent)

        del high_df

    def post_reg_mae_percent_check(self, mae_percent):
        self.statistics_recorder["POST_MAEP"] = mae_percent
        criterion_key = 'post_reg_maep'
        if not criterion_key in self.criteria_by_var:
            return
        if mae_percent > self.criteria[criterion_key]:
            self.is_pass = False
            self.msg[criterion_key] = '拟合后相对偏差为{:.4f}, 高于标准{:.4f}, 拟合后相对偏差检测不通过~~~'.format(mae_percent, self.criteria[criterion_key])
        else:
            self.msg[criterion_key] = '拟合后相对偏差为{:.4f}, 拟合后相对偏差检测通过'.format(mae_percent)

    def maep_check(self, merge_df, static_json_parser):
        criterion_key = 'maep'
        if not criterion_key in self.criteria_by_var:
            return

        max_exp_val = static_json_parser.get_second_layer("MAX_EXP_VAL", self.var)
        cur_df = merge_df[(merge_df[self.var] <= max_exp_val)].copy()

        maep = np.mean(abs(cur_df['BASE_{}'.format(self.var)] - cur_df[self.var]) / cur_df['BASE_{}'.format(self.var)])
        self.statistics_recorder["MAEP"] = maep

        if maep > self.criteria[criterion_key]:
            self.is_pass = False
            self.msg[criterion_key] = '相对偏差为{:.4f}, 高于标准{:.4f}，相对偏差(平行性)检测不通过~~~'.format(maep, self.criteria[criterion_key])
        else:
            self.msg[criterion_key] = '相对偏差为{:4f}, 相对偏差（平行性）检测通过'.format(maep)

        del cur_df

    def mae_check(self, merge_df, static_json_parser):
        criterion_key = 'mae'
        if not criterion_key in self.criteria_by_var:
            return
        max_exp_val = static_json_parser.get_second_layer("MAX_EXP_VAL", self.var)
        cur_df = merge_df[(merge_df[self.var] <= max_exp_val)].copy()

        mae = np.mean(abs(cur_df['BASE_{}'.format(self.var)] - cur_df[self.var]))
        self.statistics_recorder["MAE"] = mae

        if mae > self.criteria[criterion_key]:
            self.is_pass = False
            self.msg[criterion_key] = '平均绝对偏差为{:.4f}, 高于标准{:.4f}，绝对偏差检测不通过~~~'.format(mae, self.criteria[criterion_key])
        else:
            self.msg[criterion_key] = '平均绝对偏差为{:4f}, 绝对偏差检测通过'.format(mae)

        del cur_df

    def correlation_check(self, merge_df):
        criterion_key = 'corr_floor'
        if not criterion_key in self.criteria_by_var:
            return
        corr = np.corrcoef(merge_df['BASE_{}'.format(self.var)], merge_df[self.var])[0, 1]
        self.statistics_recorder["CORR"] = corr

        if corr < self.criteria[criterion_key]:
            self.is_pass = False
            self.msg[criterion_key] = '相关系数{:.4f}低于标准{:.4f}, 检测不通过~~~'.format(corr, self.criteria[criterion_key])
        else:
            self.msg[criterion_key] = '相关系数为{:.4f}, 检测通过'.format(corr)

    def slope_floor_check(self, slope):
        criterion_key = 'slope_floor'
        if not criterion_key in self.criteria_by_var:
            return
        if slope < self.criteria[criterion_key]:
            self.is_pass = False
            self.msg[criterion_key] = '斜率为{:.4f}, 低于标准范围最低值{:.4f}, 最低值检测不通过~~~'.format(slope, self.criteria[criterion_key])
        else:
            self.msg[criterion_key] = '斜率为{:.4f}, 最低值检测通过'.format(slope)

    def slope_ceil_check(self, slope):
        criterion_key = 'slope_ceil'
        if not criterion_key in self.criteria_by_var:
            return
        if slope > self.criteria[criterion_key]:
            self.is_pass = False
            self.msg[criterion_key] = '斜率为{:.4f}, 高于标准范围最高值{:.4f}，最高值检测不通过~~~'.format(slope, self.criteria[criterion_key])
        else:
            self.msg[criterion_key] = '斜率{:.4f}, 最高值检测通过'.format(slope)

    def get_qe_msg(self):
        msg = ''
        for criterion in self.criteria_by_var:
            msg += '{}: {}\n'.format(self.criteria_ch[criterion], self.msg[criterion])
        return msg

    def get_overall_assessment(self):
        if self.is_pass == True:
            return '通过质保{}级标准'.format(self.level)
        else:
            return '未通过质保{}级标准'.format(self.level)


    def get_statistics(self):
        return self.statistics_recorder



