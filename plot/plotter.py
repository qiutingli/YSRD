import matplotlib.pyplot as plt
import pandas as pd
import os

import common
from utility.file_utility import FileUtility
import utility.time_utility as tu

class Plotter():
    def __init__(self):
        self.file_utility = FileUtility()

    def plot_overall_original_data(self, var, base_dev_channel_dict, qe_dev_channel_list):
        '''
        Args:
            var: 需要画图的参数名
            base_dev_channel_dict: {DEV_ID:CHANNEL_LIST} 基准设备ID及通道
            qe_dev_channel_list: 需要被质保设备在var这个参数下的通道
            data_type: 画图的数据类型（原始数据或者是处理后的数据）, 'original' or 'processed'
        '''
        fig = plt.figure(figsize=(12, 8))
        ax = fig.add_subplot(111)
        base_df = pd.read_csv(self.file_utility.get_orig_base_data_path(var))
        # 把captime转化为日期形式
        base_df['CAP_TIME'] = base_df.apply(lambda x: tu.time_str_to_datetime(x.CAP_TIME), axis=1)
        for dev in base_dev_channel_dict.keys():
            for channel in base_dev_channel_dict[dev]:
                tmp_df = base_df[(base_df['DEV_ID'] == dev)][['CAP_TIME', channel]]
                tmp_df.sort_values(by=['CAP_TIME'], inplace=True)
                ax.plot(tmp_df['CAP_TIME'], tmp_df[channel], label='{}:{}'.format(dev, channel))
        ax.legend()
        ax.set_title('{}: Base devices with base channels [original]'.format(var), fontsize=14)
        plt.savefig(self.file_utility.get_orig_overall_base_plot(var))

        # 画出要被质保的数据的原图
        no_qe_data = True
        fig = plt.figure(figsize=(12, 8))
        ax = fig.add_subplot(111)
        qe_df = pd.read_csv(self.file_utility.get_orig_qe_data_path())

        qe_df['CAP_TIME'] = qe_df.apply(lambda x: tu.time_str_to_datetime(x.CAP_TIME), axis=1)
        qe_dev = qe_df['DEV_ID'].unique()
        for dev in qe_dev:
            for channel in qe_dev_channel_list:
                tmp_df = qe_df[qe_df['DEV_ID'] == dev][['CAP_TIME', channel]]
                if tmp_df.empty:
                    continue
                no_qe_data = False
                tmp_df.sort_values(by=['CAP_TIME'], inplace=True)
                ax.plot(tmp_df['CAP_TIME'], tmp_df[channel])

        if not no_qe_data:
            ax.set_title('{}: Devices to be quality ensured [original]'.format(var), fontsize=14)
            plt.savefig(self.file_utility.get_orig_overall_qe_plot(var))

        plt.close()

    def plot_overall_processed_data(self, var, base_dev_channel_dict, qe_dev_channel_list):
        '''
        Args:
            var: 需要画图的参数名
            base_dev_channel_dict: {DEV_ID:CHANNEL_LIST} 基准设备ID及通道
            qe_dev_channel_list: 需要被质保设备在var这个参数下的通道
            data_type: 画图的数据类型（原始数据或者是处理后的数据）, 'original' or 'processed'
        '''

        base_dev_channel_dict['MEAN_DEV'] = ['MEAN_CHANNEL']
        fig = plt.figure(figsize=(12, 8))
        ax = fig.add_subplot(111)
        base_df = pd.read_csv(self.file_utility.get_proc_base_data_path(var))
        base_dev_list = base_dev_channel_dict.keys()

        for dev in base_dev_list:
            for channel in base_dev_channel_dict[dev]:
                tmp_df = base_df[(base_df['DEV_ID'] == dev) & (base_df['CHANNEL'] == channel)][['DEV_ID', 'CHANNEL', 'BASE_{}'.format(var), 'TIME_INDEX']]
                tmp_df.sort_values(by=['TIME_INDEX'], inplace=True)
                ax.plot(tmp_df['TIME_INDEX'], tmp_df['BASE_{}'.format(var)], label='{}:{}'.format(dev, channel))
        ax.legend()
        ax.set_title('{}: Base devices with base channels [processed]'.format(var), fontsize=14)
        plt.savefig(self.file_utility.get_proc_overall_base_plot(var))

        fig = plt.figure(figsize=(12, 8))
        ax = fig.add_subplot(111)

        if os.path.exists(self.file_utility.get_proc_qe_data_path(var)):
            qe_df = pd.read_csv(self.file_utility.get_proc_qe_data_path(var))
        else:
            return

        no_qe_data = True
        qe_dev = qe_df['DEV_ID'].unique()
        for dev in qe_dev:
            for channel in qe_dev_channel_list:

                tmp_df = qe_df[(qe_df['DEV_ID'] == dev) & (qe_df['CHANNEL'] == channel)][['TIME_INDEX', 'CHANNEL', var]]

                if tmp_df.empty:
                    continue
                no_qe_data = False

                tmp_df.sort_values(by=['TIME_INDEX'], inplace=True)
                ax.plot(tmp_df['TIME_INDEX'], tmp_df[var])

        if not no_qe_data:
            ax.set_title('{}: Devices to be quality ensured [original]'.format(var), fontsize=14)
            plt.savefig(self.file_utility.get_proc_overall_qe_plot(var))

        plt.close()

    def make_dev_level_plots(self, var, merge_df, qe_dev, qe_channel):
        self.make_line_plot(var, merge_df, qe_dev, qe_channel)
        self.make_scatter_plot(var, merge_df, qe_dev, qe_channel)

    def make_line_plot(self, var, merge_df, qe_dev, qe_channel):
        if merge_df.empty:
            return

        fig = plt.figure(figsize=(12, 8))
        ax = fig.add_subplot(111)
        ax.plot(merge_df['TIME_INDEX'], merge_df['BASE_{}'.format(var)], label='base device (group)')
        ax.plot(merge_df['TIME_INDEX'], merge_df[var], label='{}:{}'.format(qe_dev, qe_channel))
        ax.set_title('time plot for device {}'.format(qe_dev))
        ax.legend()
        ax.set_xlabel('time in minutes')
        ax.set_ylabel('{}'.format(var))
        plt.savefig(self.file_utility.get_dev_line_plot(var, qe_dev, qe_channel))
        plt.close()

    def make_scatter_plot(self, var, merge_df, qe_dev, qe_channel):
        if merge_df.empty:
            return

        fig = plt.figure(figsize=(12, 8))
        ax = fig.add_subplot(111)
        ax.scatter(merge_df['BASE_{}'.format(var)], merge_df[var])
        ax.set_xlabel('Base device')
        ax.set_ylabel('Device')
        ax.set_title('scatter plot for device {}'.format(qe_dev))
        plt.savefig(self.file_utility.get_dev_scatter_plot(var, qe_dev, qe_channel))
        plt.close()
