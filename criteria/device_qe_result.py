# coding=utf-8
import common

from criteria.criteria import Criteria


class DeviceCriteria():
    def __init__(self, static_parser, dynamic_parser):
        '''
        根据dynamic json parser获得所需要的质保通道，每一个通道new两个Criteria object，代表一级标准和二级标准，放到一个list当中
        '''
        self.num_levels = 2

        # key 1: channel
        # value: [Criteria1, Critera2]
        self.criteria_object_dict = {}

        # key 1: channel
        # value: [Bool, Bool]
        # 代表某个通道是否通过一级标准及二级标准
        self.is_pass_by_channel = {}

        self.init_criteria_and_pass_status(static_parser, dynamic_parser)


    def init_criteria_and_pass_status(self, static_parser, dynamic_parser):
        self.qe_channels = dynamic_parser.get_first_layer("CHANNELS_NEED_TO_BE_QUALITY_ENSURED")

        for var, channels in self.qe_channels.items():
            for channel in channels:
                self.is_pass_by_channel[channel] = [True, True]
                self.criteria_object_dict[channel] = [Criteria(var, 1, static_parser, dynamic_parser), Criteria(var, 2, static_parser, dynamic_parser)]

    def check_device_pass(self):
        for i in range(self.num_levels):
            for channel in self.criteria_object_dict.keys():
                if not self.criteria_object_dict[channel][i].is_pass:
                    self.is_pass_by_channel[channel][i] = False

    def compose_overall_assessment(self, channel, level):
        if self.is_pass_by_channel[channel][0]:
            return "通过{}级质保标准".format(1)
        elif self.is_pass_by_channel[channel][1]:
            return "通过{}级质保标准".format(2)
        else:
            return "未通过{}级质保标准".format(2)

    def get_channel_assessment_by_level(self, channel, level):
        if level == 1:
            assess_str = self.criteria_object_dict[channel][0].get_qe_msg()
            msg = '一级质保结果:\n{}'.format(assess_str)
        else:
            assess_str = self.criteria_object_dict[channel][1].get_qe_msg()
            msg = '二级质保结果:\n{}'.format(assess_str)
        return msg

    def to_string(self):
        msg = ''
        for channel in self.is_pass_by_channel.keys():
            msg += '通道 {} '.format(channel)
            msg += self.get_channel_assessment(channel)
            msg += '::'
        return msg