import os

import common


class FileUtility:
    def __init__(self):
        self.file_names = {
            "orig_base_data":"orig_base_data_{}.csv",
            "orig_qe_data":"orig_qe_data.csv",
            "processed_base_data":"proc_base_data_{}.csv",
            "processed_qe_data":"proc_qe_data_{}.csv",
            "reg_result":"reg_result.csv",
            "assessment":"assessment_result.csv",
            "orig_base_overall_plot":"orig_base_overall_{}.png",
            "orig_qe_overall_plot":"orig_qe_overall_{}.png",
            "proc_base_overall_plot":"proc_base_overall_{}.png",
            "proc_qe_overall_plot":"proc_qe_overall_{}.png",
            "proc_line_plot":"proc_line_{}_{}.png",
            "proc_scatter_plot":"proc_scatter_{}_{}.png"
        }
        self.data_path = '{}/output/data'.format(os.pardir)
        self.plot_path = '{}/output/plot'.format(os.pardir)
        self.numeric_path = '{}/output/numeric'.format(os.pardir)

    def mkdirs(self):
        if not os.path.exists(self.data_path):
            os.makedirs(self.data_path)

        if not os.path.exists(self.plot_path):
            os.makedirs(self.plot_path)

        if not os.path.exists(self.numeric_path):
            os.makedirs(self.numeric_path)

    def get_should_qe_dev_list_path(self):
        return "{}/input/qe_dev_ids.csv".format(os.pardir)

    def get_orig_base_data_path(self, var, is_write=False):
        cur_path = self.data_path + "/" + self.file_names["orig_base_data"].format(var)

        if is_write or os.path.exists(cur_path):
            return cur_path
        else:
            return -1

    def get_orig_qe_data_path(self, is_write=False):
        cur_path = self.data_path + "/" + self.file_names["orig_qe_data"]

        if is_write or os.path.exists(cur_path):
            return cur_path
        else:
            return -1

    def get_proc_base_data_path(self, var, is_write=False):
        cur_path = self.data_path + "/" + self.file_names["processed_base_data"].format(var)

        if is_write or os.path.exists(cur_path):
            return cur_path
        else:
            return -1

    def get_proc_qe_data_path(self, var, is_write=False):
        cur_path = self.data_path + "/" + self.file_names["processed_qe_data"].format(var)

        if is_write or os.path.exists(cur_path):
            return cur_path
        else:
            return -1

    def get_reg_file_path(self):
        return self.numeric_path + "/" + self.file_names["reg_result"]

    def get_assessmemt_file_path(self):
        return self.numeric_path + "/" + self.file_names["assessment"]

    def get_orig_overall_base_plot(self, var):
        if not os.path.exists(self.plot_path + "/{}".format(var)):
            os.makedirs(self.plot_path + "/{}".format(var))

        return self.plot_path + "/{}/".format(var) + self.file_names["orig_base_overall_plot"].format(var)

    def get_orig_overall_qe_plot(self, var):
        if not os.path.exists(self.plot_path + "/{}".format(var)):
            os.makedirs(self.plot_path + "/{}".format(var))

        return self.plot_path + "/{}/".format(var) + self.file_names["orig_qe_overall_plot"].format(var)

    def get_proc_overall_base_plot(self, var):
        if not os.path.exists(self.plot_path + "/{}".format(var)):
            os.makedirs(self.plot_path + "/{}".format(var))

        return self.plot_path + "/{}/".format(var) + self.file_names["proc_base_overall_plot"].format(var)

    def get_proc_overall_qe_plot(self, var):
        if not os.path.exists(self.plot_path + "/{}".format(var)):
            os.makedirs(self.plot_path + "/{}".format(var))

        return self.plot_path + "/{}/".format(var) + self.file_names["proc_qe_overall_plot"].format(var)

    def get_dev_line_plot(self, var, dev, channel):
        if not os.path.exists(self.plot_path + "/{}".format(var)):
            os.makedirs(self.plot_path + "/{}".format(var))

        return self.plot_path + "/{}/".format(var) + self.file_names["proc_line_plot"].format(dev, channel)

    def get_dev_scatter_plot(self, var, dev, channel):
        if not os.path.exists(self.plot_path + "/{}".format(var)):
            os.makedirs(self.plot_path + "/{}".format(var))

        return self.plot_path + "/{}/".format(var) + self.file_names["proc_scatter_plot"].format(dev, channel)
