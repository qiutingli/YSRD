import pandas as pd

# import common
import config.json_parser as json_parser
import utility.mysql_connector as mysql_connector


class GetData:
    # 判断是不是分月情况
    def is_split(self, dynamic_path):
        jp = json_parser.ReadJson(dynamic_path)
        start_time = jp.get_first_layer('START_TIME')
        end_time = jp.get_first_layer('END_TIME')
        return start_time.split(' ')[0].split('-')[1] == end_time.split(' ')[0].split('-')[1]

    # 获取年月时间, 拼接在表头
    def get_sheet_time(self, dynamic_path, start_time):
        # start_time eg. 2018-9-20 12:25:00
        head_time_list = []
        jp = json_parser.ReadJson(dynamic_path)
        end_time = jp.get_first_layer('END_TIME')
        time_left = start_time.split(' ')[0]
        time_list = time_left.split('-')
        data_sheet_head_time = time_list[0] + time_list[1]
        head_time_list.append(data_sheet_head_time)
        if self.is_split(dynamic_path):
            pass
        else:
            temp_time = end_time.split(' ')[0].split('-')
            middle_time = temp_time[0] + temp_time[1]
            head_time_list.append(middle_time)
        return head_time_list

    # dynamic_path 动态配置文件路径， device_id_list_path 质保设备csv文件路径
    def get_qe_sql(self, dynamic_path, device_id_list_path):
        jp = json_parser.ReadJson(dynamic_path)
        capture_table = jp.get_first_layer("CAPTURE_TABLE")
        start_time = jp.get_first_layer('START_TIME')

        end_time = jp.get_first_layer('END_TIME')
        fields = jp.get_first_layer('FIELDS')
        read_csv = pd.read_csv(device_id_list_path, header=None)
        device_id_list = list(read_csv[0])

        # 由于直接调用tuple(device_id_list)不能处理只有一个设备的情况，包括只有0个设备不能报错，因此在这里增加了判断和处理
        if len(device_id_list) == 0:
            print('没有配置任何需要质保的设备，请检查配置项')
        elif len(device_id_list) == 1:
            device_id_list_sql = "('{}')".format(device_id_list[0])
        else:
            device_id_list_sql = tuple(device_id_list)

        sql_list = []

        sql_str = "SELECT DEV_ID, CAP_TIME, {} FROM DEVICE_CAPTURE_DATA_{}_{} WHERE CAP_TIME >= '{}' AND CAP_TIME <= '{}' AND DEV_ID IN {} "
        if self.is_split(dynamic_path):
            sheet_head_time = self.get_sheet_time(dynamic_path, start_time)[0]
            sql = sql_str.format(fields, capture_table, sheet_head_time, start_time, end_time, device_id_list_sql)
            sql_list.append(sql)
            return sql_list
        else:
            # 分月的话SQL语句分成两部分
            temp_time = end_time.split(' ')[0].split('-')
            middle_time = temp_time[0] + '-' + temp_time[1] + '-' + '1' + ' ' + '00:00:00'
            sheet_head_time_1, sheet_head_time_2 = self.get_sheet_time(dynamic_path, start_time)
            sql1 = sql_str.format(fields, capture_table, sheet_head_time_1, start_time, middle_time, device_id_list_sql)
            sql2 = sql_str.format(fields, capture_table, sheet_head_time_2, middle_time, end_time, device_id_list_sql)
            sql_list.append(sql1)
            sql_list.append(sql2)
        return sql_list

    # 动态配置文件路径， 质保设备csv 文件路径
    def get_qe_data(self, dynamic_path, device_id_list_path):
        database_section = 'MYSQL-SENSOR1'
        if self.is_split(dynamic_path):
            sql = self.get_qe_sql(dynamic_path, device_id_list_path)[0]
            df = mysql_connector.mysql_export_data_to_df(sql, database_section)

        else:
            sql1, sql2 = self.get_qe_sql(dynamic_path, device_id_list_path)
            df1 = mysql_connector.mysql_export_data_to_df(sql1, database_section)
            df2 = mysql_connector.mysql_export_data_to_df(sql2, database_section)
            df = pd.concat([df1, df2], axis=0, ignore_index=True)
        return df

    # 动态配置文件路径， 污染物名称， 静态配置文件路径
    def get_base_sql(self, dynamic_path, option, static_path):
        # 从动态配置文件中读取配置项
        jp = json_parser.ReadJson(dynamic_path)
        base_dev_for_qe = jp.get_second_layer("BASE_DEV_FOR_QE", option)
        way_to_qe = base_dev_for_qe['WAY_TO_QE']
        start_time = jp.get_first_layer('START_TIME')
        sheet_head_time = self.get_sheet_time(dynamic_path, start_time)
        end_time = jp.get_first_layer('END_TIME')
        # 静态配置文件中读取配置项
        jp2 = json_parser.ReadJson(static_path)
        capture_table_fields = jp2.get_first_layer("CAPTURE_TABLE_FIELDS")
        sql_list = []
        sql_str = "SELECT DEV_ID, CAP_TIME, {} FROM DEVICE_CAPTURE_DATA_{}_{} WHERE CAP_TIME >= '{}' AND CAP_TIME <= '{}' AND DEV_ID IN {} "

        if self.is_split(dynamic_path):
            # print(self.is_split(dynamic_path))
            # print("1")
            # 判断是基准设备还是基准设备组
            sheet_head_time = sheet_head_time[0]
            if way_to_qe == 'SINGLE_BASE':
                device_id_list = base_dev_for_qe['BASE_DEV_ID']
                # print(device_id_list)
                for key, value in capture_table_fields.items():
                    sql = sql_str.format(value, key, sheet_head_time, start_time, end_time, '("%s")' % device_id_list)
                    sql_list.append(sql.strip(" \n"))

            elif way_to_qe == 'BASE_DEVICE_COMMITTEE':
                jp = json_parser.ReadJson(static_path)

                try:
                    device_is_json = jp.get_second_layer('BASE_DEVICE_COMMITTEE', option)
                except:
                    raise Exception('option error')
                # print(device_is_json)
                device_id_list = tuple(device_is_json.keys())
                for key, value in capture_table_fields.items():
                    sql_list.append(
                        sql_str.format(value, key, sheet_head_time, start_time, end_time, tuple(device_id_list)))
            # print(sql_list[0])
            return sql_list
        else:
            # print("2")
            temp_time = end_time.split(' ')[0].split('-')
            middle_time = temp_time[0] + '-' + temp_time[1] + '-' + '1' + ' ' + '00:00:00'
            # print(middle_time)
            end_time = end_time
            sheet_head_time1, sheet_head_time2 = self.get_sheet_time(dynamic_path, start_time)
            if way_to_qe == 'SINGLE_BASE':
                device_id_list = base_dev_for_qe['BASE_DEV_ID']
                for key, value in capture_table_fields.items():
                    sql1 = sql_str.format(value, key, sheet_head_time1, start_time, middle_time,
                                          '("%s")' % device_id_list)
                    sql2 = sql_str.format(value, key, sheet_head_time2, middle_time, end_time,
                                          '("%s")' % device_id_list)
                    sql_list.append(sql1.strip(" \n"))
                    sql_list.append(sql2.strip(" \n"))
            elif way_to_qe == 'BASE_DEVICE_COMMITTEE':
                jp = json_parser.ReadJson(static_path)
                device_is_json = jp.get_second_layer('BASE_DEVICE_COMMITTEE', option)
                device_id_list = tuple(device_is_json.keys())
                for key, value in capture_table_fields.items():
                    sql1 = sql_str.format(value, key, sheet_head_time1, start_time, middle_time, tuple(device_id_list))
                    sql2 = sql_str.format(value, key, sheet_head_time2, middle_time, end_time, tuple(device_id_list))
                    sql_list.append(sql1)
                    sql_list.append(sql2)
            return sql_list

    def get_base_data(self, dynamic_path, option, static_path):
        database_section = 'MYSQL-SENSOR1'
        sql_list = self.get_base_sql(dynamic_path, option, static_path)
        df = pd.DataFrame(data=None,
                          columns=['DEV_ID', 'CAP_TIME', 'PM25_1', 'PM25_2', 'PM25_3', 'PM10_1', 'PM10_2', 'PM10_3',
                                   'LONGITUDE', 'LATITUDE', 'TEMPERATURE', 'HUMIDITY'])
        for sql in sql_list:
            result = mysql_connector.mysql_export_data_to_df(sql, database_section)
            # print(sql)
            df = pd.concat([df, result], axis=0, ignore_index=True, sort=False)
            if not df.empty:
                print(df.shape[0])
        # print(df.columns)
        # print(df["DEV_ID"].unique())
        return df


if __name__ == '__main__':
    dynamic_path = '../input/qe_config_dynamic.JSON'
    static_path = '../input/qe_config_static.JSON'
    device_id_list_path = '../input/qe_dev_ids.csv'
    option = 'PM25'
    getd = GetData()
    sql1 = getd.get_qe_sql(dynamic_path, device_id_list_path)
    print(sql1)
    result1 = getd.get_qe_data(dynamic_path, device_id_list_path)
    print(result1)
    # sql2 = getd.get_base_sql(dynamic_path, option, static_path)
    # print(sql2)
    # result2 = getd.get_base_data(dynamic_path, option, static_path)
    # print(result2)
