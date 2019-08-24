import pandas as pd


class Adopt:
    # 判断设备包含经纬度与不包含经纬度两种方式的通过情况

    def judge(self, ass_path):
        data = pd.read_csv(ass_path)
        df = pd.DataFrame(data)
        # print(list(df.columns))
        for i in range(len(df.index)):
            switch = 1
            for j in list(df.columns):
                if str(j[-7:]) == 'II_PASS':
                    # print(j[-7:])
                    # print(df[j][i])
                    if bool(df[j][i]) is False:
                        # print('0')
                        switch = switch * 0
                    else:
                        # print('1')
                        switch = switch * 1
            if switch == 1:
                df.loc[i, 'contain'] = '1'
            else:
                df.loc[i, 'contain'] = '0'

        for i in range(len(df.index)):
            switch = 1
            for j in list(df.columns)[0:-9]:
                if str(j[-7:]) == 'II_PASS':
                    if bool(df[j][i]) is False:
                        switch = switch * 0
                    else:
                        switch = switch * 1
            if switch == 1:
                df.loc[i, 'notcontain'] = '1'
            else:
                df.loc[i, 'notcontain'] = '0'
        # print(df)
        return df

    # 将通过标准的设备 格式化输出
    def format_result(self, reg_path, ass_path):
        data = pd.read_csv(reg_path, keep_default_na=False)
        df = pd.DataFrame(data)
        result = self.judge(ass_path)
        # print(result)
        temp = result['notcontain'] == '1'
        dev_ids = result[temp]['DEV_ID']
        # print(dev_ids)
        empty_df = pd.DataFrame(data=None)
        for dev_id in list(dev_ids):
            df3 = df[df['DEV_ID'] == dev_id]
            empty_df = empty_df.append(df3, ignore_index=True)
        print(empty_df)
        result_df = pd.DataFrame()
        result_df['index'] = empty_df.index
        result_df['DEV_ID'] = empty_df['DEV_ID']
        result_df['VAR'] = empty_df['CHANNEL']
        try:
            result_df['ARG1'] = empty_df['SLOPE']
        except:
            result_df['ARG1'] = empty_df['COEFFICIENT']
        try:
            result_df['ARG2'] = empty_df['INTERCEPT']
        except:
            result_df['ARG2'] = '0'
        result_df['MODEL_FILE'] = empty_df['DEV_ID'] + '_' + empty_df['CHANNEL']
        result_df.sort_values('index', inplace=True)
        sum_num = len(result_df.index)
        select_num = sum_num - len(dev_ids)*4
        final_result = result_df.iloc[0:select_num]
        return final_result


if __name__ == '__main__':
    reg_path = '../output/numeric/reg_result.csv'
    ass_path = '../output/numeric/assessment_result.csv'
    adopt = Adopt()
    result1 = adopt.judge(ass_path)
    print("result1: ", result1.head())
    result2 = adopt.format_result(reg_path, ass_path)
    print("result2: ", result2.head())

