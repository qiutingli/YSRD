import json
import jsonpath

import common


# 数据取到 倒数第二层，取出的是字典结构
class ReadJson:
    def __init__(self, path):
        try:
            fp = open(path, 'r+')
            json_str = fp.read()
            json_object = json.loads(json_str, encoding='utf-8')
            self.json_object = json_object
            fp.close()
        except:
            print('path error,confirm your config path')

    # 获取一个非最内层不重名子节点下的所有内容
    # 返回值是一个list
    def get_rest(self, section):
        result = jsonpath.jsonpath(self.json_object, expr='$..%s' % section)
        return result

    # 获取json第一层数据
    def get_first_layer(self, section):
        result = jsonpath.jsonpath(self.json_object, expr='$..%s' % section)
        if result:
            return result[0]
        else:
            return ''

    # 获取json第二层数据
    def get_second_layer(self, section=None, option=None):
        result = jsonpath.jsonpath(self.json_object, expr='$..%s.%s' % (section, option))
        return result[0]

    # 获取json第三层数据
    def get_third_layer(self, section, option, key):
        result = jsonpath.jsonpath(self.json_object, expr='$..%s.%s.%s' % (section, option, key))
        return result

    # 修改标准(三层)
    def modify_criteria(self, path, section, option, key, value):
        self.json_object[section][option][key] = value
        json_str = json.dumps(self.json_object)
        with open(path, 'w+') as fp:
            fp.write(json_str)


if __name__ == '__main__':
    path = 'qe_config.json'
    readjson = ReadJson(path)
    conf = readjson.get_first_layer('CRITERIA_1')
    print(conf)
    conf2 = readjson.get_second_layer('CRITERIA_1', 'PM25')
    print(conf2)
