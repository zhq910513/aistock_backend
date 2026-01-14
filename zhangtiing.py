#!/usr/bin/python3
# -*- coding: utf-8 -*-

import hashlib
import pprint
import time
from urllib.parse import urlencode

import requests

# 行业
r_trades_filter = [
    '建筑材料',
    '医疗器械',
    '工业金属',
    '光学光电子',
    '其他社会服务',
    '非汽车交运',
    '电力设备',
    '食品加工制造',
    '半导体及元件',
    '国防军工',
    '化学制药',
    '贵金属',
    '专用设备',
    '生物制品',
    '消费电子',
    '计算机应用',
    '建筑装饰',
    '医药商业',
    '电力',
    '小金属',
    '通信设备',
    '厨卫电器',
    '中药',
    '通用设备',
    '传媒',
    '包装印刷',
    '饮料制造',
    '家用轻工',
    '教育',
    '医疗服务',
    '服装家纺'
]

# 限制板块  000 深市A股   002 中小板   300 创业板   600/601/603 沪市A股
r_market_filter = ['000']

requests.packages.urllib3.disable_warnings()
pp = pprint.PrettyPrinter(indent=4)

# https://data.10jqka.com.cn/dataapi/limit_up/continuous_limit_pool?page=1&limit=200&field=199112,10,330329,330325,133971,133970,1968584,3475914,3541450,9004&filter=HS,GEM2STAR&order_field=330329&order_type=0&date=&_=1655790923953

# 运行时间   每天下午16：00


def get_lianbang_stocks():
    hour_now = int(time.strftime("%H", time.localtime(time.time())))
    if hour_now >= 18:
        date = time.strftime("%Y%m%d", time.localtime(time.time()))
    else:
        date = time.strftime("%Y%m%d", time.localtime(time.time() - 86400))
    url = "https://data.10jqka.com.cn/dataapi/limit_up/continuous_limit_pool"
    params = {
        "page": "1",
        "limit": "200",
        "field": "199112,10,330329,330325,133971,133970,1968584,3475914,3541450,9004",
        "filter": "HS,GEM2STAR",
        "order_field": "330329",
        "order_type": "0",
        "date": f"{date}"
    }
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Host': 'data.10jqka.com.cn',
        'Pragma': 'no-cache',
        'sec-ch-ua': '" Not A;Brand";v="99", "Chromium";v="102", "Google Chrome";v="102"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'
    }
    try:
        req = requests.get(url=url, headers=headers, params=urlencode(params), verify=False)
        if req.status_code == 200:
            if req.json().get('status_code') == 0 and req.json().get('status_msg') == 'success':
                result = req.json().get('data')
                all_data = result.get('info')
                print(f'---------- {date} 连榜 {len(all_data)} 只 ----------')
                for data in all_data:
                    try:
                        if str(data['code'])[:3] not in r_market_filter: continue
                        hash_key = hashlib.md5((date + data['code']).encode("utf8")).hexdigest()
                        data.update({'hash_key': hash_key, 'date': date})
                        print(data)
                    except:
                        pass
            else:
                print(req.json())
    except Exception as error:
        print(error)


if __name__ == '__main__':
    get_lianbang_stocks()


# 返回的样例数据
# {'code': '000593', 'limit_up_type': '换手板', 'order_volume': 1172490.0, 'is_new': 0, 'currency_value': 6241192000.0, 'market_id': 33, 'sum_market_value': 6243765900.0, 'is_again_limit': 1, 'change_rate': 9.981, 'turnover_rate': 20.3314, 'order_amount': 20413051.0, 'high_days': '3天3板', 'name': '德龙汇能', 'high_days_value': 196611, 'change_tag': 'LIMIT_BACK', 'market_type': 'HS', 'latest': 17.41, 'time_preview': [3.6639, 7.3279, 5.5591, 7.012, 7.8332, 6.7593, 5.8117, 5.2432, 7.3279, 8.0227, 8.7808, 8.2123, 8.4018, 9.9179, 9.4757, 9.602, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.9179, 9.223, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981, 9.981], 'hash_key': '9e2e10bab6f2feffc29b3505e5d8f7b3', 'date': '20260114'}

