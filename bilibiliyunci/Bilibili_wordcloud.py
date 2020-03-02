import requests
import json
import re


# 根据av号获得cid
def get_cid(av):
    av = av.strip('av')
    url = f'https://api.bilibili.com/x/player/pagelist?aid={av}&jsonp=jsonp'
    res = requests.get(url)
    res_text = res.text
    res_dict = json.loads(res_text)
    cid = res_dict['data'][0]['cid']
    return cid


# 根据cid 请求弹幕
def get_dan_mu(cid):
    url = f'https://api.bilibili.com/x/v1/dm/list.so?oid={cid}'
    res = requests.get(url)
    res_xml = res.content.decode('utf-8')
    pattern = re.compile('<d.*?>(.*?)</d>')
    dan_mu_list = pattern.findall(res_xml)
    return dan_mu_list


# 保存弹幕文件
def save_to_file(dan_mu_list, filename):
    with open(filename, mode='w',encoding='utf-8') as f:
        for one_dan_mu in dan_mu_list:
            f.write(one_dan_mu)
            f.write('\n')


# 主流程
def main(av):
    cid = get_cid(av)
    dan_mu_list=get_dan_mu(cid)
    save_to_file(dan_mu_list,f'{av}.txt')


if __name__ == '__main__':
    av_code = 'av88149130'
    main(av_code)
