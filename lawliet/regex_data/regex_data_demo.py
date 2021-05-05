# @Time    : 2021/4/28 11:43 上午
# @Author  : luoh
# @Email   : luohenghlx@163.com
# @File    : regex_data_demo.py
# @Software: PyCharm
# @Description:

import  re

target = """
2021-04-26 00:01:06 [http-nio-9010-exec-3] [7e3569899809411cabb8ad804aba90bf] [ INFO] [com.fuse.artemis.api.service.tokopedia.travel.order.TokopeidaTravelOrderValidator:26] -to be validate obj :{"cBackUrl":"https://www.tokopedia.com/flight-insurance/asuransi/perjalanan/webhook/1/updatepolicy","contactEmail":"sandisaputra2232@yahoo.com","contactName":"Sandi Saputra","contactPhone":"81272093232","destination":"Palembang","flightOrigins":[{"arrival":"01/05/2021 07:20:00","departure":"01/05/2021 05:00:00","destination":"Indonesia","flightCompanyName":"Batik Air","flightCompanyShortName":"Batik Air","flightNumber":"ID6881","journeyID":"374364","origin":"Indonesia","routeID":"407916","timeZone":"Asia/Jakarta","utcOffsetHours":"+07"},{"arrival":"01/05/2021 09:40:00","departure":"01/05/2021 08:35:00","destination":"Indonesia","flightCompanyName":"Batik Air","flightCompanyShortName":"Batik Air","flightNumber":"ID6872","journeyID":"374364","origin":"Indonesia","routeID":"407917","timeZone":"Asia/Jakarta","utcOffsetHours":"+07"}],"flightPlan":"ONEWAY","flightReturns":[],"flightType":"DOMESTIC","insuranceType":"0","insureds":[{"name":"Sandi Saputra","passportNumber":"","type":"ADULT","userID":"5434471"}],"merchantCode":"TK00001","merchantKey":"6542B775E3263C27E321B929F52FC6E0","origin":"Medan","postingID":"3811554","premiumPrice":"30000","randomStr":"GHNNJGJIPYRNBKOE","sign":"68460520DCEB5C6B8C32F1CBDEC85F68","tokopediaUserID":"1565601","tripEnd":"01/05/2021","tripStart":"01/05/2021","uniqueCode":"3811554"}

"""

pattern = re.compile(r'{"cBackUrl"(.+?)"}')

result = pattern.findall(target)

print(result)

