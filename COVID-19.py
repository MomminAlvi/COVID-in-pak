import urllib.request, urllib.parse, urllib.error
import ssl
import json
import pandas as pd
import xlsxwriter
import time
import datetime
import requests

def clean_dict ():
    serviceurl ='https://services1.arcgis.com/0MSEUqKaxRlEPj5g/arcgis/rest/services/ncov_cases/FeatureServer/1/query?'

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    #parameters for pakistan
    headers = {
    'authority': 'services1.arcgis.com',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.89 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-user': '?1',
    'sec-fetch-dest': 'document',
    'referer': 'https://services1.arcgis.com/0MSEUqKaxRlEPj5g/arcgis/rest/services/ncov_cases/FeatureServer/2/query',
    'accept-language': 'en-US,en;q=0.9',
    'if-none-match': 'sd128643_411915915',
    'if-modified-since': 'Wed, 22 Jul 2020 06:47:12 GMT',
    }

    params = (
        ('where', 'OBJECTID>0'),
        ('objectIds', '28'),
        ('time', ''),
        ('geometry', ''),
        ('geometryType', 'esriGeometryEnvelope'),
        ('inSR', ''),
        ('spatialRel', 'esriSpatialRelIntersects'),
        ('resultType', 'none'),
        ('distance', '0.0'),
        ('units', 'esriSRUnit_Meter'),
        ('returnGeodetic', 'false'),
        ('outFields', '*'),
        ('returnGeometry', 'false'),
        ('featureEncoding', 'esriDefault'),
        ('multipatchOption', 'xyFootprint'),
        ('maxAllowableOffset', ''),
        ('geometryPrecision', ''),
        ('outSR', ''),
        ('datumTransformation', ''),
        ('applyVCSProjection', 'false'),
        ('returnIdsOnly', 'false'),
        ('returnUniqueIdsOnly', 'false'),
        ('returnCountOnly', 'false'),
        ('returnExtentOnly', 'false'),
        ('returnQueryGeometry', 'false'),
        ('returnDistinctValues', 'false'),
        ('cacheHint', 'false'),
        ('orderByFields', ''),
        ('groupByFieldsForStatistics', ''),
        ('outStatistics', ''),
        ('having', ''),
        ('resultOffset', ''),
        ('resultRecordCount', ''),
        ('returnZ', 'false'),
        ('returnM', 'false'),
        ('returnExceededLimitFeatures', 'true'),
        ('quantizationParameters', ''),
        ('sqlFormat', 'none'),
        ('f', 'pjson'),
        ('token', ''),
    )

    response = requests.get('https://services1.arcgis.com/0MSEUqKaxRlEPj5g/arcgis/rest/services/ncov_cases/FeatureServer/2/query', headers=headers, params=params)

    #parameters for provinces
    parms = dict()
    where = 'OBJECTID>0'
    objectIds = '42, 48, 144, 185, 220, 360, 416'
    geometryType = 'esriGeometryEnvelope'
    spatialRel = 'esriSpatialRelIntersects'
    resultType = 'none'
    distance = 0.0
    fval = False
    tval = True
    units = 'esriSRUnit_Meter'
    outFields = '*'
    featureEncoding = 'esriDefault'
    multipatchOption = 'xyFootprint'
    sqlFormat = None
    f = 'pjson'
    parms['where'] = where
    parms['objectIds'] = objectIds
    parms['geometryType'] = geometryType
    parms['SpatialRel'] = spatialRel
    parms['resultType'] = resultType
    parms['distance'] = distance
    parms['units'] = units
    parms['returnGeodetic'] = fval
    parms['outFields'] = outFields
    parms['returnGeometry'] = fval
    parms['featureEncoding'] = featureEncoding
    parms['multipatchOption'] = multipatchOption
    parms['applyVCSProjection'] = fval
    parms['returnIdsOnly'] = fval
    parms['returnUniqueIdsOnly'] = fval
    parms['returnCountOnly'] = fval
    parms['returnExtentOnly'] = fval
    parms['returnQueryGeometry'] = fval
    parms['returnDistinctValues'] = fval
    parms['cacheHint'] = fval
    parms['returnZ'] = fval
    parms['returnM'] = fval
    parms['returnExceededLimitFeatures'] = tval
    parms['sqlFormat'] = sqlFormat
    parms['f'] = f
    url = serviceurl + urllib.parse.urlencode(parms)
    uh = urllib.request.urlopen(url, context=ctx)

    data = uh.read().decode()
    js = json.loads(data)
    js['features'] =js['features'] + response.json()['features']

    master_dict = {}

    for item in js['features']:
        ind_list = []
        data_dict = {}
        info = item['attributes']
        data_dict['Total']     = info['Confirmed']
        data_dict['Recovered'] = info['Recovered']
        data_dict['Deaths']    = info['Deaths']
        data_dict['Active']    = info['Active']
        ind_list.append(data_dict)
        try:
            master_dict[info['Province_State']] = ind_list
        except:
            master_dict[info['Country_Region']] = ind_list
    return master_dict

result = clean_dict()

stime = time.ctime(time.time())
ftime = stime[4:11] +stime[20:24]
df_dict = {}

try:

    df_dict = pd.read_excel('master.xlsx', sheet_name = None)
    for key in result:
        if df_dict[key].iloc[-1,0] == ftime :
            continue
        ndf = pd.DataFrame(result[key])
        ndf['Date'] = ftime
        cols = list(ndf.columns.values)
        ndf = ndf[[cols[-1]] + cols[0:4]]
        df_dict[key] = df_dict[key].append(ndf)
    writer = pd.ExcelWriter('master.xlsx', engine='xlsxwriter')
    for sheet_name in df_dict.keys():
        df_dict[sheet_name].to_excel(writer, sheet_name=sheet_name, index=False)
    writer.save()
    print ('Done for today!')

except:
    for key in result:
        df = pd.DataFrame(result[key])
        df['Date'] = ftime
        cols = list(df.columns.values)
        df = df[[cols[-1]] + cols[0:4]]
        df_dict[key] = df
    writer = pd.ExcelWriter('master.xlsx', engine='xlsxwriter')
    for sheet_name in df_dict.keys():
        df_dict[sheet_name].to_excel(writer, sheet_name=sheet_name, index=False)
    writer.save()
    print ('Done for today!')
