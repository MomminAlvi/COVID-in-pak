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
    'cookie': 'at_check=true; AMCVS_ED8D65E655FAC7797F000101^%^40AdobeOrg=1; esri_gdpr=oneTrust; AMCV_ED8D65E655FAC7797F000101^%^40AdobeOrg=-408604571^%^7CMCIDTS^%^7C18470^%^7CMCMID^%^7C64270285105456225450389713582496853230^%^7CMCAAMLH-1596369392^%^7C3^%^7CMCAAMB-1596369392^%^7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y^%^7CMCOPTOUT-1595771793s^%^7CNONE^%^7CMCAID^%^7CNONE^%^7CvVersion^%^7C4.6.0; s_cc=true; sat_track=true; pi_opt_in8202=true; _biz_uid=943ad0eced034ad89f31ae92ea294d49; _fbp=fb.1.1595764598842.1490977485; _biz_flagsA=^%^7B^%^22Version^%^22^%^3A1^%^2C^%^22Ecid^%^22^%^3A^%^221583890594^%^22^%^2C^%^22XDomain^%^22^%^3A^%^221^%^22^%^2C^%^22ViewThrough^%^22^%^3A^%^221^%^22^%^7D; mbox=session^#257df9db6ce04ecc9969a08a054d9006^#1595766497^|PC^#257df9db6ce04ecc9969a08a054d9006.38_0^#1659009394; s_tp=5899; s_sq=^%^5B^%^5BB^%^5D^%^5D; OptanonConsent=isIABGlobal=false^&datestamp=Sun+Jul+26+2020+16^%^3A57^%^3A16+GMT^%^2B0500+(Pakistan+Standard+Time)^&version=5.15.0^&landingPath=NotLandingPage^&groups=1^%^3A1^%^2C2^%^3A1^%^2C3^%^3A1^%^2C4^%^3A1^&hosts=^&legInt=^&consentId=97296c15-2de3-42cb-8011-48d3a5e3bb87^&interactionCount=0^&AwaitingReconsent=false; s_ptc=0.00^%^5E^%^5E0.00^%^5E^%^5E0.00^%^5E^%^5E0.00^%^5E^%^5E0.34^%^5E^%^5E0.40^%^5E^%^5E1.84^%^5E^%^5E0.01^%^5E^%^5E2.22; _biz_nA=4; _uetsid=f3be92e3564252386cbb0128282395a4; _uetvid=bd311e7d5119515c38a19b38c77dba2f; _biz_pendingA=^%^5B^%^5D; s_ppv=developers.arcgis.com^%^253A^%^2520rest^%^253A^%^2520geoenrichment^%^253A^%^2520api-reference^%^253A^%^2520geoenrichment-service-overview.htm^%^2C100^%^2C13^%^2C5898',
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
            print ('Done for Today!')
            break
        ndf = pd.DataFrame(result[key])
        ndf['Date'] = ftime
        cols = list(ndf.columns.values)
        ndf = ndf[[cols[-1]] + cols[0:4]]
        df_dict[key] = df_dict[key].append(ndf)
    writer = pd.ExcelWriter('master.xlsx', engine='xlsxwriter')
    for sheet_name in df_dict.keys():
        df_dict[sheet_name].to_excel(writer, sheet_name=sheet_name, index=False)
    writer.save()

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
