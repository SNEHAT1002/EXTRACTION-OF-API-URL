#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


import requests
import json
import csv


# In[3]:


import time
import datetime as dt


# In[4]:


import sqlite3


# In[5]:


# extraction of data from API using request
r=requests.get('https://devmobapp.lntidpl.com/couch/reserve/_design/reports/_view/vehiclelog')


# In[6]:


print(r.status_code)         # current status of data


# In[7]:


#print(r.text)   # op in text format


# In[8]:


print(r.json())  # op in json format


# In[9]:


data = r.json()  # storing json format data in variable


# In[10]:


#json_data = pd.read_json(data)      #  reading json data


# In[11]:


print(type(data))        # op data type


# In[12]:


df = pd.DataFrame(data)
#print(type(df))


# In[13]:


data_json = pd.read_json(r"C:\Users\Sneha Theva\OneDrive\Documents\api_data\report.json")  # read of json data


# In[14]:


data_normalize = pd.json_normalize(data_json["rows"])      # json normalize (alignment)


# Reading csv data into database

# In[216]:


# csv = data_normalize.to_csv(r"C:\Users\Sneha Theva\OneDrive\Documents\api_data\report.csv",sep=',', mode="w") 
# conversion into csv


# In[14]:


csv_file = pd.read_csv(r"C:\Users\Sneha Theva\OneDrive\Documents\api_data\report.csv")


# In[15]:


csv_file.head(5)


# In[16]:


csv_file.columns


# In[17]:


csv_file = csv_file.drop(columns = 'Unnamed: 0')


# In[18]:


test = pd.DataFrame(csv_file.columns)


# In[19]:


test.replace('.','')


# In[20]:


today_timestamp = dt.datetime.today()
extraction_timestamp = pd.Timestamp(today_timestamp)


# In[21]:


csv_file['extraction_datetime'] = extraction_timestamp


# In[22]:


extraction_timestamp


# In[23]:


list(csv_file.columns)


# In[24]:


db_conn = sqlite3.connect(r"C:\Users\Sneha Theva\OneDrive\Documents\api_data\database\test.db")


# In[25]:


c = db_conn.cursor()


# table = """CREATE TABLE csv_table(id TEXT, key TEXT, docType VARCHAR, businessObjectDefinitionId VARCHAR, project VARCHAR, date DATE,
# time DATETIME, vehicle VARCHAR, parkLigh VARCHAR, parkLighDesc VARCHAR, dimLight VARCHAR, dimLightDesc VARCHAR, brightLight
# VARCHAR, brightLightDesc VARCHAR, tailLight VARCHAR, tailLightDesc VARCHAR, brakeLight VARCHAR, brakeLightDesc VARCHAR, 
# indicator VARCHAR, indicatorDesc VARCHAR, interiorLighting VARCHAR, interiorLightingDesc VARCHAR, rotatingLight VARCHAR, 
# rotatingLightDesc VARCHAR, reflector VARCHAR, reflectorDesc VARCHAR, rearView VARCHAR, rearViewDesc VARCHAR, radio VARCHAR, 
# radioDesc VARCHAR, windScreenWiper VARCHAR, windScreenWiperDesc VARCHAR, hooters VARCHAR, hootersDesc VARCHAR, 
# spareWheel VARCHAR, spareWheelDescspareWheelDesc VARCHAR, warningGauge VARCHAR, warningGaugeDesc VARCHAR, 
# reflectiveCones VARCHAR, reflectiveConesDesc VARCHAR, fireExtinguisher VARCHAR, fireExtinguisherDesc VARCHAR, camera VARCHAR,
# cameraDesc VARCHAR, liquidContainer VARCHAR, liquidContainerDesc VARCHAR, torch VARCHAR, torchDesc VARCHAR, 
# rubberGloves VARCHAR, rubberGlovesDesc VARCHAR, leatherGloves VARCHAR, leatherGlovesDesc VARCHAR, hardBristleBroom VARCHAR, 
# hardBristleBroomDesc VARCHAR, shovel VARCHAR, shovelDesc VARCHAR,startLeads VARCHAR, startLeadsDesc VARCHAR, 
# emergencySigns VARCHAR, emergencySignsDesc VARCHAR, blankets VARCHAR, blanketsDesc VARCHAR, 
# ambFlashingLights VARCHAR, ambFlashingLightsDesc VARCHAR, firstAidKit VARCHAR, EK_Desc VARCHAR, funnel VARCHAR, 
# funnelDesc VARCHAR, hydraulicJack VARCHAR, hydraulicJackDesc VARCHAR, paSystem VARCHAR, paSystemDesc VARCHAR, 
# mobileScreenGuardCover VARCHAR, mobileScreenGuardCoverDesc VARCHAR, VTSGPS VARCHAR, VTSGPSDesc VARCHAR, 
# dashCamWithSDCard VARCHAR, dashCamWithSDCardDesc VARCHAR, hazchemKit VARCHAR, hazchemKitDesc VARCHAR, sealedBucket VARCHAR, 
# sealedBucketDesc VARCHAR, mask VARCHAR, maskDesc VARCHAR, drinkingWaterJug VARCHAR, drinkingWaterJugDesc VARCHAR, 
# salt VARCHAR, saltDesc VARCHAR, limePowder VARCHAR, limePowderDesc VARCHAR, barricadingTAPE VARCHAR, 
# barricadingTAPEDesc VARCHAR, trafficFlag VARCHAR, trafficFlagDesc VARCHAR, reflectiveCones750 VARCHAR, 
# reflectiveCones750Desc VARCHAR, reflectiveCones450 VARCHAR, reflectiveCones450Desc VARCHAR, searchLight VARCHAR, 
# searchLightDesc VARCHAR, deadAnimalHook VARCHAR, towBelt VARCHAR, towBeltDesc VARCHAR, crowbar VARCHAR, 
# crowbarDesc VARCHAR, hackSawComplete VARCHAR, hackSawCompleteDesc VARCHAR, hackSawBlades VARCHAR, hackSawBladesDesc VARCHAR,
# sandbagEmpty VARCHAR,sandbagEmptyDesc VARCHAR, bucket10Litters VARCHAR, bucket10LittersDesc VARCHAR, shrouds VARCHAR, 
# shroudsDesc VARCHAR, toolKitBox VARCHAR, toolKitBoxDesc VARCHAR, toolCutting VARCHAR, toolCuttingDesc VARCHAR, 
# fencingPliers VARCHAR, fencingPliersDesc VARCHAR, shiftingSpanner VARCHAR, shiftingSpannerDesc VARCHAR, 
# screwdriverSet VARCHAR, screwdriverSetDesc VARCHAR, ballPinHammer VARCHAR, ballPinHammerDesc VARCHAR, flatSpanner VARCHAR, 
# flatSpannerDesc VARCHAR, ringSpanner VARCHAR, ringSpannerDesc VARCHAR, fourWheelSpanner VARCHAR, electricPlier VARCHAR, 
# electricPlierDesc VARCHAR, waterPumpPlier VARCHAR, waterPumpPlierDesc VARCHAR, safetyJacket VARCHAR, 
# safetyJacketDesc VARCHAR, safetyHelmet VARCHAR, safetyHelmetDesc VARCHAR, safetyShoes VARCHAR, safetyShoesDesc VARCHAR, 
# whistleLanyard VARCHAR, whistleLanyardDesc VARCHAR, rainSuit VARCHAR, rainSuitDesc VARCHAR, fourWheelSpannerDesc VARCHAR);"""

# In[26]:


#c. execute(table)


# In[27]:


csv_file.to_sql('test', db_conn, if_exists='append', index=False)


# In[28]:


db_conn.commit()


# In[29]:


db_conn.close()


# In[30]:


db_conn = sqlite3.connect(r"C:\Users\Sneha Theva\OneDrive\Documents\api_data\database\test.db")
c = db_conn.cursor()


# In[31]:


date_check = db_conn.execute('SELECT extraction_datetime from test').fetchall()[-1]


# In[32]:


date_check


# In[33]:


attachDatabaseSql = "ATTACH DATABASE ? as new1"


# In[34]:


dbspec = (r"C:\Users\Sneha Theva\OneDrive\Documents\api_data\database\new1.db",)


# In[35]:


#cursorObject = db_conn.cursor()


# In[36]:


c.execute(attachDatabaseSql, dbspec)


# In[37]:


tablesql = """CREATE TABLE new1.new_table(id TEXT, key TEXT, docType VARCHAR, businessObjectDefinitionId VARCHAR, project VARCHAR,
date DATE, time DATETIME, vehicle VARCHAR, parkLigh VARCHAR, parkLighDesc VARCHAR, dimLight VARCHAR, dimLightDesc VARCHAR,
brightLight VARCHAR, brightLightDesc VARCHAR, tailLight VARCHAR, tailLightDesc VARCHAR, brakeLight VARCHAR,
brakeLightDesc VARCHAR, indicator VARCHAR, indicatorDesc VARCHAR, interiorLighting VARCHAR, interiorLightingDesc VARCHAR,
rotatingLight VARCHAR, rotatingLightDesc VARCHAR, reflector VARCHAR, reflectorDesc VARCHAR, rearView VARCHAR,
rearViewDesc VARCHAR, radio VARCHAR, radioDesc VARCHAR, windScreenWiper VARCHAR, windScreenWiperDesc VARCHAR,
hooters VARCHAR, hootersDesc VARCHAR, spareWheel VARCHAR, spareWheelDesc VARCHAR, warningGauge VARCHAR,
warningGaugeDesc VARCHAR, reflectiveCones VARCHAR, reflectiveConesDesc VARCHAR, fireExtinguisher VARCHAR, 
fireExtinguisherDesc VARCHAR, camera VARCHAR, cameraDesc VARCHAR, liquidContainer VARCHAR, liquidContainerDesc VARCHAR,
torch VARCHAR, torchDesc VARCHAR, rubberGloves VARCHAR, rubberGlovesDesc VARCHAR, leatherGloves VARCHAR, 
leatherGlovesDesc VARCHAR, hardBristleBroom VARCHAR, hardBristleBroomDesc VARCHAR, shovel VARCHAR, shovelDesc VARCHAR,
startLeads VARCHAR, startLeadsDesc VARCHAR, emergencySigns VARCHAR, emergencySignsDesc VARCHAR, blankets VARCHAR,
blanketsDesc VARCHAR, ambFlashingLights VARCHAR, ambFlashingLightsDesc VARCHAR, firstAidKit VARCHAR, firstAidKitDesc VARCHAR,
funnel VARCHAR, funnelDesc VARCHAR, hydraulicJack VARCHAR, hydraulicJackDesc VARCHAR, paSystem VARCHAR, paSystemDesc VARCHAR,
mobileScreenGuardCover VARCHAR, mobileScreenGuardCoverDesc VARCHAR, VTSGPS VARCHAR, VTSGPSDesc VARCHAR, 
dashCamWithSDCard VARCHAR, dashCamWithSDCardDesc VARCHAR, hazchemKit VARCHAR, hazchemKitDesc VARCHAR, sealedBucket VARCHAR,
sealedBucketDesc VARCHAR, mask VARCHAR, maskDesc VARCHAR, drinkingWaterJug VARCHAR, drinkingWaterJugDesc VARCHAR,
salt VARCHAR, saltDesc VARCHAR, limePowder VARCHAR, limePowderDesc VARCHAR, barricadingTAPE VARCHAR, 
barricadingTAPEDesc VARCHAR, trafficFlag VARCHAR, trafficFlagDesc VARCHAR, reflectiveCones750 VARCHAR, 
reflectiveCones750Desc VARCHAR, reflectiveCones450 VARCHAR, reflectiveCones450Desc VARCHAR, searchLight VARCHAR, 
searchLightDesc VARCHAR, deadAnimalHook VARCHAR, deadAnimalHookDesc VARCHAR, towBelt VARCHAR, towBeltDesc VARCHAR, crowbar VARCHAR, crowbarDesc VARCHAR,
hackSawComplete VARCHAR, hackSawCompleteDesc VARCHAR, hackSawBlades VARCHAR, hackSawBladesDesc VARCHAR, sandbagEmpty VARCHAR,
sandbagEmptyDesc VARCHAR, bucket10Litters VARCHAR, bucket10LittersDesc VARCHAR, shrouds VARCHAR, shroudsDesc VARCHAR,
toolKitBox VARCHAR, toolKitBoxDesc VARCHAR, toolCutting VARCHAR, toolCuttingDesc VARCHAR, fencingPliers VARCHAR,
fencingPliersDesc VARCHAR, shiftingSpanner VARCHAR, shiftingSpannerDesc VARCHAR, screwdriverSet VARCHAR,
screwdriverSetDesc VARCHAR, ballPinHammer VARCHAR, ballPinHammerDesc VARCHAR, flatSpanner VARCHAR, flatSpannerDesc VARCHAR,
ringSpanner VARCHAR, ringSpannerDesc VARCHAR, fourWheelSpanner VARCHAR, electricPlier VARCHAR, electricPlierDesc VARCHAR, 
waterPumpPlier VARCHAR, waterPumpPlierDesc VARCHAR, safetyJacket VARCHAR, safetyJacketDesc VARCHAR, safetyHelmet VARCHAR,
safetyHelmetDesc VARCHAR, safetyShoes VARCHAR, safetyShoesDesc VARCHAR, whistleLanyard VARCHAR, whistleLanyardDesc VARCHAR,
rainSuit VARCHAR, rainSuitDesc VARCHAR, fourWheelSpannerDesc VARCHAR, extraction_datetime DATETIME);"""


# In[38]:


c.execute(tablesql)


# In[39]:


db_date_check = db_conn.execute('SELECT extraction_datetime from test').fetchall()[-1]


# In[40]:


db_date_check


# In[41]:


db_date = pd.Timestamp(db_date_check[0])


# In[42]:


db_date


# In[43]:


today_timestamp = dt.datetime.today()
extraction_timestamp = pd.Timestamp(today_timestamp)


# In[44]:


extraction_timestamp


# In[45]:


db_conn_2 = sqlite3.connect(r"C:\Users\Sneha Theva\OneDrive\Documents\api_data\database\test.db")


# In[46]:


c = db_conn_2.cursor()


# In[47]:


table = """ CREATE TABLE STUDENT(NAME VARCHAR(255), CLASS VARCHAR(255), SECTION VARCHAR(255));"""


# In[48]:


c.execute(table)


# In[49]:


c.execute('''INSERT INTO STUDENT (CLASS, SECTION, NAME) VALUES ('7TH', 'A', 'HARISH')''')


# In[ ]:





# In[ ]:





# In[ ]:




