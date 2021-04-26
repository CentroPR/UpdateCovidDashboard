import io
import re
from datetime import datetime
from pathlib import Path
import os
import requests
import pandas as pd
from sodapy import Socrata
from states import us_state_abbrev,pr_states

def reviseGEOID(df,col):
    df.loc[:,col]=df.loc[:,col].apply(lambda x: ''.join(['0']*(5-len(str(x)))) +str(x) if len(str(x))<5 else str(x))    
    return df

def getVacineData():
    client = Socrata("data.cdc.gov", None)
    results = client.get("q9mh-h2tw", limit=4000)
    results_df = pd.DataFrame.from_records(results)
    results_df=results_df.loc[:,['fips_code', 'ability_to_handle_a_covid', 'percent_adults_fully']]
    results_df.loc[:,'percent_adults_fully']=results_df.loc[:,'percent_adults_fully'].astype(float).apply(lambda val: round(100*val,2))
    results_df.loc[:,'ability_to_handle_a_covid']=results_df.loc[:,'ability_to_handle_a_covid'].astype(float)
    results_df.loc[:,'fips_code']=results_df.loc[:,'fips_code'].astype(int)
    results_df=reviseGEOID(results_df,'fips_code')
    return results_df

class COVID:
    def __init__(self,source='usafacts'):
        if source=='usafacts':
            usafacts_covid_dth_url="https://static.usafacts.org/public/data/covid-19/covid_deaths_usafacts.csv"
            usafacts_covid_inf_url="https://static.usafacts.org/public/data/covid-19/covid_confirmed_usafacts.csv"

            r=requests.get (usafacts_covid_dth_url, stream=True).content
            all_covid_data=pd.read_csv(io.StringIO(r.decode('utf-8')))
            all_covid_data=all_covid_data.drop(columns='StateFIPS').rename(columns={'countyFIPS':'fips','County Name':'county','State':'state'})

            r=requests.get (usafacts_covid_inf_url, stream=True).content
            all_covid_data2=pd.read_csv(io.StringIO(r.decode('utf-8')))
            all_covid_data2=all_covid_data2.drop(columns='StateFIPS').rename(columns={'countyFIPS':'fips','County Name':'county','State':'state'})

            covid_data_melt=all_covid_data.melt(id_vars=['fips','county','state'],var_name='date',value_name='deaths')
            covid_data_melt2=all_covid_data2.melt(id_vars=['fips','county','state'],var_name='date',value_name='cases')
            covid_data_melt['deaths']=covid_data_melt['deaths'].astype(int)
            
            all_covid_data=all_covid_data.loc[all_covid_data['county']!="Statewide Unallocated",:]
            all_covid_data=pd.merge(covid_data_melt,covid_data_melt2,on=['fips','county','state','date'])
            all_covid_data=all_covid_data.loc[all_covid_data.loc[:,'county']!="Statewide Unallocated",:]
            all_covid_data['county']= all_covid_data['county'].apply(lambda countyname: countyname.replace(" County",'') if " County" in countyname else countyname)
            
        elif source=='nytimes':
            url="https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"
            all_covid_data=pd.read_csv(url)
            all_covid_data.loc[:,'state'] = all_covid_data.loc[:,'state'].map(us_state_abbrev)
            
        self.all_covid_data=all_covid_data.loc[:,:].dropna(subset=['fips'])
        self.all_covid_data.loc[:,'fips']=self.all_covid_data.loc[:,'fips'].astype(int)
        self.all_covid_data.loc[:,'fips']=reviseGEOID(self.all_covid_data,'fips')
        self.all_covid_data.loc[:,'date']=pd.to_datetime(self.all_covid_data.loc[:,'date'])
        
        self.recent_date = self.all_covid_data.loc[:,'date'].max()
        self.recent_covid_data=self.all_covid_data.loc[self.all_covid_data.loc[:,'date']==self.recent_date,:]
        
        query_us = (self.all_covid_data.loc[:,'state']!="Puerto Rico")
        
        self.covid_data_query_us = self.all_covid_data.loc[query_us,:]
        
        self.recent_date_us = self.covid_data_query_us.loc[:,'date'].max()
        self.recent_covid_data_us=self.covid_data_query_us.loc[self.covid_data_query_us.loc[:,'date']==self.recent_date_us,:]
        
        if source=='nytimes':
            query_pr = (self.all_covid_data.loc[:,'state']=="Puerto Rico") & (self.all_covid_data.loc[:,'county']!="Unknown")
            self.covid_data_query_pr = self.all_covid_data.loc[query_pr,:]
            self.recent_date_pr = self.covid_data_query_pr.loc[:,'date'].max()
            self.recent_covid_data_pr=self.covid_data_query_pr.loc[self.covid_data_query_pr.loc[:,'date']==self.recent_date_pr,:]

        
class query_COVID(COVID):
    def __init__(self,source):
        super().__init__(source=source)

    def _us_or_pr(self,us_or_pr='us'):
        if us_or_pr=='us':
            return self.covid_data_query_us
        elif us_or_pr=='pr':
            return self.covid_data_query_pr
        
    @staticmethod
    def add_pr_states(df,col):
        df.loc[:,'PR_State'] = df.loc[:,col].apply(lambda x:'PR' if x.lower() in pr_states else 'US')
        return df
    
    @staticmethod
    def query_by_date(df,date_col,days):
        df=df.set_index(date_col)
        lastdayfrom=datetime.today()
        df_reset = df.loc[lastdayfrom - pd.Timedelta(days=days):lastdayfrom].reset_index()
        return df_reset
    
    def getCovidData_mthYear(self,cases_or_dths='cases',add_pr_states=True,us_or_pr='us'):
        covid_data_query=query_COVID._us_or_pr(self,us_or_pr=us_or_pr)
        if us_or_pr=='pr':
            days=730
        else:
            days=365
        covid_data_query_mthyear = query_COVID.query_by_date(covid_data_query,'date',days) 
        covid_data_query_mthyear.loc[:,'Month_Year'] = covid_data_query_mthyear.loc[:,'date'].dt.strftime('%Y-%m').drop(columns=['Date'])
        covid_data_query_mthyear=covid_data_query_mthyear.groupby(['Month_Year','state','county']).sum().reset_index()
        covid_data_query_mthyear.loc[:,'Month_Year']=pd.to_datetime(covid_data_query_mthyear.loc[:,'Month_Year'])
        covid_data_query_mthyear.loc[:,'Month_Year']=covid_data_query_mthyear.loc[:,'Month_Year'].apply(lambda date:date.replace(day=2))
            
        if add_pr_states:
            covid_data_query_mthyear=query_COVID.add_pr_states(covid_data_query_mthyear,'state')
        return covid_data_query_mthyear
    
    def getCovidData_2wks(self,cases_or_dths='cases',add_pr_states=True,us_or_pr='us'):
        covid_data_query=query_COVID._us_or_pr(self,us_or_pr=us_or_pr)
        if us_or_pr=='pr':
            days=28
        else:
            days=14
        covid_data_query_mthyear_2wks = query_COVID.query_by_date(covid_data_query,'date',days) 
        if add_pr_states:
            covid_data_query_mthyear_2wks=query_COVID.add_pr_states(covid_data_query_mthyear_2wks,'state')
        return covid_data_query_mthyear_2wks

    
