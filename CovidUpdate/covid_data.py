import io
import re
from datetime import datetime
from pathlib import Path
import os
import requests
import pandas as pd
from sodapy import Socrata

def reviseGEOID(df,col):
    df[col]=df[col].apply(lambda x: ''.join(['0']*(5-len(str(x)))) +str(x) if len(str(x))<5 else str(x))    
    return df

def getVacineData():
    client = Socrata("data.cdc.gov", None)
    results = client.get("q9mh-h2tw", limit=4000)
    results_df = pd.DataFrame.from_records(results)
    results_df=results_df[['fips_code', 'ability_to_handle_a_covid', 'percent_adults_fully']]
    results_df['percent_adults_fully']=results_df['percent_adults_fully'].astype(float).apply(lambda val: round(100*val,2))
    results_df['ability_to_handle_a_covid']=results_df['ability_to_handle_a_covid'].astype(float)
    results_df=reviseGEOID(results_df,'fips_code')
    return results_df

class COVID:
    def __init__(self,url):
        r=requests.get (url, stream=True).content
        all_covid_data=pd.read_csv(io.StringIO(r.decode('utf-8')))
        all_covid_data['countyFIPS'] = all_covid_data['countyFIPS'].apply(lambda x: ''.join(['0']*(5-len(str(x)))) +str(x) 
                                                                      if len(str(x))<5 else str(x) )
        
        all_covid_data = all_covid_data.loc[all_covid_data['County Name']!="Statewide Unallocated",:]
        
        self.all_covid_data=all_covid_data
        self.recent_date=COVID.getRecentData(all_covid_data.columns.tolist())
        self.recent_covid_data=all_covid_data[['countyFIPS',self.recent_date]]
    
    @staticmethod
    def getRecentData(date_list):
        date_pat="^(2021|2020)[-\/](0[1-9]|1[012])[-\/](0[1-9]|[12][0-9]|3[01])$"
        posted_dates=[datetime(int(col.split('-')[0]),int(col.split('-')[1]),int(col.split('-')[2]))
                      for col in date_list if re.match(date_pat,str(col))]
        #print (posted_dates)
        recent_date=max(posted_dates).strftime('%Y-%#m-%#d')
        recent_date_split=recent_date.split('-')

        year=recent_date_split[0]
        month = '0'+recent_date_split[1] if len(recent_date_split[1])!=2 else recent_date_split[1]
        day = '0'+recent_date_split[2] if len(recent_date_split[2])!=2 else recent_date_split[2]

        recent_date= '-'.join([year,month,day])
        return recent_date
    
class query_COVID(COVID):
    def __init__(self,url):
        super().__init__(url)
        drop_col = [col for col in self.all_covid_data.columns if ('state' in col.lower() and 'fips' in col.lower()) or
               ('county' in col.lower() and 'name' in col.lower())]
        self.all_covid_data['County']=self.all_covid_data['County Name'].apply(lambda x: x.split(' County')[0])
       
        # melt covid data by fips,county, and state. Converts all data columns to rows
        covid_data_melt=self.all_covid_data.drop(columns=drop_col).melt(id_vars=['countyFIPS','County','State'])
        covid_data_melt['Date'] = pd.to_datetime(covid_data_melt['variable'].apply(lambda x: "{0}/{1}/{2}".format(int(x.split('-')[1]),x.split('-')[2],x.split('-')[0])))
        
        #drop melted data column
        covid_data_melt=covid_data_melt.drop(columns=['variable'])
        covid_data_melt['State']=covid_data_melt['State'].str.strip()
        self.covid_data_melt=covid_data_melt

    @staticmethod
    def add_pr_states(df,col):
        pr_states=['ca','california',
                   'tx','texas',
                   'fl','florida',
                   'il','illonis',
                   'ga','georgia',
                   'nc','north carolina',
                   'va','virgina',
                   'oh','ohio',
                   'pa','pennsylvania',
                   'nj','New jersey',
                   'ny','New york',
                   'ct','connecticut'
                   'ma','massachusetts']
        
        df['PR_State'] = df[col].apply(lambda x:'PR' if x.lower() in pr_states else 'US')
        return df
    
    @staticmethod
    def query_by_date(df,date_col,days):
        df=df.set_index('Date')
        lastdayfrom=datetime.today()
        df_reset = df.loc[lastdayfrom - pd.Timedelta(days=days):lastdayfrom].reset_index()
        return df_reset
    
    def getCovidData_mthYear(self,add_pr_states=True):
        covid_all_melt_mthyear = query_COVID.query_by_date(self.covid_data_melt,'Date',365) 
        covid_all_melt_mthyear['Month_Year'] = covid_all_melt_mthyear['Date'].dt.strftime('%Y-%m').drop(columns=['Date'])
        covid_all_melt_mthyear_groupby=covid_all_melt_mthyear.groupby(['Month_Year','State','County']).sum().reset_index()
        covid_all_melt_mthyear_groupby['Month_Year']=pd.to_datetime(covid_all_melt_mthyear_groupby['Month_Year'])
        covid_all_melt_mthyear_groupby['Month_Year']=covid_all_melt_mthyear_groupby['Month_Year'].apply(lambda date:date.replace(day=2))
        if add_pr_states:
            covid_all_melt_mthyear_groupby=query_COVID.add_pr_states(covid_all_melt_mthyear_groupby,'State')
        return covid_all_melt_mthyear_groupby
    
    def getCovidData_2wks(self,add_pr_states=True):
        covid_all_melt_2wks = query_COVID.query_by_date(self.covid_data_melt,'Date',14) 
        if add_pr_states:
            covid_all_melt_2wks=query_COVID.add_pr_states(covid_all_melt_2wks,'State')
        return covid_all_melt_2wks
    
