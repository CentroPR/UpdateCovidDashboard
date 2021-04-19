from covid_data import *
from agol import *
from arcgis.gis import GIS
from pathlib import Path
import os

item_ids={'COVID DATA':'4512d4375c4d49c0815d606d3bad8930',
          'covid_dth_monthyr':'46b77c822f934c25b36d0acc847b3042',
          'covid_dth_2wks':'2c04d718485b47deb9f3460c23ca4fe5',
          'covid_inf_monthyr':'bee78be853064da7996d7497753db503',
          'covid_inf_2wks':'c780192fb4724d8e81e9154fc2fa23aa'}

BASE_DIR = os.path.dirname(Path(__file__).resolve())
CSV_PATH=os.path.join(BASE_DIR,'CSV')

def run_update():
    gis =GIS(username="adeleon_HC",password="centroPR123")
    
    #get covid data
    new_covid_dth_url="https://static.usafacts.org/public/data/covid-19/covid_deaths_usafacts.csv"
    covid_dth=query_COVID(new_covid_dth_url)

    new_covid_inf_url="https://static.usafacts.org/public/data/covid-19/covid_confirmed_usafacts.csv"
    covid_inf=query_COVID(new_covid_inf_url)

    #get covid death counts from last 2 weeks
    #get covid deaths counts from last 12 months
    dth_by_2wks=covid_dth.getCovidData_2wks()
    dth_by_month=covid_dth.getCovidData_mthYear()

    dth_by_2wks_path=os.path.join(CSV_PATH,'covid_dth_2wks.csv')
    dth_by_month_path=os.path.join(CSV_PATH,'covid_dth_monthyr.csv')

    dth_by_2wks.to_csv(dth_by_2wks_path)
    dth_by_month.to_csv(dth_by_month_path)

    #get existing item containing previous covid deaths counts for last 2 weeks
    #get existing item containing previous covid deaths counts for last 12 months
    covid_dth2wks_item=ItemUpdate(gis,itemid=item_ids['covid_dth_2wks'])
    covid_dth12mths_item=ItemUpdate(gis,itemid=item_ids['covid_dth_monthyr'])

    #overwrite covid death items
    covid_dth2wks_item.overwriteItem(dth_by_2wks_path)
    covid_dth12mths_item.overwriteItem(dth_by_month_path)

    #get covid infection counts from last 2 weeks
    #get covid infection counts from last 12 months
    inf_by_2wks=covid_inf.getCovidData_2wks()
    inf_by_month=covid_inf.getCovidData_mthYear()

    inf_by_2wks_path=os.path.join(CSV_PATH,'covid_inf_2wks.csv')
    inf_by_month_path=os.path.join(CSV_PATH,'covid_inf_monthyr.csv')

    inf_by_2wks.to_csv(inf_by_2wks_path)
    inf_by_month.to_csv(inf_by_month_path)

    #get existing item containing previous covid infection counts for last 2 weeks
    #get existing item containing previous covid infection counts for last 12 months
    covid_inf2wks_item=ItemUpdate(gis,itemid=item_ids['covid_inf_2wks'])
    covid_inf12mths_item=ItemUpdate(gis,itemid=item_ids['covid_inf_monthyr'])

    #overwrite covid infection items
    covid_inf2wks_item.overwriteItem(inf_by_2wks_path)
    covid_inf12mths_item.overwriteItem(inf_by_month_path)

    #get main item on agol "COVID DATA
    covid_alldata=ItemUpdate(gis,itemid=item_ids['COVID DATA'])
    covid_alldata_item=covid_alldata.item

    #get recent data and the data of most recent data
    covid_inf_data=covid_inf.recent_covid_data
    covid_inf_date=covid_inf.recent_date

    covid_dth_data=covid_dth.recent_covid_data
    covid_dth_date=covid_dth.recent_date

    #add new data do respective columns
    covid_alldata.updateColumnData(df=covid_inf_data,
                                   join_field_df='countyFIPS',
                                   join_field_attr='GEOID', 
                                   update_field_attr='INF',
                                   update_field_df=covid_inf_date)
    
    covid_alldata.updateColumnData(df=covid_dth_data,
                               join_field_df='countyFIPS',
                               join_field_attr='GEOID', 
                               update_field_attr='DTH',
                               update_field_df=covid_dth_date)

    #finalize changes
    covid_alldata.pushChanges()

if __name__ == "__main__":
   run_update()
