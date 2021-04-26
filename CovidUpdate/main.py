from covid_data import *
from agol import *
from arcgis.gis import GIS
from pathlib import Path
import os

item_ids={'COVID DATA':'4512d4375c4d49c0815d606d3bad8930',
          'covid_by_monthyr':'52a8074622d040bd9e4c71a2786f2018',
          'covid_by_2wks':'5c6067e23f4342a7935b33c33651f7a4',
          'pr_indices':'4ae63cc894dc45038619b152462c53cf',
          'covid_inf_monthyr_pr':'f13d99b0be4d442faa6505076c900b29',
          'covid_inf_daily_pr':'0b832fc2fe4e47bb86d91ebe160d458e',
          'covid_inf_4wks_pr':'73eccf144a5f44798fcbd31a493aa896'}

BASE_DIR = os.path.dirname(Path(__file__).resolve())
CSV_PATH=os.path.join(BASE_DIR,'CSV')

def update_stateside_dashboard():
    gis =GIS(username="",password="")
    covid_data=query_COVID(source='usafacts')
    
    #get covid infection counts and death counts from last 2 weeks
    #get covid infection counts and death counts from last 12 months
    covid_by_2wks=covid_data.getCovidData_2wks(us_or_pr='us')
    covid_by_monthyr=covid_data.getCovidData_mthYear(us_or_pr='us')
    
    covid_by_2wks_path=os.path.join(CSV_PATH,'covid_by_2wks.csv')
    covid_by_monthyr_path=os.path.join(CSV_PATH,'covid_by_month.csv')

    covid_by_2wks.to_csv(covid_by_2wks_path,index=False)
    covid_by_monthyr.to_csv(covid_by_monthyr_path,index=False)

    #get existing item containing previous covid infection counts and death counts for last 2 weeks
    #get existing item containing previous covid infection counts and death counts for last 12 months
    covid_by_2wks_item=ItemUpdate(gis,itemid=item_ids['covid_by_2wks'])
    covid_by_monthyr_item=ItemUpdate(gis,itemid=item_ids['covid_by_monthyr'])

    #overwrite covid infection items
    covid_by_2wks_item.overwriteItem(covid_by_2wks_path)
    covid_by_monthyr_item.overwriteItem(covid_by_monthyr_path)

    #get main item on agol "COVID DATA
    covid_alldata=ItemUpdate(gis,itemid=item_ids['COVID DATA'])
    covid_alldata_item=covid_alldata.item

    #get recent data
    covid_recent_data=covid_data.recent_covid_data_us

    #add new data do respective columns
    covid_alldata.updateColumnData(df=covid_recent_data,
                                   join_field_df='fips',
                                   join_field_attr='GEOID', 
                                   update_field_attr='INF',
                                   update_field_df="cases"
                                   )
    
    covid_alldata.updateColumnData(df=covid_recent_data,
                                   join_field_df='fips',
                                   join_field_attr='GEOID', 
                                   update_field_attr='DTH',
                                   update_field_df="deaths"
                                   )

    vaccine_data=getVacineData()
    
    covid_alldata.updateColumnData(df=vaccine_data,
                                   join_field_df='fips_code',
                                   join_field_attr='GEOID', 
                                   update_field_attr='VACIN_PER',
                                   update_field_df='percent_adults_fully'
                                   )

    covid_alldata.updateColumnData(df=vaccine_data,
                                   join_field_df='fips_code',
                                   join_field_attr='GEOID', 
                                   update_field_attr='CVAC',
                                   update_field_df='ability_to_handle_a_covid'
                                   )

    
    #finalize changes
    covid_alldata.pushChanges()

def update_pr_dashboard():
    gis =GIS(username="",password="")
    covid_data=query_COVID(source='nytimes')
    
    pr_inf_path=os.path.join(CSV_PATH,'covid_inf_daily_pr.csv')
    pr_inf_by_4wks_path=os.path.join(CSV_PATH,'covid_inf_4wks_pr.csv')
    pr_inf_by_month_path=os.path.join(CSV_PATH,'covid_inf_monthyr_pr.csv')
    
    pr_inf_data=covid_data.covid_data_query_pr
    pr_inf_by_4wks=covid_data.getCovidData_2wks(add_pr_states=False,us_or_pr='pr')
    pr_inf_by_month=covid_data.getCovidData_mthYear(add_pr_states=False,us_or_pr='pr')
    
    pr_inf_data.to_csv(pr_inf_path,index=False)    
    pr_inf_by_4wks.to_csv(pr_inf_by_4wks_path,index=False)
    pr_inf_by_month.to_csv(pr_inf_by_month_path,index=False)
    
    pr_inf_item=ItemUpdate(gis,itemid=item_ids['covid_inf_daily_pr'])
    pr_inf_by_4wks_item=ItemUpdate(gis,itemid=item_ids['covid_inf_4wks_pr'])
    pr_inf_by_month_item=ItemUpdate(gis,itemid=item_ids['covid_inf_monthyr_pr'])
    
    #overwrite covid infection items
    pr_inf_item.overwriteItem(pr_inf_path)
    pr_inf_by_4wks_item.overwriteItem(pr_inf_by_4wks_path)
    pr_inf_by_month_item.overwriteItem(pr_inf_by_month_path)
    
def run_update():
    update_stateside_dashboard()
    update_pr_dashboard()
##    gis =GIS(username="jhinojos_HC",password="Rudy1949!")
##    
##    inf_by_2wks=covid_data.getCovidData_2wks(cases_or_dths='cases',us_or_pr='pr')
##    inf_by_month=covid_data.getCovidData_mthYear(cases_or_dths='cases',us_or_pr='pr')
##
##    inf_by_2wks_path=os.path.join(CSV_PATH,'covid_inf_2wks_pr.csv')
##    inf_by_month_path=os.path.join(CSV_PATH,'covid_inf_monthyr_pr.csv')
##
##    inf_by_2wks.to_csv(inf_by_2wks_path)
##    inf_by_month.to_csv(inf_by_month_path)
##
##    covid_inf2wks_item=ItemUpdate(gis,itemid=item_ids['covid_inf_2wks'])
##    covid_inf12mths_item=ItemUpdate(gis,itemid=item_ids['covid_inf_monthyr'])
##
##    #overwrite covid infection items
##    covid_inf2wks_item.overwriteItem(inf_by_2wks_path)
##    covid_inf12mths_item.overwriteItem(inf_by_month_path)
    
if __name__ == "__main__":
   run_update()
