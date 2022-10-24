from omsapi import OMSAPI
import pandas as pd
import json, time
import pdb

my_app_id='hlt_counts_and_lumi'
my_app_secret='be9e7ffe-854c-46e1-afb0-c663baea504f'
omsapi = OMSAPI("https://cmsoms.cern.ch/agg/api", "v1", cert_verify=False)
omsapi.auth_oidc(my_app_id,my_app_secret)

# part_str = '_part4'
# era = 'eraC_part1'

json_file_dict = {
  'eraB_part1'       : 'Cert_Collisions2022_eraB_355100_355769_Golden_part1.json',
  'eraB_part2'       : 'Cert_Collisions2022_eraB_355100_355769_Golden_part2.json',
  'eraC_part1'       : 'Cert_Collisions2022_eraC_355862_357482_Golden_part1.json',
  'eraC_part2'       : 'Cert_Collisions2022_eraC_355862_357482_Golden_part2.json',
  'eraC_part3'       : 'Cert_Collisions2022_eraC_355862_357482_Golden_part3.json',
  'eraD_part1' : 'Cert_Collisions2022_eraD_357538_357900_Golden_part1.json',
  'eraD_part2' : 'Cert_Collisions2022_eraD_357538_357900_Golden_part2.json',
  'eraD_part3' : 'Cert_Collisions2022_eraD_357538_357900_Golden_part3.json',
  'eraD_part4' : 'Cert_Collisions2022_eraD_357538_357900_Golden_part4.json',
  'eraE'       : 'Cert_Collisions2022_eraE_359022_360331_Golden.json',
}

for era in json_file_dict.keys():
  ## read physics json file
  with open(json_file_dict[era]) as f: 
    json_data = json.load(f)
  
  run_lumi_dict = {} 
  run_rate_dict = {} 
  
  good_ls_dict = {}
  for i,iirun in enumerate(json_data.keys()):
    irun = int(iirun)
  #   if i > 0:  continue
    good_ls_dict[irun] = []
    for irange in json_data[iirun]:
      this_good_ls_list = [j for j in range(irange[0],irange[1])]
      good_ls_dict[irun].extend(this_good_ls_list)
  
  print ('json %s loaded' %json_file_dict[era])
      
  run_lumi_dict = {}
  run_rate_dict = {}
  l1_name = 'L1_DoubleIsoTau32er2p1'

  # retrieve inst lumi
  ls_query = omsapi.query("lumisections")
  ls_query.verbose = False
  # retrieve the rate
  rate_query = omsapi.query("l1algorithmtriggers")
  rate_query.per_page = 1000  # to get all names in one go
  rate_query.attrs(["name","bit","pre_dt_before_prescale_rate"])
  # retrieve the trigger bit
  algo_query = omsapi.query("l1algorithmtriggers")
  algo_query.verbose = False
  algo_query.per_page = 2000  # to get all names in one go
  algo_query.attrs(["name","bit"])
  
  for irun in good_ls_dict.keys():
    print ('now working on run', irun)
  
    ## first fetch inst. lumi information per lumisection
    ls_query.clear_filter().filter("run_number", irun)  # returns data per lumisection
    resp = ls_query.paginate(page=1, per_page=2000).data()
    lumi_list = resp.json()['data']                    
  
    ## fill dictionary with inst. lumi information per lumisection 
    for ls in lumi_list:
      this_ls = ls['attributes']['lumisection_number']
      if this_ls not in good_ls_dict[irun]:  continue
      run_lumi_dict[irun, this_ls] = ls['attributes']['init_lumi']
  
    print ('filled inst lumi dict')
  
    ## then get the trigger bit L1
    algo_query.clear_filter().filter("run_number", irun )
    
    # Execute query and fetch data
    the_trigger_bit = -1
    
    algo_resp = algo_query.data()
    algo_data = algo_resp.json()['data']
    for row in algo_data:
        algo = row['attributes']
        if algo['name'] == l1_name:
            the_trigger_bit = ( algo['bit'] )

    if (the_trigger_bit < 0):
      print ('l1 algo not found in run %s!!'%irun)
      continue
    rate_query.clear_filter().filter("run_number", irun).filter('bit', the_trigger_bit).custom('group[granularity]','lumisection')
    rate_resp = rate_query.paginate(page=1, per_page=1000).data()
    try:
      rate_list = rate_resp.json()['data'] ## list
    except:
      pass  
  
    for ils in rate_list:
      if int(ils['id'].split('__')[-1]) in good_ls_dict[irun]:
        this_ls = int(ils['id'].split('__')[-1])
        run_rate_dict[irun, this_ls] = ils['attributes']['pre_dt_before_prescale_rate']
      
  lumi_df = pd.DataFrame.from_dict(run_lumi_dict, orient='index', columns=['inst_lumi'])
  rate_df = pd.DataFrame.from_dict(run_rate_dict, orient='index', columns=['rate'])

  lumi_df.index = lumi_df.index.set_names(['run_ls'])
  lumi_df.reset_index()

  rate_df.index = rate_df.index.set_names(['run_ls'])
  rate_df.reset_index()

  result = rate_df.merge(lumi_df, left_on='run_ls', right_on='run_ls')
  result.to_csv('l1_rate_lumi_%s.csv'%(era), index=True)
  
