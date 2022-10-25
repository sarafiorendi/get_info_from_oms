from omsapi import OMSAPI
import pandas as pd
import json, time
import pdb

my_app_id='hlt_counts_and_lumi'
my_app_secret='be9e7ffe-854c-46e1-afb0-c663baea504f'
omsapi = OMSAPI("https://cmsoms.cern.ch/agg/api", "v1", cert_verify=False)
omsapi.auth_oidc(my_app_id,my_app_secret)

# part_str = '_part4'
# era = 'eraE'

json_file_dict = {
  'eraB_part1' : 'Cert_Collisions2022_eraB_355100_355769_Golden_part1.json',
  'eraB_part2' : 'Cert_Collisions2022_eraB_355100_355769_Golden_part2.json',
  'eraC_part1' : 'Cert_Collisions2022_eraC_355862_357482_Golden_part1.json',
  'eraC_part2' : 'Cert_Collisions2022_eraC_355862_357482_Golden_part2.json',
  'eraC_part3' : 'Cert_Collisions2022_eraC_355862_357482_Golden_part3.json',
  'eraD_part1' : 'Cert_Collisions2022_eraD_357538_357900_Golden_part1.json',
  'eraD_part2' : 'Cert_Collisions2022_eraD_357538_357900_Golden_part2.json',
  'eraD_part3' : 'Cert_Collisions2022_eraD_357538_357900_Golden_part3.json',
  'eraD_part4' : 'Cert_Collisions2022_eraD_357538_357900_Golden_part4.json',
  'eraE'       : 'Cert_Collisions2022_eraE_359022_360331_Golden.json',
}


for era in json_file_dict.keys():
## read physics json file
# with open('Cert_Collisions2022_%s_357538_357900_Golden%s.json'%(era,part_str)) as f:
# with open('Cert_Collisions2022_eraB_355100_355769_Golden.json') as f:
  with open(json_file_dict[era]) as f: 
    json_data = json.load(f)

  run_lumi_dict = {} 
  run_rate_dict = {} 
  
  good_ls_dict = {}
  for i,iirun in enumerate(json_data.keys()):
    irun = int(iirun)
    good_ls_dict[irun] = []
    for irange in json_data[iirun]:
      this_good_ls_list = [j for j in range(irange[0],irange[1])]
      good_ls_dict[irun].extend(this_good_ls_list)
  
  print ('---------- json %s loaded -----------------------------------' %json_file_dict[era])
      
  ls_query = omsapi.query("lumisections")
  rate_query = omsapi.query("hltpathrates")
  
  run_fill_dict = {}
  run_lumi_dict = {}
  run_rate_dict = {}
  
  for irun in good_ls_dict.keys():
    print ('now working on run', irun)
    ## first fetch inst. lumi information per lumisection
    ls_query.clear_filter().filter("run_number", irun)  # returns data per lumisection
    resp = ls_query.paginate(page=1, per_page=2000).data()
    lumi_list = resp.json()['data']                     ## this should be a list of dictionaries
  
    ## fill dictionary with inst. lumi information per lumisection 
    for ls in lumi_list:
      this_ls = ls['attributes']['lumisection_number']
      if this_ls not in good_ls_dict[irun]:  continue
      run_lumi_dict[irun, this_ls] = ls['attributes']['init_lumi']
      run_fill_dict[irun, this_ls] = ls['attributes']['fill_number']
  
    print ('filled inst lumi dict')
  
  
    ## now fetch rate information per lumisection
    path_v = 'v1'
  #   if int(irun) > 355862: path_v = 'v2'
#     if int(irun) > 359246 : path_v = 'v2'
#     path_name = 'HLT_DoubleMediumDeepTauPFTauHPS35_L2NN_eta2p1_%s'%path_v
    if int(irun) > 355862: path_v = 'v2'
    if int(irun) > 359246 : path_v = 'v3'
    path_name = 'HLT_DoubleMediumChargedIsoDisplacedPFTauHPS32_Trk1_eta2p1_%s'%path_v
    
    
    for ils in good_ls_dict[irun][::10]:
      rate_query.clear_filter().filter("run_number", irun).filter('first_lumisection_number', ils).filter('path_name', path_name)#.custom("group[size]", 20)
      rate_resp = rate_query.paginate(page=1, per_page=10).data()
      time.sleep(0.5)
      try:
        rate_list = rate_resp.json()['data'] ## list
      except:
        pass  
  
      ## fill dictionary with rate information per lumisection 
      for ipath in rate_list:
        if ipath['attributes']['path_name'] == path_name:
          this_ls = ipath['attributes']['last_lumisection_number']
          run_rate_dict[irun, this_ls] = ipath['attributes']['rate']
          
  
  lumi_df = pd.DataFrame.from_dict(run_lumi_dict, orient='index', columns=['inst_lumi'])
  rate_df = pd.DataFrame.from_dict(run_rate_dict, orient='index', columns=['rate'])
  fill_df = pd.DataFrame.from_dict(run_fill_dict, orient='index', columns=['fill'])
  
  lumi_df.index = lumi_df.index.set_names(['run_ls'])
  lumi_df.reset_index()
  # lumi_df = lumi_df.reset_index().rename(columns={lumi_df.index.name:'run_ls'})
  # lumi_df.rename(columns={"index": "run_ls"})
  
  rate_df.index = rate_df.index.set_names(['run_ls'])
  rate_df.reset_index()
  # rate_df = rate_df.reset_index().rename(columns={rate_df.index.name:'run_ls'})
  # rate_df.rename(columns={"index": "run_ls"})

  fill_df.index = fill_df.index.set_names(['run_ls'])
  fill_df.reset_index()
  
  result_tmp = fill_df.merge(lumi_df, left_on='run_ls', right_on='run_ls')
  result = rate_df.merge(result_tmp, left_on='run_ls', right_on='run_ls')
  result.to_csv('rate_lumi_%s.csv'%(era), index=True)
#   result.to_csv('rate_lumi_%s_prompt_ditau.csv'%(era), index=True)
  
  # out = result.to_json(orient='index')[1:-1].replace('},{', '} {')
  # with open('rate_lumi_eraD%s.json'%part_str, 'w') as f:
  #   f.write(out)
  
