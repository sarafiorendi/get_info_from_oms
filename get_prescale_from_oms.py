from omsapi import OMSAPI
import pandas as pd
import json, time
import pdb, argparse

my_app_id='hlt_counts_and_lumi'
my_app_secret='be9e7ffe-854c-46e1-afb0-c663baea504f'
omsapi = OMSAPI("https://cmsoms.cern.ch/agg/api", "v1", cert_verify=False)
omsapi.auth_oidc(my_app_id,my_app_secret)

parser = argparse.ArgumentParser(description="get info from oms")
parser.add_argument(
	"--year",
	choices=['2022','2023', '2024'],
	required=True,
	help='Specify the year you want to investigate')

parser.add_argument(
	"--nls",
	required=False,
	default=10,
	type=int,
	help='frequency of query, in LS')

args = parser.parse_args()
year = args.year
nLS = args.nls


json_file_dict = {
  '2022B'       : 'Cert_Collisions2022_eraB_355100_355769_Golden.json',
  '2022C'       : 'Cert_Collisions2022_eraC_355862_357482_Golden.json',
  '2022D'       : 'Cert_Collisions2022_eraD_357538_357900_Golden.json',
  '2022E'       : 'Cert_Collisions2022_eraE_359022_360331_Golden.json',
  '2022F'       : 'Cert_Collisions2022_eraF_360390_362167_Golden.json',
  '2022G'       : 'Cert_Collisions2022_eraG_362433_362760_Golden.json',

  '2023B'       : 'Cert_Collisions2023_eraB_366403_367079_Golden.json',
  '2023C'       : 'Cert_Collisions2023_eraC_367095_368823_Golden.json',
  '2023D'       : 'Cert_Collisions2023_eraD_369803_370790_Golden.json',

  '2024B'       : 'Cert_Collisions2024_eraB_Golden.json',
  '2024C'       : 'Cert_Collisions2024_eraC_Golden.json',
  '2024D'       : 'Cert_Collisions2024_eraD_Golden.json',
  '2024E'       : 'Cert_Collisions2024_eraE_Golden.json',
  '2024F'       : 'Cert_Collisions2024_eraF_Golden.json',
  '2024G'       : 'Cert_Collisions2024_378981_386071_golden_eraG.json',
  '2024H'       : 'Cert_Collisions2024_378981_386071_golden_eraH.json',
}


for era in json_file_dict.keys():
  if year not in era:  continue
  ## read physics json file
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
  run_pu_dict = {}
  run_prescale_dict = {}
  run_psindex_dict = {}
  
  for irun in good_ls_dict.keys():
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
      run_pu_dict[irun, this_ls] = ls['attributes']['pileup']
      run_prescale_dict[irun, this_ls] = ls['attributes']['prescale_name']
      run_psindex_dict[irun, this_ls] = ls['attributes']['prescale_index']
  
  lumi_df = pd.DataFrame.from_dict(run_lumi_dict, orient='index', columns=['inst_lumi'])
  fill_df = pd.DataFrame.from_dict(run_fill_dict, orient='index', columns=['fill'])
  pu_df   = pd.DataFrame.from_dict(run_pu_dict, orient='index', columns=['pileup'])
  ps_df   = pd.DataFrame.from_dict(run_prescale_dict, orient='index', columns=['prescale'])
  idx_df  = pd.DataFrame.from_dict(run_psindex_dict, orient='index', columns=['prescale_index'])
  
  lumi_df.index = lumi_df.index.set_names(['run_ls'])
  lumi_df.reset_index()

  fill_df.index = fill_df.index.set_names(['run_ls'])
  fill_df.reset_index()

  pu_df.index = pu_df.index.set_names(['run_ls'])
  pu_df.reset_index()

  ps_df.index = ps_df.index.set_names(['run_ls'])
  ps_df.reset_index()

  idx_df.index = idx_df.index.set_names(['run_ls'])
  idx_df.reset_index()
  
  result_tmp  = fill_df.merge(lumi_df   , left_on='run_ls', right_on='run_ls')
  result_tmp3 = pu_df.merge(result_tmp, left_on='run_ls', right_on='run_ls')
  result_tmp4 = ps_df.merge(result_tmp3, left_on='run_ls', right_on='run_ls')
  result      = idx_df.merge(result_tmp4, left_on='run_ls', right_on='run_ls')
  result.to_csv('prescale_info_%s.csv'%(era), index=True)
