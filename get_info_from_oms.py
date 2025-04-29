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
	"--path",
	required=False,
	choices=['ditau','etau', 'mutau'],
	default='ditau',
	type=str,
	help='Specify the hlt path')

parser.add_argument(
	"--nls",
	required=False,
	default=10,
	type=int,
	help='frequency of query, in LS')

args = parser.parse_args()
year = args.year
path_type = args.path
nLS = args.nls


json_file_dict = {
#   '2022B_part2' : 'Cert_Collisions2022_eraB_355100_355769_Golden_part2.json',
#   '2022C_part1' : 'Cert_Collisions2022_eraC_355862_357482_Golden_part1.json',
#   '2022C_part2' : 'Cert_Collisions2022_eraC_355862_357482_Golden_part2.json',
#   '2022C_part3' : 'Cert_Collisions2022_eraC_355862_357482_Golden_part3.json',
#   '2022C_part4' : 'Cert_Collisions2022_eraC_355862_357482_Golden_part4.json',
#   '2022D_part1' : 'Cert_Collisions2022_eraD_357538_357900_Golden_part1.json',
#   '2022D_part2' : 'Cert_Collisions2022_eraD_357538_357900_Golden_part2.json',
#   '2022D_part3' : 'Cert_Collisions2022_eraD_357538_357900_Golden_part3.json',
#   '2022D_part4' : 'Cert_Collisions2022_eraD_357538_357900_Golden_part4.json',
#   '2022E'       : 'Cert_Collisions2022_eraE_359022_360331_Golden.json',
#   '2022F'       : 'Cert_Collisions2022_eraF_360390_362167_Golden.json',
#   '2022G'       : 'Cert_Collisions2022_eraG_362433_362760_Golden.json',

  '2023B'       : 'Cert_Collisions2023_eraB_366403_367079_Golden.json',
  '2023C'       : 'Cert_Collisions2023_eraC_367095_368823_Golden.json',
  '2023D'       : 'Cert_Collisions2023_eraD_369803_370790_Golden.json',
# 
#   '2024B'       : 'Cert_Collisions2024_eraB_Golden.json',
#   '2024C'       : 'Cert_Collisions2024_eraC_Golden.json',
#   '2024D'       : 'Cert_Collisions2024_eraD_Golden.json',
#   '2024E'       : 'Cert_Collisions2024_eraE_Golden.json',
#   '2024F'       : 'Cert_Collisions2024_eraF_Golden.json',
#   '2024G'       : 'Cert_Collisions2024_378981_386071_golden_eraG.json',
#   '2024H'       : 'Cert_Collisions2024_378981_386071_golden_eraH.json',
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
      run_pu_dict[irun, this_ls] = ls['attributes']['pileup']
  
    print ('filled inst lumi dict')

    ## now fetch rate information per lumisection
    ditau_v = 'v1'
    mutau_v = 'v3'
#     path_name = 'HLT_DoubleMediumDeepTauPFTauHPS35_L2NN_eta2p1_%s'%path_v
    if int(irun) > 355862: ditau_v = 'v2'
    if int(irun) > 359246 : ditau_v = 'v3'
    if int(irun) > 366403 : ditau_v = 'v4'
    if int(irun) > 367661 : ditau_v = 'v5'
    ## from here is 2024
    if int(irun) > 378985 : ditau_v = 'v8'
    if int(irun) > 380306 : 
      ditau_v = 'v9'
      mutau_v = 'v4'
    if int(irun) > 380963 : 
      ditau_v = 'v10'
      mutau_v = 'v5'
    if int(irun) > 382229 : 
      ditau_v = 'v11'
      mutau_v = 'v6'
    if int(irun) > 383811 : 
      ditau_v = 'v12'
      mutau_v = 'v7'

    
    if path_type=='ditau':
      path_name = 'HLT_DoubleMediumChargedIsoDisplacedPFTauHPS32_Trk1_eta2p1_%s'%ditau_v
    elif path_type=='mutau':
      path_name = 'HLT_DisplacedMu24_MediumChargedIsoDisplacedPFTauHPS24_%s'%mutau_v
    elif path_type=='etau':
      path_name = 'HLT_Photon34_R9Id90_CaloIdL_IsoL_DisplacedIdL_MediumChargedIsoDisplacedPFTauHPS34_%s'%mutau_v
    else:
      print ('path type not recognised')
      exit()  
    
    for ils in good_ls_dict[irun][::nLS]:
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
  pu_df   = pd.DataFrame.from_dict(run_pu_dict, orient='index', columns=['pileup'])
  
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

  pu_df.index = pu_df.index.set_names(['run_ls'])
  pu_df.reset_index()
  
  result_tmp  = fill_df.merge(lumi_df   , left_on='run_ls', right_on='run_ls')
  result_tmp2 = rate_df.merge(result_tmp, left_on='run_ls', right_on='run_ls')
  result      = pu_df.merge(result_tmp2, left_on='run_ls', right_on='run_ls')
  result.to_csv('rate_lumi_pu_%s_%s.csv'%(path_type,era), index=True)
  
  # out = result.to_json(orient='index')[1:-1].replace('},{', '} {')
  # with open('rate_lumi_eraD%s.json'%part_str, 'w') as f:
  #   f.write(out)
  
