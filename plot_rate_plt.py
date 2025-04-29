import pandas as pd
import glob, os, re
from natsort import natsort_keygen
import numpy as np
import pdb, argparse

import matplotlib 
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure
import more_itertools
import matplotlib.ticker as plticker

# import cmsstyle as CMS
# CMS.SetExtraText("Simulation Preliminary")
# CMS.SetEnergy("13.6")
# CMS.SetLumi("")

import mplhep as hep  # HEP (CMS) extensions/styling on top of mpl
hep.style.use("CMS")
# CMS.SetEnergy("13.6")


parser = argparse.ArgumentParser(description="get info from oms")
# parser.add_argument(
# 	"--year",
# 	choices=['2022','2023', '2024'],
# 	required=True,
# 	help='Specify the year you want to investigate')

parser.add_argument(
	"--path",
	required=False,
	choices=['ditau','etau', 'mutau'],
	default='ditau',
	type=str,
	help='Specify the hlt path')

args = parser.parse_args()
# year = args.year
path_type = args.path

if path_type=='ditau':
  plot_title = 'HLT_DoubleMediumChargedIsoDisplacedPFTauHPS32_Trk1_eta2p1'
elif path_type=='mutau':
  plot_title = 'HLT_DisplacedMu24_MediumChargedIsoDisplacedPFTauHPS24'
elif path_type=='etau':
  plot_title = 'HLT_Photon34_R9Id90_CaloIdL_IsoL_DisplacedIdL_MediumChargedIsoDisplacedPFTauHPS34'


def atoi(text):
    return int(text) if text.isdigit() else text

def natural_keys(text):
    '''
    alist.sort(key=natural_keys) sorts in human order
    http://nedbatchelder.com/blog/200712/human_sorting.html
    (See Toothy's implementation in the comments)
    '''
    return [ atoi(c) for c in re.split(r'(\d+)', text) ]

def concat_n_consecutive(test_list, n):
    # Using the `chunked` method from the `more_itertools` library to split the list into chunks of n elements
    # The `chunked` method returns a list of chunks, each chunk being a list of n elements
    # Using a list comprehension to join the elements of each chunk into a single string
    return [''.join(i) for i in more_itertools.chunked(test_list, n)]
 
# all_eras = ['Z']
# all_eras = ['D', 'E', 'F']

## 2022
# all_eras = ['B_part2','C_part1', 'C_part2', 'C_part3', 'C_part4', \
#             'D_part1', 'D_part2', 'D_part3', 'D_part4', 'E', 'F', 'G']
all_eras = ['B','C', 'D', 'E', 'F', 'G']
year = '2022'

# all_eras = ['B','C', 'D', 'E', 'F', 'G', 'H']
# all_eras = [ 'E', 'F', 'G', 'H']
# 2023
# all_eras = ['B','C', 'D']
# year = '2023'

era_short_string = concat_n_consecutive(all_eras, len(all_eras))[0]
era_short_string = year + era_short_string

files_eras = []
# all_files = glob.glob("data/rate*.csv")
for era in all_eras:
  print ("data/rate_lumi_pu_%s_%s%s.csv"%(path_type,year,era))
  files_eras.append(glob.glob("data/rate_lumi_pu_%s_%s%s.csv"%(path_type,year,era)))
#   print ("data2024/rate_lumi_pu_%s_2024%s.csv"%(path_type,era))
#   files_eras.append(glob.glob("data2024/rate_lumi_pu_%s_2024%s.csv"%(path_type,era)))

pd_eras = []

for iera,era in enumerate(all_eras):
  pd_eras.append(pd.concat((pd.read_csv(f) for f in files_eras[iera]), ignore_index=True))
  pd_eras[iera] =  pd_eras[iera].sort_values( by="run_ls", key=natsort_keygen())
  pd_eras[iera]['rate2e34'] = pd_eras[iera]['rate']/pd_eras[iera]['inst_lumi']*20
  pd_eras[iera]['era'] = year + era
#   pd_eras[iera]['era'] = '2024' + era
  pd_eras[iera] = pd_eras[iera].dropna()
  ## remove super outliers
  pd_eras[iera] = pd_eras[iera].drop(pd_eras[iera][pd_eras[iera]['rate2e34'] > 100].index)
  
df_all = pd.concat(pd_eras)

df_all['run'] = df_all.apply(lambda x: int(x['run_ls'].split(',')[0].replace('(','')), axis=1)
df_all['ls']  = df_all.apply(lambda x: int(x['run_ls'].split(',')[1].replace(')','')), axis=1)
df_all['run_ls_id'] = df_all.run * 1E4 + df_all.ls

## create_fill_run_dict(df):
fill_run_dict = pd.Series(df_all.fill.values,index=df_all.run_ls).to_dict()

fig, ax = plt.subplots(figsize=(12, 6))

for era, d in df_all.groupby('era'):
    plt.scatter(x=d['run_ls'], y=d['rate2e34'], label=era, s=4)
#     plt.scatter(x=d['run_ls_id'], y=d['rate2e34'], label=era, s=5)
    plt.xticks(list(fill_run_dict.keys()), fill_run_dict.values())
    ## manual workaround to properly set the fill ticks at the right position
    loc = plt.xticks()[0]
    vals = plt.xticks()[1]
    fill_list = []
    for itick in vals:
      itext = itick.get_text()
      if itext in fill_list:
        itick.set_text('')
      else:
        fill_list.append(itext)
    plt.xticks(loc, vals)

plt.xticks(fontsize=6)
plt.xticks(rotation=90)
ax.set_ylabel('rate @2E34')
ax.set_xlabel('LHC fill')

ax.set_title(plot_title + ' rate')

plt.legend(handletextpad=0.1)
plt.show()

plt.savefig('norm_rate_vs_fill_new_%s_%s.pdf'%(era_short_string, path_type))
plt.savefig('norm_rate_vs_fill_new_%s_%s.png'%(era_short_string, path_type))




######## plot non-normalized rate ########
plt.clf()
fig, ax = plt.subplots(figsize=(12, 6))

for era, d in df_all.groupby('era'):
    plt.scatter(x=d['run_ls'], y=d['rate'], label=era, s=4)
    plt.xticks(list(fill_run_dict.keys()), fill_run_dict.values())

    ## manual workaround to properly set the fill ticks at the right position
    loc = plt.xticks()[0]
    vals = plt.xticks()[1]
    fill_list = []
    for itick in vals:
      itext = itick.get_text()
      if itext in fill_list:
        itick.set_text('')
      else:
        fill_list.append(itext)
    plt.xticks(loc, vals)

plt.xticks(fontsize=6)
plt.xticks(rotation=90)
ax.set_ylabel('rate')
ax.set_xlabel('LHC fill')
ax.set_title(plot_title + ' rate')

    
plt.legend(handletextpad=0.1)
plt.show()

plt.savefig('rate_vs_fill_new_%s_%s.pdf'%(era_short_string, path_type))
plt.savefig('rate_vs_fill_new_%s_%s.png'%(era_short_string, path_type))


######## plot vs pu ######## 
plt.clf()
fig, ax = plt.subplots()
for era, d in df_all.groupby('era'):
    ## do not split color by era
    plt.scatter(x=d['pileup'], y=d['rate'], label=era, s=10, c='b')
    ## change color by era
#     plt.scatter(x=d['pileup'], y=d['rate'], label=era, s=10, c='b')

ax.set_ylabel('Rate (Hz)')
ax.set_xlabel('PU')
ax.set_ylim(0, 15)
ax.set_xlim(0, 70)
# ax.set_title(plot_title + ' rate', fontsize=8)
hep.cms.label("Preliminary", data = True,  year=year, com=13.6, ax=ax)
# ax.set_title('HLT_DoubleMediumChargedIsoDisplacedPFTauHPS32_Trk1_eta2p1 rate', fontsize=8)
# plt.legend(handletextpad=0.1)
plt.grid(True)
plt.savefig('rate_vs_pileup_new_%s_%s.pdf'%(era_short_string, path_type))
plt.savefig('rate_vs_pileup_new_%s_%s.png'%(era_short_string, path_type))

exit(0)