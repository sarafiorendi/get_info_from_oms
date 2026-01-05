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

import numpy as np
import yaml

# import cmsstyle as CMS
# CMS.SetEnergy("13.6")
# CMS.SetLumi("")

import mplhep as hep  # HEP (CMS) extensions/styling on top of mpl
hep.style.use("CMS")
# CMS.SetEnergy("13.6")


parser = argparse.ArgumentParser(description="get info from oms")

parser.add_argument(
	"--path",
	required=False,
	choices=['ditau','etau', 'mutau', 'displphoton', 'diphoton'],
	default='ditau',
	type=str,
	help='Specify the hlt path')

parser.add_argument(
	"--year",
	required=False,
	choices=['2022','2023', '2024', '2025'],
	default='2022',
	type=str,
	help='Specify the year of data taking')

parser.add_argument(
	"--oneprescale",
	required=False,
	default=False,
	help='Specify if you want the plot per one prescale column')

parser.add_argument(
	"--allprescale",
	required=False,
	default=False,
	help='Specify if you want the plots for all prescale columns')

parser.add_argument(
	"--perfill",
	required=False,
	default=True,
	help='Specify if you want the plots vs fill number')

args = parser.parse_args()
year = args.year
path_type = args.path

if path_type=='ditau':
  plot_title = 'HLT_DoubleMediumChargedIsoDisplacedPFTauHPS32_Trk1_eta2p1'
elif path_type=='mutau':
  plot_title = 'HLT_DisplacedMu24_MediumChargedIsoDisplacedPFTauHPS24'
elif path_type=='etau':
  plot_title = 'HLT_Photon34_R9Id90_CaloIdL_IsoL_DisplacedIdL_MediumChargedIsoDisplacedPFTauHPS34'
elif path_type=='displphoton':
  plot_title = 'HLT_Photon60_R9Id90_CaloIdL_IsoL_DisplacedIdL_PFHT350MinPFJet15'
elif path_type=='diphoton':
  plot_title = 'HLT_DiPhoton10Time1ns'


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
 
## 2022
# all_eras = ['B_part2','C_part1', 'C_part2', 'C_part3', 'C_part4', \
#             'D_part1', 'D_part2', 'D_part3', 'D_part4', 'E', 'F', 'G']
# all_eras = ['E', 'F', 'G']
if year == '2022':
  all_eras = ['B','C', 'D', 'E', 'F', 'G']
  if path_type=='ditau':
    all_eras = ['E', 'F', 'G']
if year == '2023':
  all_eras = ['B','C', 'D']
  if path_type=='ditau':
    all_eras = ['D']
if year == '2024':
#   all_eras = ['B','C', 'D', 'E', 'F', 'G', 'H', 'I']
  all_eras = ['C']#, 'D']#, 'E']#, 'F']
if year == '2025':
  all_eras = ['B', 'C', 'Cdcs', 'D']

# 2023
# all_eras = ['D']

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
  pd_eras[iera] = pd_eras[iera].dropna()
  ## remove super outliers
  pd_eras[iera] = pd_eras[iera].drop(pd_eras[iera][pd_eras[iera]['rate2e34'] > 100].index)
  
df_all = pd.concat(pd_eras)

df_all['run'] = df_all.apply(lambda x: int(x['run_ls'].split(',')[0].replace('(','')), axis=1)
df_all['ls']  = df_all.apply(lambda x: int(x['run_ls'].split(',')[1].replace(')','')), axis=1)
df_all['run_ls_id'] = df_all.run * 1E4 + df_all.ls
## create_fill_run_dict(df):
fill_run_dict = pd.Series(df_all.fill.values,index=df_all.run_ls).to_dict()


## plots vs fill number
if args.perfill == True:
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
    
    # ax.set_title(plot_title + ' rate', fontsize = 0.6 )
    ax.set_ylim(0, 40)
    
    plt.legend(handletextpad=0.1)
    plt.savefig('plots_%s/norm_rate_vs_fill_new_%s_%s.pdf'%(path_type, era_short_string, path_type))
    plt.savefig('plots_%s/norm_rate_vs_fill_new_%s_%s.png'%(path_type, era_short_string, path_type))


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
    # ax.set_title(plot_title + ' rate', fontsize = 0.6 )
      
    plt.legend(handletextpad=0.1)
    plt.savefig('plots_%s/rate_vs_fill_new_%s_%s.pdf'%(path_type, era_short_string, path_type))
    plt.savefig('plots_%s/rate_vs_fill_new_%s_%s.png'%(path_type, era_short_string, path_type))


######## plot vs pu ######## 

plt.clf()
fig, ax = plt.subplots()
for era, d in df_all.groupby('era'):
    plt.scatter(x=d['pileup'], y=d['rate'], label=era, s=10)#, c='b')

ax.set_ylabel('HLT rate [Hz]')
ax.set_xlabel('PU')
ax.set_ylim(0, 15)
if path_type=='displphoton' :
  ax.set_ylim(0, 50)
if path_type=='diphoton':
  ax.set_ylim(0, 40)
ax.set_xlim(0, 70)

# hep.cms.label("Preliminary", data = True,  year=year, ax=ax, loc=4)
# hep.cms.label("Preliminary", data = True,  com=13.6, ax=ax, loc=4)
# hep.cms.text(100, ax=ax, loc=4)
## for 2023
if path_type =='ditau' and year == '2023':
  ax.text(43, 15.2, r'9.5 fb$^{-1}$(13.6 TeV)', fontsize=24)
# hep.cms.text('Preliminary, 2023', ax=ax, loc=2)
## for 2022
elif year == '2022':
  if path_type =='displphoton':
    ax.text(43, 50.2, r'26.7 fb$^{-1}$(13.6 TeV)', fontsize=24)
  elif path_type =='diphoton': ## 2024
    ax.text(43, 40.2, r'XX fb$^{-1}$(13.6 TeV)', fontsize=24)
  else:
    ax.text(43, 15.2, r'26.7 fb$^{-1}$(13.6 TeV)', fontsize=24)
# hep.cms.text('Preliminary, {}'.format(year), ax=ax, loc=2)
hep.cms.text('{}'.format(year), ax=ax, loc=2)

plt.legend(handletextpad=0.1)
# plt.grid(True)
plt.savefig('plots_%s/rate_vs_pileup_%s_%s.pdf'%(path_type, era_short_string, path_type))
plt.savefig('plots_%s/rate_vs_pileup_%s_%s.png'%(path_type, era_short_string, path_type))


print (df_all['prescale'].unique())
## skim on prescale column
## df_all['prescale'].unique()
if year =='2022':
  all_possible_prescales = [ 
       '1p60E+34',
       '1p70E+34', '1p50E+34', '1p40E+34', '1p30E+34', '1p20E+34',
       '1p10E+34', '2p00E+34', '9p00E+33',
       '2p00E+34+ZeroBias+HLTPhysics', 
       '2p10E+34', '2p20E+34', 
       '2p0E34',
       '0p9E34', '1p1E34', '1p3E34', '2p0E34+ZeroBias+HLTPhysics',
       '1p2E34', '1p4E34', '1p6E34', '1p5E34', '1p0E34', '0p8E34',
       '0p7E34', 
       '1p8E34', 
       '1p7E34', 
       '0p6E34', 
       '1p9E34', 
       '2p1E34',
       '2p2E34', '2p3E34']

elif year =='2023':
  all_possible_prescales = [ 
       '2p0E34', '1p7E34', '1p1E34', '0p8E34', '0p6E34',
       '0p9E34', '1p0E34', '1p2E34', '1p3E34', '1p5E34', '1p6E34',
       '1p4E34', '0p7E34', '1p8E34', '2p0E34+ZeroBias+HLTPhysics',
       '1p9E34', '2p0E34+HLTPhysics', '2p0E34+ZeroBias', '2p1E34',
       '2p2E34', '2p0E34noHMT', '2p05E34']

elif year =='2024':
  all_possible_prescales = [ 
#       '2p0E34+ZeroBias+HLTPhysics',
#       '2p0E34+HLTPhysics', '2p0E34', 
      '1p8E34', 
#       '1p8E34+ZeroBias', '2p0E34+ZeroBias',
#       '1p8E34+ZeroBias+HLTPhysics', '1p6E34', '1p4E34', '1p2E34', '1p3E34',
      ]

else:
  print ('no list of prescale columns available, breaking')
  exit(0)


if args.allprescale:
  for iprescale in all_possible_prescales:
    print (iprescale)
    
    df_all_tmp = df_all[df_all.prescale == iprescale]
 
    plt.clf()
    fig, ax = plt.subplots()
    for era, d in df_all_tmp.groupby('era'):
        plt.scatter(x=d['pileup'], y=d['rate'], label=era, s=10)#, c='b')
        plt.legend()
  
    ax.set_ylabel('HLT rate [Hz]', fontsize=31)
    ax.set_xlabel('PU', fontsize=31)
    plt.xticks(fontsize=30)  # Change the font size of x-axis tick labels
    plt.yticks(fontsize=30)  # Change the font size of x-axis tick labels
  
  #   plt.rc('xtick', labelsize=40) 
  #   plt.rcParams.update({'font.size': 30})
  
    ax.set_ylim(0, 15)
    if path_type=='displphoton':
      ax.set_ylim(0, 50)
    if path_type=='diphoton':
      ax.set_ylim(0, 40)
    ax.set_xlim(0, 70)
    if year == '2022':
      if path_type !='displphoton':
        ax.text(38, 15.2, r' fb$^{-1}$(13.6 TeV)', fontsize=30)
      elif iprescale == '1p8E34':
        ax.text(38, 50.5, r'10.6 fb$^{-1}$(13.6 TeV)', fontsize=30)
    elif year == '2023':
      if iprescale == '2p0E34':
        ax.text(39, 50.3, r'3.1 fb$^{-1}$(13.6 TeV)', fontsize=30)
    elif year == '2024':
      if path_type =='diphoton' and iprescale == '1p8E34':
        ax.set_ylim(0, 15)
        ax.text(37, 15.15, r'14.2 fb$^{-1}$(13.6 TeV)', fontsize=30, fontweight='normal')
    hep.cms.text('{}'.format(year), ax=ax, loc=2)
  #   hep.cms.text('Preliminary, {}'.format(year), ax=ax, loc=2)
  
    # ax.text(6, 30, r'prescale column {}'.format(iprescale), fontsize=20)
    # plt.legend(handletextpad=0.1)
#     plt.grid(True)
    plt.savefig('plots_%s/rate_vs_pileup_new_%s_%s_ps%s.pdf'%(path_type, era_short_string, path_type, iprescale))
    plt.savefig('plots_%s/rate_vs_pileup_new_%s_%s_ps%s.png'%(path_type, era_short_string, path_type, iprescale))



   ### try color per RUN
    if path_type !='diphoton' and year != '2024':
      continue

    df_all_tmp = df_all[df_all.prescale == iprescale]
  
    plt.clf()
    fig, ax = plt.subplots()
 
    for fill, d in df_all_tmp.groupby('fill'):
        if fill <= 9570:  continue
        all_x = []
        all_y = []
        all_fill = []

        plt.scatter(x=d['pileup'], y=d['rate'], label='Fill ' + str(fill), s=12)
        plt.legend(loc='upper center', bbox_to_anchor=(0.45, 1.0), handletextpad=0.1)

        ### save to csv for HepData
        ## write YAML file per fill
        all_x.extend(d['pileup'].tolist())
        all_y.extend(d['rate'].tolist())
        all_fill.extend([fill] * len(d))
        
        print (fill, len(d), len(all_x), len(all_fill), len(d['pileup']))

        hepdata_table = {
            "independent_variables": [
                {
                    "header": {"name": "pileup"},
                    "values": [{"value": float(v)} for v in all_x]
                }
            ],
            "dependent_variables": [
                {
                    "header": {"name": "rate"},
                    "values": [{"value": float(v)} for v in all_y]
                },
                {
                    "header": {"name": "fill"},
                    "values": [{"value": int(v)} for v in all_fill]
                }
            ]
        }
        print ('\n', len(all_fill), len(all_x), len(all_y))
        
        with open(f"/Users/sara/Desktop/displacedTaus/plots_for_llp_paper/hepData/HepData_EXO-23-016/data_Sara/hepdata_{path_type}_rate_{year}_fill{fill}.yaml", "w") as f:
            yaml.dump(hepdata_table, f, sort_keys=False)
  
        plt.savefig('plots_%s/rate_vs_pileup_new_perFill_lastFour_%s_%s_ps%s_onlyFill%s.pdf'%(path_type, era_short_string, path_type, iprescale, fill))
    ax.set_ylabel('HLT rate [Hz]', fontsize=31)
    ax.set_xlabel('PU', fontsize=31)
    plt.xticks(fontsize=30)  # Change the font size of x-axis tick labels
    plt.yticks(fontsize=30)  # Change the font size of x-axis tick labels
  
    ax.set_xlim(0, 70)
    ax.set_ylim(0, 10)
    ax.text(23, 10.12, r'0.677 fb$^{-1}$ (2024) (13.6 TeV)', fontsize=30, fontweight='normal')
#       ax.text(37, 15.15, r'14.2 fb$^{-1}$(13.6 TeV)', fontsize=30, fontweight='normal')
    hep.cms.text('', ax=ax, loc=2)
#     hep.cms.text('{}'.format(year), ax=ax, loc=2)
  
#     plt.grid(True)
    plt.savefig('plots_%s/rate_vs_pileup_new_perFill_lastFour_%s_%s_ps%s.pdf'%(path_type, era_short_string, path_type, iprescale))
    plt.savefig('plots_%s/rate_vs_pileup_new_perFill_lastFour_%s_%s_ps%s.png'%(path_type, era_short_string, path_type, iprescale))


'''
fills era C
9539
9543
9548
9559
9562
9564
9565
9566
9567
9568
9569
9570
9573
9574
9575
9579
'''



       
exit(0)