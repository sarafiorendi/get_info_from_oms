## Retrieve rate information from OMS

Simple scripts to retrieve rate info for specific HLT paths from OMS per LS, together with inst. lumi and pileup. 
It uses the [CMS OMS API python client](https://gitlab.cern.ch/cmsoms/oms-api-client) and requires its installation.

```
python get_info_from_oms.py --year 2024 --path mutau
```

The `--nls` flag allows to retrieve the info every N LS. Default is 10.

To plot the rate as a function of the Fill number or pileup simply use
 ```
 python plot_rate_plt.py
 ```
 