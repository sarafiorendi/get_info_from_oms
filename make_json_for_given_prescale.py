import csv
import json
from collections import defaultdict
import pdb

desired_prescale = "1p8E34"
input_csv = "prescale_info_2022BCDEFG.csv"
output_json = "lumiJson_2022_{}.json".format(desired_prescale)

# input_csv = "prescale_info_2023BCD.csv"
# output_json = "lumiJson_2023_2p0E34.json"

def process_csv(input_csv, output_json):

  runs_dict = defaultdict(list)

  with open(input_csv, mode="r") as csv_file:
    reader = csv.DictReader(csv_file)
    current_run = None
    current_ls_range = []

    for row in reader:
      run_ls = row["run_ls"].split(",")
      prescale = row["prescale"]

      if prescale == "1p8E34":
        run = int(run_ls[0].replace('(',''))
        ls = int(run_ls[1].replace(')',''))

        if not current_ls_range or current_run != run:
          ## finalize the previous run's LS range
          if current_run is not None:
            runs_dict[current_run].append(current_ls_range)

          current_run = run
          current_ls_range = [ls, ls]
        elif ls == current_ls_range[1] + 1:
          ## continue with the current LS range
          current_ls_range[1] = ls
        else:
          # close the current LS range and start a new one
          runs_dict[current_run].append(current_ls_range)
          current_ls_range = [ls, ls]

    ## add last LS range to dict
    if current_run is not None:
      runs_dict[current_run].append(current_ls_range)

  ## convert ranges for output json
  formatted_output = {
    str(run): [[range_[0], range_[1]] for range_ in ranges]
    for run, ranges in runs_dict.items()
  }

  with open(output_json, mode="w") as json_file:
    json.dump(formatted_output, json_file, indent=4)


process_csv(input_csv, output_json)

