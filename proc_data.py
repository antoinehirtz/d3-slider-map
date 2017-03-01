"""
    - processes original csv file
        - removes duplicate entries
        - adds region codes (iso alpha 3)
        - removes countries w/o matched region code
        - removes rows with nans
    - writes unmapped region names into a yaml file (to add to existing mapping)
"""

import pandas as pd
import yaml
import json

# params
url_mapping = "https://github.com/tomschulze/region-code-mapping/raw/master/region-code-mapping.tsv"

fname_add = "add_regions.yaml"
fname_out = "data/data_sdgs-disaster_proc.json"
fname_in = "data/data_sdgs-disaster.csv"
years = [str(x) for x in range(2000,2014)]
columns = ['Series Code', 'Country or Area', 'SIDS', 'LDC',
           'LLDC'] + years

# load dataframes
df = pd.read_csv(fname_in)
df_mapping = pd.read_csv(url_mapping, sep="\t").loc[:, ['Country', 'alpha-3']]

# select one indicator
df = df.groupby("Series Code").get_group('VC_DSR_MORT')
df = df[columns].drop_duplicates()

# unique countries in unprocessed frame
orig_regions = set(df['Country or Area'].unique())

# add iso codes
df = pd.merge(df, df_mapping,
              left_on='Country or Area', right_on='Country',
              how='inner')

columns.insert(2, 'alpha-3')
df = df[columns]

# melt dataframe and group by year
dfg = pd.melt(df, id_vars=['alpha-3'], value_vars=years, var_name="year") \
            .dropna() \
            .sort_values(by=["alpha-3", "year"]) \
            .groupby("year")

# store data in a dict to create a suitable json string
dict_json = {}
for k,v in dfg:
    v.index=v['alpha-3']
    v = v['value'].to_dict()
    dict_json[k] = v

with open(fname_out, 'w') as f:
    f.write(json.dumps(dict_json))
print "saved data to: '{0}'".format(fname_out)

# check for regions that didnt get an iso code
missing_names = orig_regions.difference(df['Country or Area'].unique())
missing_names = sorted(list(missing_names))

# if a region has not been mapped...
if len(orig_regions) / len(df['Country or Area'].unique()) != 1:

    # store in dict and write to yaml file
    dict_yaml = {}
    for idx, region in enumerate(missing_names):
        dict_yaml['add existing name ' + str(idx)] = [region]

    with open(fname_add, 'w') as f:
        f.write(yaml.dump(dict_yaml))
    print "saved region names w/o code to: '{0}'".format(fname_add)
