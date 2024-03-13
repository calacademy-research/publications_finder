"""ORCID list for author search"""

import pandas as pd

df = pd.read_csv('data/cas_authorids_v5.csv')
orcids = df[~df['orcid'].isna()]
orcid_list = list(orcids['orcid'])
