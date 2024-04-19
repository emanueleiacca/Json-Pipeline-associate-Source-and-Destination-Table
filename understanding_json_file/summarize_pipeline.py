import json
from utils import *
import pandas as pd
with open('SSCC0_FE_060_TW_SSC_RAI_OPERAZIONI_CSDR_TOP_TT_01_JPL.json', 'r') as file:
    pipeline_data_new = json.load(file)

# Summarize the json data
pipeline_summary_new = summarize_pipeline(pipeline_data_new)
print(pipeline_summary_new)

