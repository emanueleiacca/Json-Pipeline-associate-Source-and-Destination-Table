import json
from utils import *

with open('SSCC0_FE_060_TW_SSC_RAI_OPERAZIONI_CSDR_TOP_TT_01_JPL.json', 'r') as file:
    pipeline_data = json.load(file)
pipeline_summary_new = summarize_pipeline(pipeline_data)

# print all target tables
target_tables = find_target_tables_from_links(pipeline_summary_new)
print(f"Target Tables: {target_tables}")
