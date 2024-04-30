import re
import pandas as pd
query = """
SELECT
    SL2.COD_ABI,
    SL2.NUM_ISTITUTO,
    SL2.COD_TITOLO,
    substr(coalesce(trim(SL2.COD_ENTITA), ''), 1, 5) AS cod_entita
  FROM
    #$BQ_PROJID_A1777A#.#$BQ_DSNAME_A1777A#.VA_A1_HD_ARCHIV_ANAGRA_TITOLI_ON AS SL2
    INNER JOIN #$BQ_PROJID_SSC0#.#$BQ_DSNAME_SSCC0D#.V_VE_SC_GEN_CONST_SSA AS B ON upper(rtrim(B.NOM_VISTA)) = 'VA_A1_HD_ARCHIV_ANAGRA_TITOLI_ON'
     AND upper(rtrim(B.COD_PERIODICITA)) = 'G'
     AND B.NUM_PERIOD_RIF = CAST(SSCC0D.CW_TD_NORMALIZE_NUMBER(left('#TMS_CARICAMENTO#', 8)) as INT64)
     AND (upper(rtrim(B.COD_ABI)) = 'MULTI'
     AND 1 = 1
     OR upper(rtrim(B.COD_ABI)) <> 'MULTI'
     AND upper(rtrim(SL2.COD_ABI)) = upper(rtrim(B.COD_ABI)))
     AND B.TMS_PDC BETWEEN SL2.TMS_INIZIO_VALIDITA AND SL2.TMS_FINE_VALIDITA
  WHERE SL2.FLG_RIFACIMENTO = 0
   AND SL2.TMS_CANC_FISICA IS NULL
   AND upper(trim(SL2.COD_VERSIONE)) = '0';
"""


selected_columns = re.findall(r'SELECT\s+(.*?)\s+FROM', query, re.DOTALL)[0].strip().split('\n')

columns = []
for column in selected_columns:
    column = column.strip()
    if 'AS' in column:
        parts = re.split(r'\s+AS', column)
        column_name = parts[0].strip()
        alias = parts[1].strip()
        columns.append((column_name, alias))
    else:
        columns.append((column, column))

updated_columns = []
for column in columns:
    text = column[0]
    while True:
        innermost_texts = re.findall(r'\(([^()]*)\)', text)
        if not innermost_texts:
            break
        names = [item.split(',')[0].strip().replace("'", '').replace('$', '').split(' as ')[0] for item in innermost_texts]
        unique_names = [name for name in set(names) if not name.startswith('%')]
        for innermost_text in innermost_texts:
            text = re.sub(r'.*\(([^()]*)\).*', r', '.join(unique_names), text)
    updated_columns.append((text, column[1]))

from_clause = re.search(r'FROM\s+(.*?)\s+(?:INNER JOIN|WHERE|ORDER BY|LIMIT)', query, re.DOTALL).group(1)

table_alias = re.search(r'(.*)\s+AS\s+(.*)', from_clause).groups()

table_name = table_alias[0].strip()
alias = table_alias[1].strip()

updated2_columns = []
for column in updated_columns:
    text = column[0]
    if alias + '.' in text:
        text = text.replace(alias + '.', table_name + '.')
    column_name = column[1]
    if alias + '.' in column_name:
        column_name = column_name.replace(alias + '.', table_name + '.')
    updated2_columns.append((text, column_name))

transformations = re.search(r'INNER JOIN.*?(?=;)', query, re.DOTALL).group(0)

aliases = re.findall(r'AS\s+(.*?)\s*$', transformations, re.MULTILINE)

columns = re.findall(r'UNNEST\(ARRAY\[(.*?)\]\)', transformations, re.DOTALL)

for alias, column in zip(aliases, columns):
    unique_values = list(set(re.findall(r'\(([^()]*)\)', column)))
    unique_values = [value.split(',')[0].strip(" '").split(' as ')[0].split(' ', 1)[0] for value in unique_values]
    unique_values = list(set(unique_values))
    for i, (text, column_name) in enumerate(updated2_columns):
        if alias == column_name.strip(','):
            updated2_columns.pop(i)  # Remove the previous entry
            break
    for unique_value in unique_values:
        updated2_columns.append((unique_value + '.', column_name))

data = [column for column in updated2_columns]

df = pd.DataFrame(data, columns=['source_column', 'target_column'])

# Split source column into multiple rows if it contains multiple values
df = df.assign(source_column=df.source_column.str.split(',')).explode('source_column')

# Remove trailing comma from target column
df['target_column'] = df.target_column.str.rstrip(',')

df.to_excel('medium2_sql.xlsx', index=False)


