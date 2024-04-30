import re
import pandas as pd

query = """
SELECT
    NUM_AA_TT AS num_aa_tt,
    COD_KEY_PIAZZA_REGOLAMENTO AS cod_key_piazza_regolamento,
    substr(M1.NOM_PSET, 1, 20) AS cod_key_piazza_regolamento_rel,
    M1.COD_PSET AS cod_piazza_regolamento,
    M1.NOM_PSET AS des_piazza_regolamento,
    substr(concat(substr(CAST(NUM_AA_TT as STRING), 1, 5), '|', coalesce(COD_KEY_PIAZZA_REGOLAMENTO, 'NULL')), 1, 100) AS cod_chiave_dq,
    substr(concat('#COD_SIST_ALIMNTNT#', '   '), 1, 3) AS cod_sist_alimntnt,
    substr('#COD_SSA_PROVNNZ#', 1, 10) AS cod_ssa_provnnz 
FROM
    #$BQ_PROJID_T4777A#.#$BQ_DSNAME_T4777A#.VD_T4_DB_PSET AS M1
CROSS JOIN
    UNNEST(ARRAY[
        CASE
            WHEN CAST(SSCC0D.CW_TD_NORMALIZE_NUMBER(right(left('#TMS_CARICAMENTO#', 6), 2)) as INT64) >= 1
            AND CAST(SSCC0D.CW_TD_NORMALIZE_NUMBER(right(left('#TMS_CARICAMENTO#', 6), 2)) as INT64) <= 3
                THEN CAST(SSCC0D.CW_TD_NORMALIZE_NUMBER(concat(format_date('%Y', date_add(parse_date('%Y%m%d', left('#TMS_CARICAMENTO#', 8)), interval -4 MONTH)), '4')) as INT64)
            WHEN CAST(SSCC0D.CW_TD_NORMALIZE_NUMBER(right(left('#TMS_CARICAMENTO#', 6), 2)) as INT64) >= 4
            AND CAST(SSCC0D.CW_TD_NORMALIZE_NUMBER(right(left('#TMS_CARICAMENTO#', 6), 2)) as INT64) <= 6
                THEN CAST(SSCC0D.CW_TD_NORMALIZE_NUMBER(concat(left('#TMS_CARICAMENTO#', 4), '1')) as INT64)
            WHEN CAST(SSCC0D.CW_TD_NORMALIZE_NUMBER(right(left('#TMS_CARICAMENTO#', 6), 2)) as INT64) >= 7
            AND CAST(SSCC0D.CW_TD_NORMALIZE_NUMBER(right(left('#TMS_CARICAMENTO#', 6), 2)) as INT64) <= 9
                THEN CAST(SSCC0D.CW_TD_NORMALIZE_NUMBER(concat(left('#TMS_CARICAMENTO#', 4), '2')) as INT64)
            WHEN CAST(SSCC0D.CW_TD_NORMALIZE_NUMBER(right(left('#TMS_CARICAMENTO#', 6), 2)) as INT64) >= 10
            AND CAST(SSCC0D.CW_TD_NORMALIZE_NUMBER(right(left('#TMS_CARICAMENTO#', 6), 2)) as INT64) <= 12
                THEN CAST(SSCC0D.CW_TD_NORMALIZE_NUMBER(concat(left('#TMS_CARICAMENTO#', 4), '3')) as INT64)
        END
    ]) AS num_aa_tt
CROSS JOIN
    UNNEST(ARRAY[
        substr(concat(substr(concat('#COD_SIST_ALIMNTNT#', '   '), 1, 3), M1.NOM_PSET), 1, 23)
    ]) AS cod_key_piazza_regolamento;
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
        unique_names = list(set(names))
        for innermost_text in innermost_texts:
            text = re.sub(r'.*\(([^()]*)\).*', r', '.join(unique_names), text)
    updated_columns.append((text, column[1]))

from_clause = re.search(r'FROM\s+(.*?)\s+(?:CROSS JOIN|WHERE|ORDER BY|LIMIT)', query, re.DOTALL).group(1)

table_alias = re.search(r'(.*)\s+AS\s+(.*)', from_clause).groups()

table_name = table_alias[0].strip()
alias = table_alias[1].strip()

updated2_columns = []
for column in updated_columns:
    text = column[0]
    if alias + '.' in text:
        text = text.replace(alias + '.', table_name + '.')
    updated2_columns.append((text, column[1]))

transformations = re.search(r'CROSS JOIN.*?(?=;)', query, re.DOTALL).group(0)

aliases = re.findall(r'AS\s+(.*?)\s*$', transformations, re.MULTILINE)

columns = re.findall(r'UNNEST\(ARRAY\[(.*?)\]\)', transformations, re.DOTALL)

for alias, column in zip(aliases, columns):
    unique_values = list(set(re.findall(r'\(([^()]*)\)', column)))
    unique_values = [value.split(',')[0].strip(" '") for value in unique_values]
    unique_values = list(set(unique_values))
    for i, (text, column_name) in enumerate(updated2_columns):
        if alias == column_name.strip(','):
            updated2_columns[i] = (unique_values[0] + '.', column_name)

data = [column for column in updated2_columns]

df = pd.DataFrame(data, columns=['source_column', 'target_column'])

# Split source column into multiple rows if it contains multiple values
df = df.assign(source_column=df.source_column.str.split(',')).explode('source_column')

# Remove trailing comma from target column
df['target_column'] = df.target_column.str.rstrip(',')

df.to_excel('easy_sql.xlsx', index=False)



