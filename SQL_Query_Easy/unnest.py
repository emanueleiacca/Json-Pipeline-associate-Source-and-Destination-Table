import re

text = """
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
transformations = re.search(r'CROSS JOIN.*?(?=;)', text, re.DOTALL).group(0)

aliases = re.findall(r'AS\s+(.*?)\s*$', transformations, re.MULTILINE)

columns = re.findall(r'UNNEST\(ARRAY\[(.*?)\]\)', transformations, re.DOTALL)

for alias, column in zip(aliases, columns):
    print("Alias:", alias)
    unique_values = list(set(re.findall(r'\(([^()]*)\)', column)))
    unique_values = [value.split(',')[0].strip(" '") for value in unique_values]
    print("Unique Values:", list(set(unique_values)))  # Use set to remove duplicates
    print()

