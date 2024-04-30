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
    ]) AS num_aa_tt --operations to generate the NUM_AA_TT, riguarda principalmente l'utilizzo di date per generare anno AA e trimestre TT
CROSS JOIN
    UNNEST(ARRAY[
        substr(concat(substr(concat('#COD_SIST_ALIMNTNT#', '   '), 1, 3), M1.NOM_PSET), 1, 23)
    ]) AS cod_key_piazza_regolamento; 
"""

from_clause = re.search(r'FROM\s+(.*?)\s+(?:CROSS JOIN|WHERE|ORDER BY|LIMIT)', text, re.DOTALL).group(1)

table_alias = re.search(r'(.*)\s+AS\s+(.*)', from_clause).groups()

table_name = table_alias[0].strip()
alias = table_alias[1].strip()

print("Table Name:", table_name)
print("Alias:", alias)
