import re

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
   AND upper(trim(SL2.COD_VERSIONE)) = '0';   """


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

for column in updated_columns:
    print(column)
