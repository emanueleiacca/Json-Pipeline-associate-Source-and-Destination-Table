import re

text = """
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
transformations = re.search(r'INNER JOIN.*?(?=;)', text, re.DOTALL).group(0)

aliases = re.findall(r'AS\s+(.*?)\s*$', transformations, re.MULTILINE)

columns = re.findall(r'UNNEST\(ARRAY\[(.*?)\]\)', transformations, re.DOTALL)

for alias, column in zip(aliases, columns):
    print("Alias:", alias)
    unique_values = list(set(re.findall(r'\(([^()]*)\)', column)))
    unique_values = [value.split(',')[0].strip(" '").split(' as ')[0].split(' ', 1)[0] for value in unique_values]
    print("Unique Values:", list(set(unique_values)))  # Use set to remove duplicates
    print()
#no transformation done
