import re

text = """
SELECT
    NUM_AA_MM_GG AS num_aa_mm_gg,
    COD_KEY_SALDI_TITOLI AS cod_key_saldi_titoli,
    M.COD_ABI AS cod_abi,
    M.COD_AGENZIA AS cod_agenzia,
    M.COD_DEPOSITO AS cod_deposito,
    M.COD_DIVISA AS cod_divisa,
    M.IMP_GIACENZA_CONTABILE AS imp_giacenza_contabile,
    M.NUM_ISTITUTO AS num_istituto,
    M.IMP_MOLTIPLICATORE AS imp_moltiplicatore,
    M.IMP_MOLTIPLICATORE_9_DEC AS imp_moltiplicatore_9_decimali,
    M.DAT_RIFERIMENTO AS dat_riferimento,
    M.IMP_CONTROVALORE_IN_EURO AS imp_saldo_titolo_controvalore,
    M.COD_SOTTODEPOSITO AS cod_sottodeposito,
    M.COD_TITOLO AS cod_titolo,
    M.COD_FILIALE AS cod_uo,
    substr(concat(substr(trim(format('%11d', NUM_AA_MM_GG)), 1, 8), '|', trim(coalesce(COD_KEY_SALDI_TITOLI, 'NULL'))), 1, 100) AS cod_chiave_dq,
    substr(concat('#COD_SIST_ALIMNTNT#', '   '), 1, 3) AS cod_sist_alimntnt,
    substr('#COD_SSA_PROVNNZ#', 1, 10) AS cod_ssa_provnnz,
    M.IMP_GIACENZA_REGISTRAZIONE,
    M.IMP_GIACENZA_VALUTA
  FROM
    #$BQ_PROJID_A1777A#.#$BQ_DSNAME_A1777A#.VE_A1_HD_ARCH_DATA_HOU_SAL_GG_ON AS M
    INNER JOIN #$BQ_PROJID_SSC0#.#$BQ_DSNAME_SSCC0D#.V_VE_SC_GEN_CONST_SSA AS B ON upper(rtrim(B.NOM_VISTA)) = 'VE_A1_HD_ARCH_DATA_HOU_SAL_GG_ON'
     AND upper(rtrim(B.COD_PERIODICITA)) = 'G'
     AND B.NUM_PERIOD_RIF = CAST(SSCC0D.CW_TD_NORMALIZE_NUMBER(left('#TMS_CARICAMENTO#', 8)) as INT64)
     AND (upper(rtrim(B.COD_ABI)) = 'MULTI'
     AND 1 = 1
     OR upper(rtrim(B.COD_ABI)) <> 'MULTI'
     AND upper(rtrim(M.COD_ABI)) = upper(rtrim(B.COD_ABI)))
     AND B.TMS_PDC BETWEEN M.TMS_INIZIO_VALIDITA AND M.TMS_FINE_VALIDITA
    LEFT OUTER JOIN #$BQ_PROJID_SSC0#.#$BQ_DSNAME_SSCC0D#.V_VD_SC_GEN_ABI AS SL1 ON upper(rtrim(M.COD_ABI)) = upper(rtrim(SL1.COD_ABI))    
     AND M.NUM_ISTITUTO = CAST(SSCC0D.CW_TD_NORMALIZE_NUMBER(SL1.COD_CONTST) as INT64)
     AND SL1.DAT_FIN_ATTVT_ABI < date_add(parse_date('%Y%m%d', substr(SSCC0D.CW_TD_NORMALIZE_NUMBER(left('#TMS_CARICAMENTO#', 8)), 1, 8)), interval -2 MONTH)
    CROSS JOIN UNNEST(ARRAY[
      CAST(SSCC0D.CW_TD_NORMALIZE_NUMBER(substr('#TMS_CARICAMENTO#', 1, 8)) as INT64)
    ]) AS num_aa_mm_gg
    CROSS JOIN UNNEST(ARRAY[
      substr(concat(trim('#COD_SIST_ALIMNTNT#'), '-', trim(M.COD_ABI), '-', trim(substr(CAST(M.NUM_ISTITUTO as STRING), 1, 5)), '-', trim(M.COD_AGENZIA), '-', trim(M.COD_FILIALE), '-', trim(M.COD_DEPOSITO), '-', trim(M.COD_SOTTODEPOSITO), '-', trim(M.COD_TITOLO), '-', trim(M.COD_DIVISA)), 1, 70)
    ]) AS cod_key_saldi_titoli
  WHERE M.DAT_RIFERIMENTO = parse_date('%Y%m%d', substr('#TMS_CARICAMENTO#', 1, 8))
   AND M.FLG_RIFACIMENTO = 0
   AND M.TMS_CANC_FISICA IS NULL
   AND SL1.COD_ABI IS NULL;
"""

from_clause = re.search(r'FROM\s+(.*?)\s+(?:CROSS JOIN|WHERE|ORDER BY|LIMIT)', text, re.DOTALL).group(1)

table_alias = re.search(r'(.*)\s+AS\s+(.*)', from_clause).groups()

table_name = table_alias[0].strip()
alias = table_alias[1].strip()

print("Table Name:", table_name)
print("Alias:", alias)
