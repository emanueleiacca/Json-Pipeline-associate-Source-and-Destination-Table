import re

text = """
SELECT

    NUM_AA_TT AS num_aa_tt,

    DAT_INIZIO_TT AS dat_inizio_tt,

    DAT_FINE_TT AS dat_fine_tt,

    MSTR.COD_ABI,

    MSTR.COD_ABI_LEGACY,

    substr(CAST(CASE

       upper(rtrim(substr(MSTR.COD_TITOLO_INTERNO, 7, 2)))

      WHEN '40' THEN CAST(SSCC0D.CW_TD_NORMALIZE_NUMBER(MSTR.COD_TITOLO_INTERNO) as INT64) - 40

      WHEN '46' THEN CAST(SSCC0D.CW_TD_NORMALIZE_NUMBER(MSTR.COD_TITOLO_INTERNO) as INT64) - 38

      WHEN '47' THEN CAST(SSCC0D.CW_TD_NORMALIZE_NUMBER(MSTR.COD_TITOLO_INTERNO) as INT64) - 38

      WHEN '49' THEN CAST(SSCC0D.CW_TD_NORMALIZE_NUMBER(MSTR.COD_TITOLO_INTERNO) as INT64) - 42

      ELSE CAST(SSCC0D.CW_TD_NORMALIZE_NUMBER(MSTR.COD_TITOLO_INTERNO) as INT64)

    END as STRING), 1, 15) AS cod_titolo_interno,

    MSTR.DES_DEPOSITO_CONTROPARTE,

    MSTR.DES_DEPOSITO_ORDINANTE,

    MSTR.COD_DISPOSIZIONE,

    MSTR.COD_DISPOSIZIONE_COLLEGATA,

    MSTR.COD_DIVISA_ISO_CONTROVALORE,

    MSTR.COD_TIPO_AGGREGAZIONE,

    MSTR.DAT_ESD,

    MSTR.DAT_ISD,

    MSTR.COD_ISIN,

    MSTR.DES_LEI,

    MSTR.DAT_MATCHING,

    substr(coalesce(format_date('%Y%m%d', MSTR.DAT_MATCHING), '00010101'), 1, 8) AS dat_matching_char,

    MSTR.COD_NDG_CONTROPARTE,

    MSTR.COD_NDG_ORDINANTE,

    MSTR.VAL_NOMINALE,

    MSTR.DES_PIAZZA_DEPOSITARIA,

    MSTR.DAT_RIFERIMENTO,

    MSTR.COD_SEGNO_TITOLO,

    MSTR.COD_SEGNO_CONTROVALORE,

    MSTR.COD_SSA_PROVNNZ_ORIGNR,

    MSTR.COD_STATO_DISPOSIZIONE,

    MSTR.COD_TIPO_TRANSAZIONE,

    MSTR.DAT_TRADE,

    substr(MSTR.DES_PIAZZA_REGOLAMENTO, 1, 20) AS des_piazza_regolamento,

    MSTR.COD_SIST_ALIMNTNT,

    MSTR.COD_SSA_PROVNNZ

  FROM

    #$BQ_PROJID_SSC0#.#$BQ_DSNAME_SSCC0D#.TE_SC_RAI_MOVIMENTO_CSDR_TOP_TT AS MSTR

            CROSS JOIN UNNEST(ARRAY[

      CAST(SSCC0D.CW_TD_NORMALIZE_NUMBER(CASE

        WHEN div(CAST(SSCC0D.CW_TD_NORMALIZE_NUMBER(substr('#TMS_CARICAMENTO#', 5, 2)) as INT64) - 1, 3) = 0 THEN format('%#40.0f', (CAST(SSCC0D.CW_TD_NORMALIZE_NUMBER(substr('#TMS_CARICAMENTO#', 1, 4)) as BIGNUMERIC) - 1) * 10 + 4)

        ELSE concat(substr('#TMS_CARICAMENTO#', 1, 4), CAST(div(CAST(SSCC0D.CW_TD_NORMALIZE_NUMBER(substr('#TMS_CARICAMENTO#', 5, 2)) as INT64) - 1, 3) as STRING))

      END) as INT64)

    ]) AS num_aa_tt

    CROSS JOIN UNNEST(ARRAY[

      parse_date('%Y%m%d', concat(left(CAST(NUM_AA_TT as STRING), 4), CASE

         rtrim(right(CAST(NUM_AA_TT as STRING), 1))

        WHEN '1' THEN '0101'

        WHEN '2' THEN '0401'

        WHEN '3' THEN '0701'

        ELSE '1001'

      END))

    ]) AS dat_inizio_tt

    CROSS JOIN UNNEST(ARRAY[

      last_day(date_add(DAT_INIZIO_TT, interval 2 MONTH))

    ]) AS dat_fine_tt

    LEFT OUTER JOIN #$BQ_PROJID_SSC0#.#$BQ_DSNAME_SSCC0D#.V_VD_SC_GEN_ABI AS A ON upper(rtrim(A.COD_ABI)) = upper(rtrim(MSTR.COD_ABI))     

     AND A.DAT_FIN_ATTVT_ABI < parse_date('%Y%m%d', concat(left(CAST(NUM_AA_TT as STRING), 4), CASE

       rtrim(right(CAST(NUM_AA_TT as STRING), 1))

      WHEN '1' THEN '0101'

      WHEN '2' THEN '0401'

      WHEN '3' THEN '0701'

      ELSE '1001'

    END))

  WHERE A.COD_ABI IS NULL

   AND MSTR.FLG_RECORD_VALIDO = 1

   AND DATE(MSTR.TMS_ULTIMO_AGGRNMNT_LEGACY) <= DAT_FINE_TT
"""
transformations = re.search(r'CROSS JOIN.*?(?=;)', text, re.DOTALL).group(0)

aliases = re.findall(r'AS\s+(.*?)\s*$', transformations, re.MULTILINE)

columns = re.findall(r'UNNEST\(ARRAY\[(.*?)\]\)', transformations, re.DOTALL)

for alias, column in zip(aliases, columns):
    print("Alias:", alias)
    unique_values = list(set(re.findall(r'\(([^()]*)\)', column)))
    unique_values = [value.split(',')[0].strip(" '").split(' as ')[0].split(' ', 1)[0] for value in unique_values]
    print("Unique Values:", list(set(unique_values)))  # Use set to remove duplicates
    print()
