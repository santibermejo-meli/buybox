DELETE FROM TABLEAU_TBL.DM_BUYBOX_SELLERS_LL WHERE TIM_DAY_WINNING_DATE = DATE - 1;

INSERT INTO TABLEAU_TBL.DM_BUYBOX_SELLERS_LL
  SELECT  date - 1 AS TIM_DAY_WINNING_DATE,
    I.sit_site_id,
    I.ITE_DOM_DOMAIN_ID,
    (CASE WHEN COALESCE(I.ITE_OFFICIAL_STORE_ID, 0) > 0 THEN 'TIENDA OFICIAL'
           WHEN COALESCE(I.ITE_OFFICIAL_STORE_ID, 0) = 0 AND G.CUS_CUST_ID_SEL IS NOT NULL THEN 'CARTERA GESTIONADA'
     ELSE 'LONG TAIL' END) AS SEGMENTO, 
    COUNT(DISTINCT I.ITE_ITEM_ID) LL_TS,
    SUM(case when H.ite_var_opt_ready_for_optin = 1
          or H.ite_var_opt_competing = 1 then 1 else 0 end) LL_OPTINEABLES,
    SUM(H.ite_var_opt_competing) LL_BB,
    COUNT(DISTINCT i.cus_cust_id_sel) SELLERS_TS,
    COUNT(DISTINCT (case when H.ite_var_opt_ready_for_optin = 1
          or H.ite_var_opt_competing = 1 then i.cus_cust_id_sel else null end)) SELLERS_OPTINEABLES,
    COUNT(DISTINCT (CASE WHEN H.ite_var_opt_competing = 1 then i.cus_cust_id_sel else null end)) SELLERS_BB
  FROM WHOWNER.LK_ITE_ITEMS_PH I
  LEFT JOIN WHOWNER.LK_BUYBOX_ITEMS_OPT_HIST H
    ON H.ite_item_id = I.ite_item_id
      AND H.sit_site_id = i.sit_site_id
  LEFT JOIN WHOWNER.LK_SALES_CARTERA_GESTIONADA AS G
    ON I.CUS_CUST_ID_SEL = G.CUS_CUST_ID_SEL
      AND COALESCE(I.ITE_OFFICIAL_STORE_ID, 0) = G.ITE_OFFICIAL_STORE_ID
      AND date - 1 BETWEEN G.FECHA_DESDE AND COALESCE(G.FECHA_HASTA, DATE + 1)
  JOIN WHOWNER.AG_LK_CAT_CATEGORIES_PH CAT
    ON I.CAT_CATEG_ID=CAT.CAT_CATEG_ID_L7
      AND I.PHOTO_ID=CAT.PHOTO_ID
      AND I.SIT_SITE_ID=CAT.SIT_SITE_ID
  JOIN WHOWNER.LK_CUS_CUSTOMERS_PH   CUS
    ON I.PHOTO_ID=CUS.PHOTO_ID 
      AND I.CUS_CUST_ID_SEL=CUS.CUS_CUST_ID
  WHERE (CAT.CAT_CATEG_ID_L1 IN ('1459', '1743')  OR  CAT.SIT_SITE_ID IN ('ABN') OR I.ITE_BASE_CURRENT_PRICE < 10000) 
    AND I.PHOTO_ID='TODATE' 
    AND I.ITE_AUCTION_STOP BETWEEN DATE -1 AND (I.ITE_ITEM_DURATION + (DATE-1) - 1)
    AND I.ite_status = 'active' 
    AND I.ite_trust_safety_status is null 
    AND I.ite_fraud_prevent_status is  null 
    AND I.ite_deleted = 0 
    AND I.ite_credit_policy_status is null
    AND I.ite_prontuario_status is  null
    AND CUS.PHOTO_ID ='TODATE' 
    AND I.sit_site_id IN ('MLA','MLB','MLM')
    AND h.tim_day = date - 1
  GROUP BY 1,2,3,4
