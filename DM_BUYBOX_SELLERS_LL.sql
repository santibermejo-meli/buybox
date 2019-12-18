create multiset volatile table DOMAINS as (
select distinct prd.dom_domain_id
from whowner.LK_BUYBOX_PRODUCT_STATUS prd_stat
left join WHOWNER.LK_PRD_DOMAIN_PRODUCTS prd 
  on prd.prd_product_id = prd_stat.prd_product_id 
    and prd.sit_site_id = prd_stat.sit_site_id
where status = 'active'
) with data primary index (dom_domain_id) on commit preserve rows;

DELETE FROM TABLEAU_TBL.DM_BUYBOX_SELLERS_LL WHERE TIM_DAY_WINNING_DATE = DATE - 1;

INSERT INTO TABLEAU_TBL.DM_BUYBOX_SELLERS_LL
select date - 1 as tim_day_winning_date,
  i.sit_site_id,
  I.ITE_DOM_DOMAIN_ID,
  (CASE WHEN COALESCE(I.ITE_OFFICIAL_STORE_ID, 0) > 0 THEN 'TIENDA OFICIAL'
         WHEN COALESCE(I.ITE_OFFICIAL_STORE_ID, 0) = 0 AND G.CUS_CUST_ID_SEL IS NOT NULL THEN 'CARTERA GESTIONADA'
   ELSE 'LONG TAIL' END) AS SEGMENTO, 
--  count(distinct case when i.ite_catalog_listing = 1 then p.PRD_PRODUCT_ID end) prds_con_oferta,
  null as items_totales,
  count(distinct i.ite_item_id) as items_dom_act,
  count(distinct case when p.PRD_PRODUCT_ID is not null then i.ite_item_id end) as items_bb_ready,
  count(distinct case when h.ite_var_opt_competing = 1 or h.ite_var_opt_ready_for_optin = 1 then i.ite_item_id end) as items_optineables,
  count(distinct case when i.ite_catalog_listing = 1 then i.ite_item_id end) as items_bb,
  null as sellers_totales,
  count(distinct i.cus_cust_id_sel) as sellers_dom_act,
  count(distinct case when p.PRD_PRODUCT_ID is not null then i.cus_cust_id_sel end) as sellers_bb_ready,
  count(distinct case when h.ite_var_opt_competing = 1 or h.ite_var_opt_ready_for_optin = 1 then i.cus_cust_id_sel end) as sellers_optineables,
  count(distinct case when i.ite_catalog_listing = 1 then i.cus_cust_id_sel end) as sellers_bb
from WHOWNER.LK_ITE_ITEMS_PH i
join domains d
  on d.dom_domain_id = i.ite_dom_domain_id
left join whowner.LK_BUYBOX_PRODUCT_STATUS p
 on i.ctlg_prod_id = p.PRD_PRODUCT_ID
   and i.sit_site_id = p.sit_site_id
   and p.status = 'active'
left join WHOWNER.LK_BUYBOX_ITEMS_OPT_HIST h
  ON H.ite_item_id = I.ite_item_id
    AND H.sit_site_id = i.sit_site_id
LEFT JOIN WHOWNER.LK_SALES_CARTERA_GESTIONADA AS G
  ON I.CUS_CUST_ID_SEL = G.CUS_CUST_ID_SEL
    AND COALESCE(I.ITE_OFFICIAL_STORE_ID, 0) = G.ITE_OFFICIAL_STORE_ID
    AND date - 1 BETWEEN G.FECHA_DESDE AND COALESCE(G.FECHA_HASTA, DATE + 1)
where i.photo_id = 'TODATE'
   AND i.ite_status = 'active'
   and i.ite_ll_flag = 1
   and ite_auction_start >= '2019-05-28'
   and i.sit_site_id IN ('MLA','MLB','MLM')
   and i.ITE_DOM_DOMAIN_ID is not null
group by 1,2,3,4;
  
INSERT INTO TABLEAU_TBL.DM_BUYBOX_SELLERS_LL
select date - 1 as tim_day_winning_date,
  i.sit_site_id,
  i.sit_site_id || '-TOTAL_SITE' as ITE_DOM_DOMAIN_ID,
  (CASE WHEN COALESCE(I.ITE_OFFICIAL_STORE_ID, 0) > 0 THEN 'TIENDA OFICIAL'
         WHEN COALESCE(I.ITE_OFFICIAL_STORE_ID, 0) = 0 AND G.CUS_CUST_ID_SEL IS NOT NULL THEN 'CARTERA GESTIONADA'
   ELSE 'LONG TAIL' END) AS SEGMENTO, 
--  null as prds_con_oferta,
  count(distinct i.ite_item_id) as items_totales,
  null as items_dom_act,
  null as items_bb_ready,
  null as items_optineables,
  null as items_bb,
  count(distinct i.cus_cust_id_sel) as sellers_totales,
  null as sellers_dom_act,
  null as sellers_bb_ready,
  null as sellers_optineables,
  null as sellers_bb
from WHOWNER.LK_ITE_ITEMS_PH i
LEFT JOIN WHOWNER.LK_SALES_CARTERA_GESTIONADA AS G
ON I.CUS_CUST_ID_SEL = G.CUS_CUST_ID_SEL
  AND COALESCE(I.ITE_OFFICIAL_STORE_ID, 0) = G.ITE_OFFICIAL_STORE_ID
  AND date - 1 BETWEEN G.FECHA_DESDE AND COALESCE(G.FECHA_HASTA, DATE + 1)
where i.photo_id = 'TODATE'
   AND i.ite_status = 'active'
   and i.ite_ll_flag = 1
   and i.ite_auction_start >= '2019-05-28'
   and i.sit_site_id IN ('MLA','MLB','MLM')
group by 1,2,3,4
