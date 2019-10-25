DROP TABLE TABLEAU_TBL.DM_BUYBOX_OBJETIVOS_COMERCIALES;
create MULTISET  TABLE TABLEAU_TBL.DM_BUYBOX_OBJETIVOS_COMERCIALES AS (
select a1.tim_day,
  a1.sit_site_id,
  a1.cus_cust_id_sel,
  a1.dom_domain_id,
  cu.cus_nickname,
  cg.asesor,
  cg.asesor_id,
  cg.tipofoco,
  a1.ite_item_id,
  a1.ite_item_title,
  AVG(a1.optineable) as optineable,
  AVG(a1.en_bb) as en_bb
from WHOWNER.LK_SALES_CARTERA_GESTIONADA_AC cg
  LEFT JOIN (sel i.tim_day,
      i.cus_cust_id_sel,
      i.ite_item_id,
      ip.ite_item_title,
      i.sit_site_id,
      d.dom_domain_id,
      (case when i.ite_var_opt_ready_for_optin = 1
        or i.ite_var_opt_competing = 1 then 1 else 0 end) as optineable,
      i.ite_var_opt_competing as en_bb
    from WHOWNER.LK_BUYBOX_ITEMS_OPT_HIST i
    RIGHT JOIN WHOWNER.LK_PRD_DOMAIN_PRODUCTS d
      on i.ctlg_prod_id = d.prd_product_id
        and i.sit_site_id = d.sit_site_id
    LEFT JOIN WHOWNER.LK_ITE_ITEMS_PH ip
      on i.ite_item_id = ip.ite_item_id
        and i.sit_site_id = ip.sit_site_id
    where i.sit_site_id in ('MLA', 'MLB', 'MLM')
      and i.tim_day = date - 1
      and (i.ite_var_opt_ready_for_optin = 1
        or i.ite_var_opt_already_opted_in = 1
        or i.ite_var_opt_competing = 1)) as a1
    on a1.cus_cust_id_sel = cg.cus_cust_id_sel
      and a1.sit_site_id = cg.sit_site_id
  join WHOWNER.LK_CUS_CUSTOMERS_DATA cu
    on a1.cus_cust_id_sel = cu.cus_cust_id
GROUP BY 1,2,3,4,5,6,7,8,9,10
) 
	WITH DATA INDEX(TIM_DAY, SIT_SITE_ID, DOM_DOMAIN_ID);
