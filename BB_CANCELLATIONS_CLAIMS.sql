create multiset volatile table DOMAINS as (
select distinct prd.dom_domain_id
from whowner.LK_BUYBOX_PRODUCT_STATUS prd_stat
left join WHOWNER.LK_PRD_DOMAIN_PRODUCTS prd 
  on prd.prd_product_id = prd_stat.prd_product_id 
    and prd.sit_site_id = prd_stat.sit_site_id
where status = 'active'
)
with data primary index (dom_domain_id) on commit preserve rows;

create multiset volatile table CANCELLATIONS_CLAIMS as (
SELECT B.TIM_DAY_WINNING_DATE,
  B.SIT_SITE_ID,
  B.DOM_DOMAIN_ID,
  (CASE WHEN COALESCE(B.ITE_OFFICIAL_STORE_ID, 0) > 0 THEN 'TIENDA OFICIAL'
         WHEN COALESCE(B.ITE_OFFICIAL_STORE_ID, 0) = 0 AND G.CUS_CUST_ID_SEL IS NOT NULL THEN 'CARTERA GESTIONADA'
   ELSE 'LONG TAIL' END) AS SEGMENTO,
  COALESCE(SUM(CASE WHEN C.ORD_ORDER_ID IS NOT NULL THEN b.BID_QUANTITY_OK * B.BID_SITE_CURRENT_PRICE END),0) GMV_BB_CANCELLED,
  COUNT(B.ord_order_id) ORDERS_BB,
  COUNT(X.ord_order_id) ORDERS_CASES_BB
FROM WHOWNER.BT_BIDS B
JOIN DOMAINS d
  ON d.dom_domain_id = b.DOM_DOMAIN_ID
LEFT JOIN WHOWNER.BT_CM_ORDERS_CANCELLED C
  ON B.ORD_ORDER_ID = C.ord_order_id
    AND B.SIT_SITE_ID = C.sit_site_id
LEFT JOIN WHOWNER.LK_SALES_CARTERA_GESTIONADA AS G
  ON B.CUS_CUST_ID_SEL = G.CUS_CUST_ID_SEL
    AND COALESCE(B.ITE_OFFICIAL_STORE_ID, 0) = G.ITE_OFFICIAL_STORE_ID
    AND B.TIM_DAY_WINNING_DATE BETWEEN G.FECHA_DESDE AND COALESCE(G.FECHA_HASTA, DATE + 1)
LEFT JOIN WHOWNER.BT_CX_CASE X
  ON B.ORD_ORDER_ID = X.ord_order_id
    AND B.SIT_SITE_ID = X.sit_site_id
WHERE B.PHOTO_ID = 'TODATE'
  AND B.ITE_GMV_FLAG = 1
  AND B.MKT_MARKETPLACE_ID = 'TM'
  AND B.TIM_DAY_WINNING_DATE > date - 31
  AND B.SIT_SITE_ID IN ('MLA','MLB','MLM')
  and b.ite_catalog_LISTING = 1
GROUP BY 1,2,3,4
ORDER BY 1,2,3,4
)
with data primary index (dom_domain_id) on commit preserve rows;
