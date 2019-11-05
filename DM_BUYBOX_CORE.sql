-- Me traigo todos los productos fin de rama activos en buybox
CREATE multiset VOLATILE TABLE products_activos AS
  (SELECT prd_stat.prd_product_id,
          prd_stat.sit_site_id,
          prd.prd_name,
          prd.dom_domain_id,
          cast(coalesce(substr(prd.prd_parent_id, 4), prd.prd_product_id) AS int) parent_id,
          cast(prd_stat.aud_ins_dt AS date) last_upd
   FROM whowner.LK_BUYBOX_PRODUCT_STATUS prd_stat
   LEFT JOIN WHOWNER.LK_PRD_DOMAIN_PRODUCTS prd ON prd.prd_product_id = prd_stat.prd_product_id
   AND prd.sit_site_id = prd_stat.sit_site_id
   WHERE status = 'active' ) WITH DATA index(prd_product_id,
                                             sit_site_id) ON
COMMIT PRESERVE ROWS ;

COLLECT STATISTICS COLUMN (DOM_DOMAIN_ID) ON products_activos;


CREATE multiset VOLATILE TABLE dom_activos AS
  (SELECT dom_domain_id
   FROM products_activos
   GROUP BY 1) WITH DATA index(dom_domain_id) ON
COMMIT PRESERVE ROWS ;

COLLECT STATISTICS COLUMN (DOM_DOMAIN_ID) ON dom_activos;


CREATE MULTISET VOLATILE TABLE products_totales_pre AS
  (SELECT prd.dom_domain_id,
          prd.sit_site_id,
          prd.prd_product_id prd_id,
          prd.prd_name prd_name,
          cast(trim(prd.sit_site_id) || cast(cast(prd.prd_product_id AS int) AS varchar(50)) AS varchar(255)) prd_id_string,
          prd_parent_id
   FROM whowner.LK_PRD_DOMAIN_PRODUCTS prd
   JOIN dom_activos dom ON dom.dom_domain_id = prd.dom_domain_id) WITH DATA index(prd_id_string) ON
COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (PRD_PARENT_ID) ON products_totales_pre;
COLLECT STATISTICS COLUMN (PRD_ID_STRING) ON products_totales_pre;
COLLECT STATISTICS COLUMN (SIT_SITE_ID,PRD_ID) ON products_totales_pre;


CREATE MULTISET VOLATILE TABLE products_totales AS
  (SELECT prd.prd_id_string,
          prd.dom_domain_id,
          prd.sit_site_id,
          prd.prd_id prd_id,
          prd.prd_name prd_name,
          coalesce(par.prd_id, prd.prd_id) par_id,
          coalesce(par.prd_name, prd.prd_name) par_name,
          CASE
              WHEN par.prd_id IS NULL THEN 'parent'
              ELSE 'child'
          END AS rama,
          CASE
              WHEN prd_stat.prd_product_id IS NOT NULL THEN 'bb'
              ELSE 'no bb'
          END AS status_bb,
          cast(prd_stat.aud_ins_dt AS date) last_upd
   FROM products_totales_pre prd
   LEFT JOIN products_totales_pre par ON par.prd_id_string = prd.prd_parent_id
   LEFT JOIN LK_BUYBOX_PRODUCT_STATUS prd_stat ON prd_stat.prd_product_id = prd.prd_id
   AND prd_stat.sit_Site_id = prd.sit_site_id) WITH DATA index(prd_id_string) ON
COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (PRD_ID) ON products_totales;
COLLECT STATISTICS COLUMN (SIT_SITE_ID, PRD_ID) ON products_totales;
COLLECT STATISTICS COLUMN (SIT_SITE_ID) ON products_totales;


CREATE multiset VOLATILE TABLE DM_NOLS AS
  (SELECT i.sit_site_id,
          d.dom_domain_id,
          prd.prd_id prd_product_id,
          sum(1) AS nol,
          sum(ite_catalog_listing) AS nol_buy_box
   FROM WHOWNER.LK_ITE_ITEMS_PH i
   LEFT JOIN products_totales prd ON prd_id = i.ctlg_prod_id
   AND prd.sit_site_id = i.sit_site_id
   LEFT JOIN dom_activos d ON i.ite_dom_domain_id = d.dom_domain_id
   WHERE i.photo_id = 'TODATE'
     AND i.ite_auction_start = date -1
     AND i.sit_site_id IN('MLA',
                          'MLM',
                          'MLB')
   GROUP BY 1,2,3) WITH DATA index(sit_site_id,
                               dom_domain_id,
                               prd_product_id) ON
COMMIT PRESERVE ROWS ;

COLLECT STATISTICS COLUMN (SIT_SITE_ID,DOM_DOMAIN_ID,PRD_PRODUCT_ID) ON DM_NOLS;

CREATE MULTISET VOLATILE TABLE DM_BIDS AS
  (SELECT b.sit_site_id,
          d.dom_domain_id,
          prd.prd_id prd_product_id,
          SUM(b.BID_BASE_CURRENT_PRICE * b.BID_QUANTITY_OK) GMV,
          SUM(CASE
                  WHEN b.ite_catalog_listing = 1 THEN b.BID_BASE_CURRENT_PRICE * b.BID_QUANTITY_OK
              END) GMV_BB,
          SUM(CASE
                  WHEN prd.prd_id_string IS NOT NULL THEN b.BID_BASE_CURRENT_PRICE * b.BID_QUANTITY_OK
              END) GMV_prod,
          sum(b.BID_QUANTITY_OK) SI,
          SUM(CASE
                  WHEN b.ite_catalog_listing = 1 THEN b.BID_QUANTITY_OK
              END) SI_BB,
          SUM(CASE
                  WHEN prd.prd_id_string IS NOT NULL THEN b.BID_QUANTITY_OK
              END) SI_prod,
          count(DISTINCT b.cus_cust_id_sel) sellers,
          count(DISTINCT CASE
                             WHEN b.ite_catalog_listing = 1 THEN b.cus_cust_id_sel
                         END) sellers_BB,
          count(DISTINCT CASE
                             WHEN prd.prd_id_string IS NOT NULL THEN b.cus_cust_id_sel
                         END) sellers_prod
   FROM WHOWNER.BT_BIDS b
   LEFT JOIN dom_activos d ON b.dom_domain_id = d.dom_domain_id
   LEFT JOIN products_totales prd ON prd.prd_id_string = b.ite_catalog_product_id_str
   AND prd.sit_site_id = b.sit_site_id
   WHERE b.photo_id = 'TODATE'
     AND B.SIT_SITE_ID IN ('MLA',
                           'MLB',
                           'MLM')
     AND b.tim_day_winning_date = date -1
     AND ite_gmv_flag = 1
   GROUP BY 1,2,3) WITH DATA index(sit_site_id,
                               prd_product_id,
                               dom_domain_id) ON
COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (SIT_SITE_ID,DOM_DOMAIN_ID,PRD_PRODUCT_ID) ON DM_BIDS ;

CREATE multiset VOLATILE TABLE DM_ORDERS AS
  (SELECT ord.odr_created_dt tim_day,
          ord.sit_site_id,
          prd.prd_id prd_product_id,
          d.dom_domain_id dom_domain_id,
          sum(1) orders_totales,
          sum(CASE
                  WHEN odr_status_id = 'paid' THEN 1
              END) AS orders_paid,
          sum(CASE
                  WHEN odr_status_id = 'cancelled' THEN 1
              END) AS orders_cancelled,
          sum(CASE
                  WHEN odr_cancel_cause_id = 'Seller has refunded all the payments' THEN 1
              END) AS cancelled_by_seller
   FROM whowner.BT_BIDS b
   JOIN whowner.BT_ODR_PURCHASE_ORDERS ord ON (b.ord_order_id = ord.odr_order_id)
   JOIN whowner.bt_mp_pay_payments p ON (ord.odr_order_id = p.ord_order_id)
   LEFT JOIN dom_activos d ON b.dom_domain_id = d.dom_domain_id
   LEFT JOIN products_totales prd ON prd.prd_id_string = b.ite_catalog_product_id_str
   AND prd.sit_site_id = b.sit_site_id
   WHERE b.ite_catalog_listing = 1
     AND b.photo_id = 'TODATE'
     AND b.tim_day_winning_date BETWEEN date '2019-05-28' AND date -1
     AND p.pay_move_date BETWEEN date '2019-05-28' AND date -1
     AND p.pay_status_id IN ('approved',
                             'refunded')
   GROUP BY 1,2,3,4) WITH DATA index(tim_day,sit_site_id,prd_product_id,dom_domain_id) ON
COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (PRD_NAME,PAR_ID,PAR_NAME,LAST_UPD) ON products_totales;
COLLECT STATISTICS COLUMN (SIT_SITE_ID,PRD_ID) ON products_totales;
COLLECT STATISTICS COLUMN (SIT_SITE_ID) ON products_totales;
COLLECT STATISTICS COLUMN (SIT_SITE_ID) ON DM_ORDERS;
COLLECT STATISTICS COLUMN (TIM_DAY,SIT_SITE_ID,PRD_PRODUCT_ID,DOM_DOMAIN_ID) ON DM_ORDERS;


CREATE multiset VOLATILE TABLE DM_LL AS
  (SELECT ll.sit_site_id,
          d.dom_domain_id,
          prd.prd_id prd_product_id,
          sum(livelistings) AS ll,
          sum(livelistings_catalog) AS ll_buy_box,
          count(DISTINCT cus_cust_id_sel) AS ls,
          count(DISTINCT CASE
                             WHEN livelistings_catalog > 0 THEN cus_cust_id_sel
                         END) AS ls_buy_box
   FROM WHOWNER.BT_LIVE_LISTINGS_SEL ll
   LEFT JOIN products_totales prd ON prd_id = ll.ctlg_prod_id
   AND prd.sit_site_id = ll.sit_site_id
   JOIN dom_activos d ON ll.dom_domain_id = d.dom_domain_id
   WHERE ll.tim_Day = date -1
     AND ll.sit_site_id IN('MLA',
                           'MLM',
                           'MLB')
   GROUP BY 1,2,3) WITH DATA index(sit_site_id,
                               prd_product_id,
                               dom_domain_id) ON
COMMIT PRESERVE ROWS;

COLLECT STATISTICS COLUMN (SIT_SITE_ID,PRD_PRODUCT_ID,DOM_DOMAIN_ID) ON DM_LL;


INSERT INTO TABLEAU_TBL.DM_BUYBOX_TBL_TEST
 (SELECT  date -1 as tim_day,
          coalesce(b.sit_site_id, n.sit_site_id, ll.sit_site_id, ap.sit_site_id) sit_Site_id,
          coalesce(b.prd_product_id, n.prd_product_id, ll.prd_product_id, ap.prd_id) prd_product_id,
          coalesce(b.dom_domain_id, n.dom_domain_id, ll.dom_domain_id, ap.dom_domain_id) dom_domain_id,
          ap.prd_name,
          ap.par_id,
          ap.last_upd,
          ap.par_name,
          ap.rama,
          ap.status_bb,
          sum(nol) NOLS_SITE,
          sum(nol_buy_box) NOLS_BB,
          sum(GMV) GMV_SITE,
          SUM(GMV_BB) GMV_BB,
          SUM(GMV_prod) GMV_PROD,
          sum(SI) SI_SITE,
          SUM(SI_BB) SI_BB,
          SUM(SI_prod) SI_PROD,
          SUM(sellers) SELLERS_SITE,
          SUM(sellers_BB) SELLERS_BB,
          SUM(sellers_prod) SELLERS_PROD,
          null as orders_totales,
          null as orders_paid,
          null AS orders_cancelled,
          null AS cancelled_by_seller,
          sum(ll) ll,
          sum(ll_buy_box) ll_buy_box,
          sum(ls) ls,
          sum(ls_buy_box) ls_buy_box
   FROM DM_BIDS b
   FULL OUTER JOIN DM_NOLS n
    ON b.sit_site_id = n.sit_site_id
     AND b.prd_product_id = n.prd_product_id
     AND b.dom_domain_id = n.dom_domain_id
   FULL OUTER JOIN DM_ll ll 
    ON coalesce(b.sit_site_id, n.sit_site_id, o.sit_site_id) = ll.sit_site_id
     AND coalesce(b.prd_product_id, n.prd_product_id, o.prd_product_id) = ll.prd_product_id
     AND coalesce(b.dom_domain_id, n.dom_domain_id, o.dom_domain_id) = ll.dom_domain_id
   FULL OUTER JOIN products_totales ap 
    ON coalesce(b.prd_product_id, n.prd_product_id, o.prd_product_id, ll.prd_product_id) = ap.prd_id
     AND coalesce(b.sit_site_id, n.sit_site_id, o.sit_site_id, ll.sit_site_id) = ap.sit_site_id
   GROUP BY 1,2,3,4,5,6,7,8,9,10)
   
   
   
UPDATE TABLEAU_TBL.DM_BUYBOX_TBL_TEST b
inner join DM_ORDERS o 
  on b.tim_day = o.tim_day
set b.orders_totales = o.orders_totales,
  b.orders_paid = o.orders_paid,
  b.orders_cancelled = o.orders_cancelled,
  b.cancelled_by_seller = o.cancelled_by_seller
