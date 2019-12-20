create MULTISET VOLATILE TABLE products_hijos as (
select p.sit_site_id,
  p.DOM_DOMAIN_ID,
  p.prd_product_id,
  d.PRD_NAME,
  p.ITE_ITEM_ID_WINNER as ite_winner_bb,
  p.ITE_PRICE_WINNER as precio_win_bb,
  cast(coalesce( substr(d.prd_parent_id, 4), d.prd_product_id) as int) prd_parent_id,
  count(distinct l.CUS_CUST_ID_SEL) ls,
  count(distinct case when livelistings_catalog > 0 then l.CUS_CUST_ID_SEL end) ls_bb,
  COALESCE(sum(livelistings),0) as ll,
  COALESCE(sum(livelistings_catalog),0) as ll_bb
from WHOWNER.LK_BUYBOX_PRODUCT_STATUS p
LEFT JOIN WHOWNER.LK_PRD_DOMAIN_PRODUCTS d
  ON d.PRD_PRODUCT_ID = p.PRD_PRODUCT_ID
    and d.sit_site_id = p.sit_site_id
left join WHOWNER.LK_ITE_ITEMS_PH i
  on p.sit_site_id = i.sit_site_id
    and p.prd_product_id = i.ctlg_prod_id 
    and i.PHOTO_ID = 'TODATE'
left join WHOWNER.BT_LIVE_LISTINGS_SEL l
  on p.prd_product_id = l.ctlg_prod_id
    and p.sit_site_id = l.sit_site_id
    and l.tim_Day = date - 1
where p.status = 'active'
group by 1,2,3,4,5,6,7
) with data index(SIT_SITE_ID, prd_product_id) on commit preserve rows;


create MULTISET VOLATILE TABLE products_hijos_pro as (
select p.sit_site_id,
  p.DOM_DOMAIN_ID,
  p.prd_product_id,
  p.PRD_NAME,
  p.PRD_PARENT_ID,
  d.prd_name par_name,
  p.ite_winner_bb,
  p.precio_win_bb,
  p.ls,
  p.ls_bb,
  p.ll,
  p.ll_bb
from products_hijos p
left join LK_PRD_DOMAIN_PRODUCTS d
  on p.prd_parent_id = d.prd_product_id
    and p.sit_site_id = d.sit_site_id
) with data index(SIT_SITE_ID, prd_product_id) on commit preserve rows;


create MULTISET VOLATILE TABLE gmv_si as (
select p.sit_site_id,
  p.prd_product_id,
  SUM(b.BID_BASE_CURRENT_PRICE * b.BID_QUANTITY_OK) GMV,
  SUM(b.BID_SITE_CURRENT_PRICE * b.BID_QUANTITY_OK) GMV_LC,
  SUM(case when b.ite_catalog_listing = 1 then b.BID_BASE_CURRENT_PRICE * b.BID_QUANTITY_OK end ) GMV_BB,
  SUM(case when b.ite_catalog_listing = 1 then b.BID_SITE_CURRENT_PRICE * b.BID_QUANTITY_OK end ) GMV_BB_LC,
  sum(b.BID_QUANTITY_OK) SI,
  SUM(case when b.ite_catalog_listing = 1 then b.BID_QUANTITY_OK end ) SI_BB
from products_hijos p
left join bt_bids b
  on p.prd_product_id = b.CTLG_PROD_ID
    and p.sit_site_id = b.sit_site_id
    and b.photo_id = 'TODATE'
    AND B.SIT_SITE_ID IN ('MLA', 'MLB', 'MLM') 
    and b.tim_day_winning_date BETWEEN date -31 AND date -1
    and b.ite_gmv_flag = 1
group by 1,2
) with data index(SIT_SITE_ID, prd_product_id) on commit preserve rows;

create MULTISET VOLATILE TABLE precios as (
select p.sit_site_id,
      p.prd_product_id,
      MIN(i.ITE_CURRENT_PRICE) precio_vip,
      MIN(c.comp_item_price) precio_comp
from products_hijos p
left join WHOWNER.LK_ITE_ITEMS_PH i
  on p.prd_product_id = i.ctlg_prod_id
    and p.sit_site_id = i.sit_site_id
    and i.ite_catalog_listing = 0
    and i.photo_id = 'TODATE'
    and i.ite_status = 'active'
    and i.ite_ll_flag = 1
    and ite_auction_start >= '2019-05-28'
    and i.sit_site_id IN ('MLA','MLB','MLM')
left join WHOWNER.LK_COMP_BUYBOX c
  on c.prd_product_id = p.prd_product_id
    and c.sit_site_id = p.sit_site_id
    and c.photo_id = 'TODATE'
where i.sit_site_id IN ('MLA','MLB','MLM')
group by 1,2
) with data index(SIT_SITE_ID, prd_product_id) on commit preserve rows;

insert into TABLEAU_TBL.DM_BUYBOX_PROD 
select p.*,
  g.GMV,
  g.GMV_LC,
  g.GMV_BB,
  g.GMV_BB_LC,
  g.SI,
  g.SI_BB,
  s.precio_vip,
  s.precio_comp
from products_hijos_pro p
left join gmv_si g
  on p.sit_site_id = g.sit_site_id
    and p.prd_product_id = g.prd_product_id
left join precios s
  on p.sit_site_id = s.sit_site_id
    and p.prd_product_id = s.prd_product_id
