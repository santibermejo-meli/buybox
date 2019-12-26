-- Info Claims
select b.tim_day_winning_date,
  i.sit_site_id,
  i.ite_dom_domain_id,
  i.ite_item_id,
  'BB' as site_bb,
  x.cla_reason_detail,
  count(x.order_id) cant_cases,
  COALESCE(SUM(b.BID_QUANTITY_OK * b.BID_BASE_CURRENT_PRICE),.0) GMV_BB
from WHOWNER.LK_ITE_ITEMS_PH I
join WHOWNER.BT_BIDS b
  on i.ite_item_id = b.ite_item_id
    and i.sit_site_id = b.sit_site_id
LEFT JOIN WHOWNER.BT_CM_CLAIMS X
  ON B.ORD_ORDER_ID = X.order_id
    AND B.SIT_SITE_ID = X.sit_site_id
WHERE i.ITE_REFERRER_ID = 128902916174848
  AND i.photo_id = 'TODATE'
  and i.ite_auction_start >= '2019-12-13'
  and i.sit_site_id IN ('MLA','MLB','MLM')
  and B.PHOTO_ID = 'TODATE'
  AND B.ITE_GMV_FLAG = 1
  AND B.MKT_MARKETPLACE_ID = 'TM'
  AND B.TIM_DAY_WINNING_DATE BETWEEN date '2019-12-13' and date - 1
group by 1,2,3,4,5,6
having cant_cases > 0

UNION ALL

select b.tim_day_winning_date,
  i.sit_site_id,
  i.ite_dom_domain_id,
  i.ite_item_id,
  'SITE' as site_bb,
  x.cla_reason_detail,
  count(x.order_id) cant_cases,
  COALESCE(SUM(b.BID_QUANTITY_OK * b.BID_BASE_CURRENT_PRICE),.0) GMV_BB
from WHOWNER.LK_ITE_ITEMS_PH I
join WHOWNER.LK_BUYBOX_ITEMS_PARENTS p
  on p.ite_item_id_buybox = i.ite_item_id
    and p.sit_site_id = i.sit_site_id
join WHOWNER.BT_BIDS b
  on p.ite_item_id = b.ite_item_id
    and p.sit_site_id = b.sit_site_id
    and p.ite_var_id = coalesce(b.ITE_VARIATION_ID,0)
LEFT JOIN WHOWNER.BT_CM_CLAIMS X
  ON B.ORD_ORDER_ID = X.order_id
    AND B.SIT_SITE_ID = X.sit_site_id
WHERE i.ITE_REFERRER_ID = 128902916174848
  AND i.photo_id = 'TODATE'
  and i.ite_auction_start >= '2019-12-13'
  and i.sit_site_id IN ('MLA','MLB','MLM')
  and B.PHOTO_ID = 'TODATE'
  AND B.ITE_GMV_FLAG = 1
  AND B.MKT_MARKETPLACE_ID = 'TM'
  AND B.TIM_DAY_WINNING_DATE BETWEEN date '2019-12-13' and date - 1
group by 1,2,3,4,5,6
having cant_cases > 0


-- Info Orders
select b.tim_day_winning_date,
  i.sit_site_id,
  i.ite_dom_domain_id,
  i.ite_item_id,
  i.ite_item_status,
  'BB' as site_bb,
  COALESCE(SUM(b.BID_QUANTITY_OK),0) SI,
  COALESCE(SUM(b.BID_QUANTITY_OK * b.BID_BASE_CURRENT_PRICE),.0) GMV
from WHOWNER.LK_ITE_ITEMS_PH I
join WHOWNER.BT_BIDS b
  on i.ite_item_id = b.ite_item_id
    and i.sit_site_id = b.sit_site_id
WHERE i.ITE_REFERRER_ID = 128902916174848
  AND i.photo_id = 'TODATE'
  and i.ite_auction_start >= '2019-12-13'
  and i.sit_site_id IN ('MLA','MLB','MLM')
  and B.PHOTO_ID = 'TODATE'
  AND B.ITE_GMV_FLAG = 1
  AND B.MKT_MARKETPLACE_ID = 'TM'
  AND B.TIM_DAY_WINNING_DATE BETWEEN date '2019-12-13' and date - 1
group by 1,2,3,4,5

UNION ALL

select b.tim_day_winning_date,
  i.sit_site_id,
  i.ite_dom_domain_id,
  i.ite_item_id,
  i.ite_item_status,
  'SITE' as site_bb,
  COALESCE(SUM(b.BID_QUANTITY_OK),0) SI,
  COALESCE(SUM(b.BID_QUANTITY_OK * b.BID_BASE_CURRENT_PRICE),.0) GMV
from WHOWNER.LK_ITE_ITEMS_PH I
join WHOWNER.LK_BUYBOX_ITEMS_PARENTS p
  on p.ite_item_id_buybox = i.ite_item_id
    and p.sit_site_id = i.sit_site_id
join WHOWNER.BT_BIDS b
  on p.ite_item_id = b.ite_item_id
    and p.sit_site_id = b.sit_site_id
    and p.ite_var_id = coalesce(b.ITE_VARIATION_ID,0)
WHERE i.ITE_REFERRER_ID = 128902916174848
  AND i.photo_id = 'TODATE'
  and i.ite_auction_start >= '2019-12-13'
  and i.sit_site_id IN ('MLA','MLB','MLM')
  and B.PHOTO_ID = 'TODATE'
  AND B.ITE_GMV_FLAG = 1
  AND B.MKT_MARKETPLACE_ID = 'TM'
  AND B.TIM_DAY_WINNING_DATE BETWEEN date '2019-12-13' and date - 1
group by 1,2,3,4,5



-- Info Status (carga tabla para datos historicos)
INSERT INTO TABLEAU_TBL.DM_ITEM_STATUS_FORZADO_BB
select date - 1 as tim_day_winning_date,
  i.sit_site_id,
  i.ite_dom_domain_id as DOM_DOMAIN_ID,
  i.ite_item_status,
  count(distinct i.ite_item_id)
from WHOWNER.LK_ITE_ITEMS_PH I
WHERE i.ITE_REFERRER_ID = 128902916174848
  AND i.photo_id = 'TODATE'
  and i.ite_auction_start >= '2019-12-13'
  and i.sit_site_id IN ('MLA','MLB','MLM')
group by 1,2,3,4;
