create multiset volatile table DOMAINS as (
select distinct prd.dom_domain_id
from whowner.LK_BUYBOX_PRODUCT_STATUS prd_stat
left join WHOWNER.LK_PRD_DOMAIN_PRODUCTS prd 
  on prd.prd_product_id = prd_stat.prd_product_id 
    and prd.sit_site_id = prd_stat.sit_site_id
where status = 'active'
) with data primary index (dom_domain_id) on commit preserve rows;

create multiset volatile table DM_LL as (
  select 
    ll.tim_Day, 
    ll.sit_site_id, 
    d.dom_domain_id , 
    sum(livelistings) as ll, 
    sum(livelistings_catalog) as ll_bb, 
    count(distinct cus_cust_id_sel) as ls, 
    count(distinct case when livelistings_catalog > 0 then cus_cust_id_sel end) as ls_bb
  from WHOWNER.BT_LIVE_LISTINGS_SEL ll 
  join  DOMAINS d 
   on ll.dom_domain_id = d.dom_domain_id
  where ll.tim_Day between date - 31 and date - 1
  and ll.sit_site_id in('MLA', 'MLM', 'MLB')
group by 1,2,3) with data index(tim_day, sit_site_id, dom_domain_id ) on commit preserve rows;


create multiset volatile table DM_OPTIN as (
  select h.tim_Day,
    h.SIT_SITE_ID,
    d.dom_domain_id,
    count(distinct case when h.ITE_OPT_ELEGIBLE = 1 and H.ite_var_opt_competing = 0 then H.ite_item_id end) as ll_optineables,
    count(distinct case when h.ITE_OPT_ELEGIBLE = 1 and H.ite_var_opt_competing = 0 then H.cus_cust_id_sel end) as ls_optineables 
  FROM WHOWNER.LK_BUYBOX_ITEMS_OPT_HIST h
  join LK_PRD_DOMAIN_PRODUCTS d
    on h.ctlg_prod_id = d.PRD_PRODUCT_ID
      and h.sit_site_id = d.sit_site_id
  where tim_Day between date - 31 and date - 1
group by 1,2,3) with data index(tim_day, sit_site_id, dom_domain_id ) on commit preserve rows;

DELETE FROM TABLEAU_TBL.DM_BUYBOX_SELLERS_LL WHERE TIM_DAY_WINNING_DATE = DATE - 1;

INSERT INTO TABLEAU_TBL.DM_BUYBOX_SELLERS_LL
select 
	coalesce(l.tim_day, o.tim_day) tim_day_winning_date,
	coalesce(l.sit_site_id, o.sit_site_id) sit_site_id,
	coalesce(l.dom_domain_id, o.dom_domain_id) dom_domain_id,
	l.ll,
	l.ll_bb,
	l.ls,
	l.ls_bb,
	o.ll_optineables,
	o.ls_optineables
from dm_ll l
full outer join dm_optin o
  on l.tim_day = o.tim_day
    and l.sit_site_id = o.sit_site_id
    and l.dom_domain_id = o.dom_domain_id;
