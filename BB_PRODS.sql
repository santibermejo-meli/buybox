create multiset volatile table products_activos as (
select prd_stat.prd_product_id, 
      prd_stat.sit_site_id, 
      prd.prd_name, 
      prd.dom_domain_id, 
      cast(coalesce( substr(prd.prd_parent_id, 4), prd.prd_product_id) as int) parent_id, 
      cast(prd_stat.PRD_CREATION_DT as date) PRD_CREATION_DT
from whowner.LK_BUYBOX_PRODUCT_STATUS prd_stat
left join WHOWNER.LK_PRD_DOMAIN_PRODUCTS prd 
  on  prd.prd_product_id = prd_stat.prd_product_id 
    and prd.sit_site_id = prd_stat.sit_site_id
where status = 'active'
) with data index(prd_product_id,sit_site_id ) on commit preserve rows ;

COLLECT STATISTICS COLUMN (DOM_DOMAIN_ID) ON products_activos;

create multiset volatile table dom_activos as (
select dom_domain_id from products_activos group by 1
) with data index(dom_domain_id ) on commit preserve rows ;

COLLECT STATISTICS COLUMN (DOM_DOMAIN_ID) ON dom_activos;

create MULTISET VOLATILE TABLE products_totales_pre as (
select prd.dom_domain_id, 
  prd.sit_site_id, 
  prd.prd_product_id prd_id ,   
  prd.prd_name prd_name,
  cast(trim(prd.sit_site_id) || cast(cast(prd.prd_product_id as int) as varchar(50)) as varchar(255)) prd_id_string,
  prd_parent_id
from whowner.LK_PRD_DOMAIN_PRODUCTS prd 
join dom_activos dom 
  on dom.dom_domain_id = prd.dom_domain_id
) with data index(prd_id_string) on commit preserve rows;

COLLECT STATISTICS COLUMN (PRD_PARENT_ID) ON products_totales_pre;
COLLECT STATISTICS COLUMN (PRD_ID_STRING) ON products_totales_pre;
COLLECT STATISTICS COLUMN (SIT_SITE_ID ,PRD_ID) ON products_totales_pre;

create MULTISET VOLATILE TABLE products_totales as (
select prd.prd_id_string,
  prd.dom_domain_id, 
  prd.sit_site_id, 
  prd.prd_id prd_id ,   
  prd.prd_name prd_name,
  coalesce(par.prd_id, prd.prd_id) par_id,
  coalesce(par.prd_name, prd.prd_name) par_name,
  case when par.prd_id is null then 'parent' else 'child' end as rama,
  case when prd_stat.prd_product_id is not null then 'bb' else 'no bb' end as status_bb,
  cast(prd_stat.PRD_CREATION_DT as date) PRD_CREATION_DT 
from products_totales_pre prd
left join products_totales_pre par 
  on par.prd_id_string = prd.prd_parent_id
left join LK_BUYBOX_PRODUCT_STATUS prd_stat 
  on prd_stat.prd_product_id = prd.prd_id 
    and prd_stat.sit_Site_id = prd.sit_site_id
    and prd_stat.status = 'active'
) with data index(prd_id_string) on commit preserve rows;

COLLECT STATISTICS COLUMN (PRD_ID) ON products_totales;
COLLECT STATISTICS COLUMN (SIT_SITE_ID ,PRD_ID) ON products_totales;
COLLECT STATISTICS COLUMN (SIT_SITE_ID) ON products_totales;


select p.*,
  count(distinct l.CUS_CUST_ID_SEL) sellers
  count(distinct case when livelistings_catalog > 0 then l.CUS_CUST_ID_SEL end) sellers_bb
  sum(livelistings) as ll,
  sum(livelistings_catalog) as ll_bb,
from products_totales p
join WHOWNER.BT_LIVE_LISTINGS_SEL l
  on p.prd_id = l.ctlg_prod_id
    and p.sit_site_id = l.sit_site_id
where livelistings_catalog > 0
group by 1,2,3,4,5,6,7,8,9,10
