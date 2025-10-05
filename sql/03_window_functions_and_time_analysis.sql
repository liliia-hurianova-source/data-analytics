-- Window_functions_and_time_analysis.sql
-- Goal: Monthly aggregation by UTM campaign + LAG() analysis for CPM, CTR, and ROMI dynamics
-- Topics: window functions, COALESCE, regex, monthly grouping, marketing KPIs
with union_ads_basic_daily as (select ad_date,
coalesce(url_parameters,'unknown') as url_parameters,
coalesce(spend,0) as spend, 
coalesce(impressions,0) as impressions, 
coalesce(reach,0) as reach, 
coalesce(clicks,0) as clicks,
coalesce(leads,0) as leads, 
coalesce(value,0) as value
from facebook_ads_basic_daily f
union all 
select ad_date,
coalesce(url_parameters,'unknown'),
coalesce(spend,0) as spend, 
coalesce(impressions,0) as impressions, 
coalesce(reach,0) as reach, 
coalesce(clicks,0) as clicks,
coalesce(leads,0) as leads, 
coalesce(value,0) as value
from google_ads_basic_daily),
ads_monthly as 
(select date_trunc('month', ad_date)::date as ad_month,
case 
	when lower(substring(url_parameters, 'utm_campaign=([^\&]+)')) = 'nan' then null 
	else lower(substring(url_parameters, 'utm_campaign=([^\&]+)'))
end as utm_campaign,
sum(spend) as total_spend,
sum(impressions) as total_impressions,
sum(clicks) as total_clicks,
sum(value) as total_profit,
case 
	when sum(impressions)>0 then round(sum(clicks)::numeric/sum(impressions)::numeric,2) else 0
end as ctr,
case 
	when sum(clicks) > 0 then round(sum(spend)::numeric/sum(clicks)::numeric,2) else 0
end as cpc,
case 
	when sum(impressions) > 0 then round(sum(spend)::numeric/sum(impressions)::numeric*1000,2) else 0
end as cpm,
case 
	when sum(spend) >0 then round(sum(value)::numeric/sum(spend)::numeric*100,2) else 0
end as romi
from union_ads_basic_daily
group by ad_month, utm_campaign)
select ad_month, utm_campaign, total_spend, total_clicks, total_profit,ctr, cpc, cpm, romi,
round((cpm - lag(cpm)
over (partition by utm_campaign order by ad_month))/ 
nullif(lag(cpm) over (partition by utm_campaign order by ad_month), 0) * 100, 2) as dif_cpm,
round((ctr-lag(ctr) 
over ( partition by utm_campaign order by ad_month))/
nullif(lag(ctr) over ( partition by utm_campaign order by ad_month),0)*100,2) as dif_ctr,
round((romi-lag(romi) 
over ( partition by utm_campaign order by ad_month))/
nullif(lag(romi) over (partition by utm_campaign order by ad_month),0)*100,2) as dif_romi
from ads_monthly;
