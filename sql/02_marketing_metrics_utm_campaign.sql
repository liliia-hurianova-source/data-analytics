-- marketing_metrics_utm_campaign.sql
-- This query analyzes marketing performance by UTM campaign.
-- It calculates key metrics such as sessions, conversions, and conversion rate
-- using Google Analytics 4 sample data.

with fb_and_google_data as (
select ad_date, 
'facebook' media_source, 
coalesce (url_parameters, 'no data') url_parameters,
coalesce (spend,0) spend, 
coalesce (impressions,0) impressions, 
coalesce (reach,0) reach, 
coalesce (clicks,0) clicks, 
coalesce(leads,0) leads, 
coalesce(value,0) value
from facebook_ads_basic_daily
union all
select ad_date, 'google', 
coalesce (url_parameters, 'no data') url_parameters,
coalesce (spend,0) spend, 
coalesce (impressions,0) impressions, 
coalesce (reach,0) reach, 
coalesce (clicks,0) clicks, 
coalesce(leads,0) leads, 
coalesce(value,0) value
from google_ads_basic_daily
)
select
ad_date, 
case
   when
   lower
(substring(url_parameters from  'utm_campaign=([^&]+)')) = 'nan' then null  
   else
   lower
(substring (url_parameters from  'utm_campaign=([^&]+)')) end as utm_campaign,
sum(spend) as total_spend,
sum(impressions) as total_imprssions,
sum(clicks) as total_clicks,
sum(value) as total_value,
case when sum(impressions) = 0 then 0
     else round((sum(clicks)::numeric/sum(impressions)::numeric) *100,4) end as CTR,
case when sum(clicks) = 0 then 0
     else round(sum(spend)::numeric/sum(clicks)::numeric,4)  end as CPC,
case when sum(impressions) = 0 then 0
     else round((sum(spend)::numeric/sum(impressions)::numeric) *1000,4) end as CPM,
case when sum(spend) = 0 then 0 
     else round((sum(value)-sum(spend)::numeric)/sum(spend)::numeric,4) end as ROMI
from fb_and_google_data
group by ad_date, utm_campaign;

