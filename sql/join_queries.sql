-- SQL JOIN QUERIES
-- Author: Liliia Hurianova
-- Description: Combining Facebook and Google Ads data to analyze marketing performance
  
with facebook_google_stat as(with facebook_stat as (
select fabd.ad_date,
fc.campaign_name, 
fa.adset_name, 
fabd.spend, 
fabd.impressions, 
fabd.reach, 
fabd.clicks, 
fabd.leads, 
fabd.value
from facebook_ads_basic_daily fabd
left join facebook_adset fa
on fabd.adset_id = fa.adset_id
left join facebook_campaign fc
on fabd.campaign_id = fc.campaign_id)

select 'facebook' as media_source,
ad_date, campaign_name,adset_name,
spend, impressions,
reach,clicks, leads, value
from facebook_stat
union all
select  'google' as media_source, ad_date,
campaign_name,adset_name,
spend, impressions,
reach,clicks, leads, value
from google_ads_basic_daily)
select media_source, ad_date, campaign_name,adset_name,
spend, impressions,
reach,clicks, leads, value
from facebook_google_stat ;
