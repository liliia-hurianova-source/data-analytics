-- Conversion rate analysis by traffic source using GA4 data (BigQuery)
-- Calculates session-to-cart, session-to-checkout, and session-to-purchase conversion rates
-- Dataset: bigquery-public-data.ga4_obfuscated_sample_ecommerce
-- Technologies: BigQuery (SQL), GA4 event schema
WITH user_session_id_info AS (
  SELECT 
    TIMESTAMP_MICROS (event_timestamp) AS event_timestamp, 
    event_name,
    user_pseudo_id ||
    (SELECT value.int_value FROM UNNEST (event_params) where key = 'ga_session_id') as user_session_id,
    traffic_source.name as campaign,
    traffic_source.source as source,
    traffic_source.medium as medium
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131` e 
  WHERE event_name IN (
       'session_start',
       'add_to_cart', 
       'begin_checkout', 
       'purchase')
       ),
users_count  as (
   SELECT 
   DATE(event_timestamp) AS event_date,
   source,
   medium, 
   campaign,
   COUNT (DISTINCT CASE WHEN event_name = 'session_start' THEN user_session_id END) AS count_session_start,
   COUNT (DISTINCT CASE WHEN event_name = 'add_to_cart' THEN user_session_id END) AS count_add_to_cart,
   COUNT (DISTINCT CASE WHEN event_name = 'begin_checkout' THEN user_session_id END) AS count_begin_checkout,
   COUNT (DISTINCT CASE WHEN event_name = 'purchase' THEN user_session_id END) AS count_purchase
FROM user_session_id_info
GROUP BY
   event_date,
   source,
   medium, 
   campaign
   )
SELECT
   event_date,
   source,
   medium,
   campaign,
   ROUND(count_add_to_cart/nullif(count_session_start,0),2) as visit_to_cart,
   ROUND(count_purchase/nullif(count_session_start,0),2) as visit_to_purchase,
   ROUND(count_begin_checkout/nullif(count_session_start,0),2) as visit_to_checkout
FROM users_count
;
