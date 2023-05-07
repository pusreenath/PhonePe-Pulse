-- Created by: @pusreenath https://github.com/pusreenath on 6th May 2023

CREATE OR REPLACE VIEW public.trdata_vw
AS SELECT a.state,
    d.entityname AS district,
    a.year,
    a.quarter,
    a.name AS transactioncategory,
    sum(a.count) AS totaltransactioncount,
    sum(a.amount) AS totaltransactionamount,
	sum(d.metric_count) as totaltransactioncount_district,
	sum(d.metric_amount) as totaltransactionamount_district,
    'transaction'::text AS transaction_or_users
   FROM agg_tdata a
     LEFT JOIN districts d ON a.state::text = d.state::text
  WHERE d.transaction_or_users::text = 'transaction'::text
  GROUP BY a.state, d.entityname, a.year, a.quarter, a.name
  ORDER BY a.state, d.entityname, a.year, a.quarter, a.name;

CREATE OR REPLACE VIEW userdata_vw as
SELECT a.state,
	d.name AS district,
	a.year,
	a.quarter,
	a.brand,
	sum(a.count) AS totaldevicecount,
	sum(a.data_aggregated_registeredusers) AS totalaggregisteredusers,
	sum(a.data_aggregated_appopens) AS totalaggappopens,
	sum(d.registeredusers) AS totalregisteredusers_district,
	'users'::text AS transaction_or_users
FROM agg_udata a
 	LEFT JOIN districts d ON a.state::text = d.state::text
WHERE d.transaction_or_users::text = 'users'::text
GROUP BY a.state, d.name, a.year, a.quarter, a.brand
ORDER BY a.state, d.name, a.year, a.quarter, a.brand;