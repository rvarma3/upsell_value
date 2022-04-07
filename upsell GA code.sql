-- create or replace table "EDWH_PROD"."WS_SKYWARDS_PROD".ruch_in_path_cpm_upsell_07_21 as

with upsell_table  as  -- everyone who saw an offer on the website after searching

						(

						select 
						a3.value::string as big_array, 
						a3.value:device.deviceCategory::string as device_type,
						a3.CHANNELGROUPING,
						a3.date_part,
						a3.value:totals::string as totals,
						a3.value:totals.transactions::int as trans_count,
						a3.value:totals.transactionRevenue::int as trns_revenue,
						a3.value:clientId::string as clientid,
						a3.value:fullVisitorId::string as fullvistorid,
						a3.value:visitId::string as sessionid,
						a3.value:hits::string as hits
						from
						"EDWH_PROD"."WS_MDP_PROD"."EXT_GA_RAW_DATA" as a3

						where 
						to_date(a3.date_part) between '2021-07-01' and last_day(to_date('2021-07-01')) and
						a3.value like ('%"eventAction":"Promotion Shown"%') and
						a3.value like ('%"eventLabel":"Emirates~Cash Plus Miles|%') and
						a3.value like ('%"type":"TRANSACTION"%') and
						a3.value like ('%"index":14,"value":"Cash Plus Miles"%') and
						a3.value:totals.transactions::int = 1 and
						a3.value:device.deviceCategory::string = 'desktop' and
						1 = 1 
						-- limit 100

						),
						
transaction_type_details as --capture class , brand and tier codes

					(
					select 
					t1.clientid,
					t1.sessionid,
					t1.date_part,
					t1.CHANNELGROUPING,
					-- vm.value:hitNumber::int as hit_number,
					--case when ve.value:index::string = '33' then ve.value:value::string end as tier_code,
					--case when ve.value:index::string = '52' then ve.value:value::string end as trip_type,
					--case when ve.value:index::string = '143' then ve.value:value::string end as class_brand,
					--case when ve.value:index::string = '150' then ve.value:value::string end as trip_od,
					--listagg(ve.value:value::string, '|') as testing2 ,
					t1.trans_count,
					split_part(listagg(ve.value:value::string, '|')  , '|', 1 ) as tier_code,
					split_part(listagg(ve.value:value::string, '|')  , '|', 2 ) as trip_type,
					split_part(listagg(ve.value:value::string, '|')  , '|', 3 ) as trip_od,
					split_part(listagg(ve.value:value::string, '|')  , '|', 4 ) as class_brand
					from
					upsell_table as t1,
					lateral flatten (input => parse_json(t1.hits)) as vm,
					lateral flatten (input => vm.value:customDimensions) as ve
					where
					vm.value:type = 'TRANSACTION' and
					ve.value:index::string in ('32', '52', '143', '150') and
					
					1 = 1
					group by
					t1.clientid,
					t1.trans_count,
					t1.sessionid,
					t1.date_part,
					t1.CHANNELGROUPING

						)		,				
						

custom_metric_table as -- this where the base fare of the flight selected comes from
						(
							select
							--parse_json(a1.totals):transactionRevenue as revenue,
							a1.clientid,
							a1.sessionid,
							a1.date_part,
						    a1.hits,
						    a1.device_type,
							vm.value:type as hit_type,
							--vm.value:page.pagePath as hit_type,
							vm.value:hitNumber as hit_no,
                            -- vm.value:transaction.transactionRevenue/1000000 as hit_trns_revenue,
							vm.value:transaction.transactionId as hit_trns_id,
							--ve.value:value::string as cd1,
							--vx.value:value::string as cd2,
							case when vr.value:index::string = '3' then vr.value:value::int end as miles_paid,
							case when vr.value:index::string = '73' then vr.value:value::int end as base_fare,
							case when vr.value:index::string = '74' then vr.value:value::int end as fuel_srchg,
							case when vr.value:index::string = '75' then vr.value:value::int end as tax_value,
                          
                          
                           max(hit_no) over (partition by clientid, sessionid) as max_hit_no
								
							  
							from
							upsell_table as a1,
							lateral flatten( input =>  parse_json(a1.hits) ) as vm,
							--lateral flatten( input =>  vm.value:customDimensions ) as ve,
							--lateral flatten( input =>  vm.value:customDimensions ) as vx,
							lateral flatten( input =>  vm.value:customMetrics ) as vr
							where
							
							vm.value:type like '%TRANSACTION%' and
							--vm.value:hitNumber = 1 and
							--ve.value:index::string = '75' and
							--vx.value:index::string =  '59'  and
							--vr.value:index::string =  '3'  and
							  --vr.value:index::string =  '73'  and
							  --vr.value:index::string =  '74'  and
							  --vr.value:index::string =  '75'  and
							  vr.value:index::string in  ('3', '73',  '74', '75')  and
							-- ve.value:customDimensions.index::string = and
							1=1
                          group by
							a1.clientid,
							a1.sessionid,
							a1.date_part,
						    a1.hits,
						    a1.device_type,
							vm.value:type,
							--vm.value:page.pagePath as hit_type,
							vm.value:hitNumber,
                            vm.value:transaction.transactionRevenue/1000000,
							vm.value:transaction.transactionId,
							--ve.value:value::string as cd1,
							--vx.value:value::string as cd2,
							case when vr.value:index::string = '3' then vr.value:value::int end,
							case when vr.value:index::string = '73' then vr.value:value::int end,
							case when vr.value:index::string = '74' then vr.value:value::int end,
							case when vr.value:index::string = '75' then vr.value:value::int end
						),
	 lowest_fare_text as -- this where we get the lowest fare
						(
							select
							a1.clientid,
							a1.sessionid,
							a1.date_part,
							a1.hits,
							parse_json(a1.totals) as totals,
							(parse_json(a1.totals):transactionRevenue::int)/1000000 as revenue,
							vm.value:type as hit_type,
							vm.value:page.pagePath as hit_page,
							vm.value:hitNumber::int as hit_no,
							vm.value:transaction.transactionRevenue as hit_trns_revenue,
							vm.value:transaction.transactionId as hit_trns_id,
							--case when ve.value:index::string = '59'    then  ve.value:value end as lowest_fare_str,
							--case when ve.value:index::string ='75'   then  ve.value:value end as prd_cpn_code,
							ve.value:value as lowest_fare_str2
							--vx.value:value as prd_cpn_code2
							  
							  
							from
							upsell_table as a1,
							lateral flatten( input =>  parse_json(a1.hits) ) as vm,
							lateral flatten( input =>  vm.value:customDimensions ) as ve
							--lateral flatten( input =>  vm.value:customDimensions ) as vx
							  
							where
							
							--vm.value:hitNumber = 1 and
							--ve.value:index::string in ('59', '75' ) and
							ve.value:index::string =  '59'  and
							--vx.value:index::string =  '75'  and
							-- ve.value:customDimensions.index::string = and
							1=1    

						),

	 lowest_fare_substr as -- parsing the lower fare value to aggregate it
						(
							select
							a2.clientid,
							a2.sessionid,
							a2.date_part,
							a2.revenue,
							a2.hits,
						    a2.hit_no,
							a2.lowest_fare_str2,
							-- prd_cpn_code2,
							--cast(iff (len(substr( split_part( lowest_fare_str, '||', 2 ), 20)) < 1 , 0 ,substr( split_part( lowest_fare_str, '||', 2 ), 20)) as float) as lowst_fre,
							cast(substr( split_part( lowest_fare_str2, '||', 2 ), 20) as float) as lowest_fare,
							--case when substr( split_part( lowest_fare_str, '||', 2 ), 20) as lowest_fare 
							max(hit_no) over (partition by clientid ) as mx_hit_no
							from
							lowest_fare_text as a2
							where
							len(lowest_fare_str2 ) > 0 and
							 -- len(prd_cpn_code2 ) > 0 and
							substr( split_part( lowest_fare_str2, '||', 2 ), 20) rlike '[0.0-9.0]+$' and
							  1=1
							group by
							a2.clientid,
							a2.sessionid,
							a2.date_part,
							a2.revenue,
							a2.hits,
						    a2.hit_no,
							a2.lowest_fare_str2,
							-- prd_cpn_code2,
							substr( split_part( a2.lowest_fare_str2, '||', 2 ), 20)
						)
select -- final output with the joins
base_fare.date_part,
trunc(base_fare.date_part, 'MM') as cpm_mnth ,
base_fare.device_type,
transaction_type_details.tier_code,
transaction_type_details.trip_type,
-- transaction_type_details.trip_od,
transaction_type_details.class_brand,
transaction_type_details.CHANNELGROUPING,

count(distinct base_fare.sessionid) as  session_count,
count(distinct base_fare.clientid) as client_count,

sum(transaction_type_details.trans_count) as trans_count,

sum( nvl( base_fare.miles_paid,0)  ) as  miles_paid,
(sum( nvl( base_fare.base_fare , 0) ) + sum( nvl( base_fare.fuel_srchg, 0) ) + sum( nvl( base_fare.tax_value , 0) )) / 1000000 as total_fare,

sum(the_lowest_fare.lowest_fare) as lowest_fare,


case
when
round((sum( nvl( base_fare.base_fare , 0) ) + sum( nvl( base_fare.fuel_srchg, 0) ) + sum( nvl( base_fare.tax_value , 0) )) / 1000000,0) - round(sum(the_lowest_fare.lowest_fare),0) < 0 
then 0
else
round((sum( nvl( base_fare.base_fare , 0) ) + sum( nvl( base_fare.fuel_srchg, 0) ) + sum( nvl( base_fare.tax_value , 0) )) / 1000000,0) - round(sum(the_lowest_fare.lowest_fare),0)
end
as dlta_paid_to_lwst_fre
			
			
			
			
			
			from
			
				(		
				select
				aa1.clientid,
				aa1.sessionid,
				aa1.date_part,
				aa1.device_type,
				aa1.hit_no,
				aa1.max_hit_no,
				sum( nvl( aa1.miles_paid,0)  ) as  miles_paid,
				sum( nvl( aa1.base_fare , 0) ) as base_fare,
				sum( nvl( aa1.fuel_srchg, 0) ) as fuel_srchg,
				sum( nvl( aa1.tax_value , 0) ) as tax_value

				-- (sum( nvl( aa1.base_fare , 0) ) + sum( nvl( aa1.fuel_srchg, 0) ) + sum( nvl( aa1.tax_value , 0) )) / 1000000 as total_fare,
				from
				custom_metric_table as aa1
				where
				hit_no = max_hit_no
				group by
				aa1.clientid,
				aa1.sessionid,
				aa1.date_part,
				aa1.device_type,
				aa1.hit_no,
				aa1.max_hit_no
				) as base_fare
		
				left join
				 (
				
					select
					lowest_fare_substr.clientid,
					lowest_fare_substr.sessionid,
					lowest_fare_substr.date_part,
					cast(lowest_fare_substr.lowest_fare as float) as lowest_fare
					from lowest_fare_substr
					where
					hit_no = mx_hit_no
					
				 ) as the_lowest_fare on base_fare.clientid  = the_lowest_fare.clientid and base_fare.sessionid  =  the_lowest_fare.sessionid
                 
                 left join  transaction_type_details  on  base_fare.clientid = transaction_type_details.clientid and base_fare.sessionid = transaction_type_details.sessionid
group by
--base_fare.clientid,
base_fare.date_part,
trunc(base_fare.date_part, 'MM'),
--base_fare.sessionid,
base_fare.device_type,
--transaction_type_details.testing2,
transaction_type_details.tier_code,
transaction_type_details.trip_type,
-- transaction_type_details.trip_od,
transaction_type_details.class_brand,
transaction_type_details.CHANNELGROUPING