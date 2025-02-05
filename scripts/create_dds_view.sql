CREATE MATERIALIZED VIEW dds.d_valute
DISTRIBUTED BY HASH(`hk_valute`)
REFRESH ASYNC START('2025-02-03 21:00:00') EVERY (interval 1 day)
AS 
select 
	distinct
	murmur_hash3_32(replace(value -> 'CharCode', '\"', '')) as hk_valute,
	replace(value -> 'ID', '\"', '') as id_valute,
	replace(value -> 'CharCode', '\"', '') as char_code,
	replace(value -> 'Name', '\"', '') as name,
	cast(replace(value -> 'Nominal', '\"', '') as int) as nominal,
	cast(replace(value -> 'NumCode', '\"', '') as int) as num_code
from (
	select 
		parse_json(replace(value, '\'', '\"'))->'Valute' as valute
	from stg.exchange_rates 
	where value not like '%error%'
) as K, LATERAL json_each(valute) as T;

CREATE MATERIALIZED VIEW dds.f_exchange_valute
DISTRIBUTED BY HASH(`hk_valute`)
REFRESH ASYNC START('2025-02-03 21:00:00') EVERY (interval 1 day)
AS 
select 
	D.hk_valute as hk_valute,
	S.dt_value as date,
	S.value,
	S.previous	
from (
select 
	replace(value -> 'CharCode', '\"', '') as char_code,
	cast(replace(value -> 'Previous', '\"', '') as numeric(10,4)) as previous,
	cast(replace(value -> 'Value', '\"', '') as numeric(10,4)) as value,
	dt_value
from (
	select 
		parse_json(replace(value, '\'', '\"'))->'Valute' as valute,
		dt_value
	from stg.exchange_rates 
	where value not like '%error%'
) as K, LATERAL json_each(valute) as T
) as S
	inner join dds.d_valute D on S.char_code = D.char_code;