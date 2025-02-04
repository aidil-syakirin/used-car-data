WITH dedup AS(
    SELECT 
        *, 
        ROW_NUMBER() OVER (PARTITION BY listing_id, price, car_model ORDER BY (SELECT NULL)) AS row_num
    FROM [dbo].[test_staging]
),
	cte as (
	SELECT * FROM dedup 
	WHERE row_num = 1
)
select 
	listing_id,
	case when car_model = 'myvi' then 'Myvi' 
		else car_model 
		end dim_car_model,
	price as fct_price,
	case when installment is null 
		then 
			round((CAST(REPLACE(REPLACE(price, 'RM ', ''), ',', '') AS NUMERIC) 
			* 0.0025 * POWER(1.0025, 108))/ (POWER(1.0025, 108) - 1),2)
		
		else cast(
		replace(REPLACE(REPLACE(installment, 'RM', ''), '/month', ''), ',', '')
		as numeric)
		end	AS fct_installment,
	year as fct_year,
	mileage as fct_mileage,
	color as dim_color,
	location as dim_location,
	state as dim_state,
	url,
	image,
	listing_image,
	cast (processing_date as date) as dim_processing_date
	from cte
