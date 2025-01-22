with row_num as (
		select *, row_number() over (partition by listing_id, car_model order by listing_id) as row_num_col
		 from [dbo].[test_staging] 
)

select * from row_num
