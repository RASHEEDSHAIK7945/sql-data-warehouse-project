/*
==================================================================================================
Stored Procedure: Load SILVER Layer (BRONZE -> SILVER)
==================================================================================================
Script Purpose :
          This stored procedure performs the ETL (Extract, Transform, Load) process to
          populate the 'SILVER' schema table from the 'BRONZE' schema.
        Actions Performed :
          - Truncates SILVER tables.
          - Insert transformed and cleansed data from BRONZE into SILVER tables.


        Parameters:
             None.
             This stored procedure does not accept any parameters or return any values.

        Usage Example:
              EXEC SILVER.load_silver;
===================================================================================================
*/

create or alter procedure silver.load_silver as
begin
	DECLARE @start_time DATETIME , @end_time DATETIME , @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
	SET @batch_start_time = GETDATE();
	print '=====================================================';
	print ' Loading Silver Layer';
	print '=====================================================';

	print '------------------------------------------------------';
	print 'Loading CRM Tables';
	print '-------------------------------------------------------';

	--Loading SILVER.CRM_cust_info
	SET @start_time = GETDATE();
	print '>> Truncating Table : SILVER.CRM_cust_info';
	Truncate Table SILVER.CRM_cust_info;
	print '>> Insertng Data Into : SILVER.CRM_cust_info';
	insert into SILVER.CRM_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
	)
	select 
	cst_id,
	cst_key,
  trim(cst_firstname) as cst_firstname,
	trim(cst_lastname) as cst_lastname,
	case
			when upper(trim(cst_material_status)) = 'S' then 'Single'
			when upper(trim(cst_material_status)) = 'M' then 'Married'
			else 'n/a'
      end as cst_marital_status, --normalize marital status values to readable foramt
	case 
			when upper(trim(cst_gndr)) = 'F' THEN 'Female'
			when upper(trim(cst_gndr)) = 'M' then 'Male'
			else 'n/a'
	    end as cst_gndr, --normalize gender values to readbale format
			cst_create_date
			from (
				select 
				*,
				row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
				from BRONZE.CRM_cust_info
				where cst_id is not null
			)t
			where flag_last =1; -- select the most recent record per customer
			SET @end_time = GETDATE();
			print '>> Load Duration:' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds '
			print '>> --------------';

	----Loading SILVER.CRM_prd_info
	SET @start_time = GETDATE();
	print '>> Truncating Table :SILVER.CRM_prd_info';
	Truncate Table SILVER.CRM_prd_info;
	print '>> Inserting Data Into:SILVER.CRM_prd_info';
	INSERT INTO SILVER.CRM_prd_info (
	prd_id,  
	cat_id ,   
	prd_key , 
	prd_nm   , 
	prd_cost  ,
	prd_line ,
	prd_start_dt ,
	prd_end_dt  
	)
	select 
		prd_id,
		replace(substring(prd_key ,1,5) , '-','_') as cat_id, --Extract category ID
		substring(prd_key,7,Len(prd_key)) as prd_key,
		prd_nm,
		isnull(prd_cost,0) as prd_cost,
		case 
			when upper(trim(prd_line)) = 'M' then 'Mountain'
			when upper(trim(prd_line)) = 'R' then 'Road'
			when upper(trim(prd_line)) = 'S' then 'Other Sales'
			when upper(trim(prd_line)) = 'T' then 'Touring'
			else 'n/a'
		End as prd_line , -- map product line codes to descriptive values
		cast(prd_start_dt as date) as prd_start_dt,
		cast(
				lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) -1
				as date
				) as prd_end_dt -- calculate end date as one day before the next start date
		from BRONZE.CRM_prd_info;
		SET @end_time = GETDATE();
    print '>> Load Duration:' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds '
		print '>> --------------';
    ----Loading SILVER.CRM_sales_deatils
	SET @start_time = GETDATE();
	print '>> Truncating Table :SILVER.CRM_sales_details';
	Truncate Table SILVER.CRM_sales_details;
	print '>> Inserting Data Into: SILVER.CRM_sales_details';
	insert into SILVER.CRM_sales_details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)


			select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
	  	case 
      when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
			else cast (cast(sls_order_dt as varchar ) as date )
	  	end as sls_order_dt,
	  	case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null 
			else cast(cast(sls_ship_dt as varchar) as date)
			end as sls_ship_dt,
	  	case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null 
			else cast(cast(sls_due_dt as varchar) as date)
			end as sls_due_dt,
	  	case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
			then sls_quantity * abs(sls_price)
			else sls_sales
			end as sls_sales,
			sls_quantity,
		  case when sls_price is null or sls_price <= 0
			then sls_sales/nullif (sls_quantity,0)
			else sls_price
			end as sls_price
			from BRONZE.CRM_sales_details;
			SET @end_time = GETDATE();
			print '>> Load Duration:' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds '
			print '>> --------------';

    ----Loading SILVER.ERP_cust_az12
	SET @start_time = GETDATE();
	print '>> Truncating Table :SILVER.ERP_cust_az12';
	Truncate Table SILVER.ERP_cust_az12;
	print '>> Inserting Data Into: SILVER.ERP_cust_az12';
	insert into SILVER.ERP_cust_az12 (
		cid,
		bdate,
		gen
	)
	select 
	case when cid like 'NASA%' then substring (cid,4,len(cid))
	else cid
	end as cid,
	case when bdate > getdate() then null
	else bdate
	end as bdate,
	case when upper(trim(gen)) in ('F','FEMALE') THEN 'Female'
	 when upper(trim(gen)) in ('M' , 'MALE') then 'Male'
	 else 'n/a'
	 end as gen
			 from BRONZE.ERP_cust_az12;
			 SET @end_time = GETDATE();
			 print '>> Load Duration:' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds '
			 print '>> --------------';

	----Loading SILVER.ERP_loc_a101
	SET @start_time = GETDATE();
	print '>> Truncating Table :SILVER.ERP_loc_a101';
	Truncate Table SILVER.ERP_loc_a101;
	print '>> Insert Data Into:SILVER.ERP_loc_a101';
	INSERT INTO SILVER.ERP_loc_a101 (
		cid,
		cntry
	)
	select
	replace (cid, '-','') cid,
	case when trim(cntry) = 'DE' THEN 'Germany'
		 when trim(cntry) in ('US','USA') THEN 'United States'
		 when trim(cntry) = '' or cntry is null then 'n/a' 
		 else trim(cntry)
		 end as cntry
			 from BRONZE.ERP_loc_a101;
			 SET @end_time = GETDATE();
			 print '>> Load Duration:' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds '
			 print '>> --------------';
	----Loading SILVER.ERP_px_cat_g1v2
	SET @start_time = GETDATE();
	print '>> Truncating Table :SILVER.ERP_px_cat_g1v2';
	Truncate Table SILVER.ERP_px_cat_g1v2;
	print '>> Inserting  Data Into: SILVER.ERP_px_cat_g1v2';
	insert into SILVER.ERP_px_cat_g1v2 (
		id,
		cat,
		subcat,
		maintenance
	)
	select 
		id,
		cat,
		subcat,
		maintenance
			from BRONZE.ERP_px_cat_g1v2;
			SET @end_time = GETDATE();
			print '>> Load Duration:' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds ';
			print '>> --------------';

			SET @batch_end_time =GETDATE();
			print '==============================================='
			print 'Loading SILVER Layer is Completed';
			print ' - Total Load Duration : ' + cast(datediff(second,@batch_start_time , @batch_end_time) as nvarchar) + ' seconds ';
			print '==============================================='

		END TRY
		BEGIN CATCH
				print '==============================================='
				print ' ERROR OCCURED DURING LOADING BRONZE LAYER'
				print 'Error Message' + Error_Message();
				print 'Error Message' + cast(Error_Number() as nvarchar);
				print 'Error Message' + cast (Error_State() as nvarchar);
				print '================================================='
			END CATCH
End
	
