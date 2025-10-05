/*
================================================================================================================

STORED PROCEDURE : LOAD BRONZE LAYER (SOURCE -> BRONZE)

================================================================================================================

SCRIPT PURPOSE :
          THIS STORED PROCEDURE LOADS DATA INTO THE 'BRONZE' SCHEMA FROM EXTERNAL CSV FILES.
          IT PERFORMS THE FOLLWING ACTIONS :
           - TRUNCATES THE BRONZE TABLES BEFORE LOADING DATA .
           - USES THE BULK INSERT COMMNAND  TO LOAD DATA FROM CSV FILES TO BRONZE TABLES .

PARAMETERS :
        NONE.
        THIS STORED PROCEDURE DOES NOT ACCEPT ANY PARAMETERS OR RETURN ANY VALUES.

USAGE EXAMPLE :
        EXEC BRONZE.load_bronze;

================================================================================================================

/*

create or alter procedure BRONZE.load_bronze as
begin
     DECLARE @Start_Time  DATETIME, @End_Time DATETIME , @Batch_Start_Time  DATETIME, @Batch_End_Time DATETIME;
	begin try
		set  @Batch_Start_Time = getdate();
        print '==============================================';
		print 'Loading Bronze Layer';
		print '==============================================';

		print '------------------------------------------------';
		print 'Loading CRM Tables';
		print '------------------------------------------------';

			SET @Start_Time = GETDATE();
			print '>> Truncting Table : bronze.crm_cust_info';
			truncate table BRONZE.CRM_cust_info;

			print '>> Inserting Data Into : bronze.crm_cust_info';
			BULK INSERT BRONZE.CRM_cust_info
			from 'C:\Users\DELL\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			with (
				FirstRow = 2,
				FieldTerminator = ',',
				tablock
			) ;
			SET @End_Time = GETDATE();
			print '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@Start_Time,@End_Time) as nvarchar) + 'seconds' ;
			print '>> --------';

		
			SET @Start_Time = GETDATE();
			print '>> Truncting Table : bronze.crm_prd_info';
			truncate table BRONZE.CRM_prd_info;

			print '>> Inserting Data Into : bronze.crm_prd_info';
			BULK INSERT BRONZE.CRM_prd_info
			from 'C:\Users\DELL\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			with (
				FirstRow = 2,
				FieldTerminator = ',',
				tablock
			) ;
			SET @End_Time = GETDATE();
			print '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@Start_Time,@End_Time) as nvarchar) + 'seconds' ;
			print '>> --------';

			SET @Start_Time = GETDATE();
			print '>> Truncting Table : bronze.crm_sales_details';
			truncate table BRONZE.CRM_sales_details;

			print '>> Inserting Data Into : bronze.crm_sales_details';
			BULK INSERT BRONZE.CRM_sales_details
			from 'C:\Users\DELL\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			with (
				FirstRow = 2,
				FieldTerminator = ',',
				tablock
			) ;
			SET @End_Time = GETDATE();
			print '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@Start_Time,@End_Time) as nvarchar) + 'seconds' ;
			print '>> --------';

		print '------------------------------------------------';
		print 'Loading ERP Tables';
		print '------------------------------------------------';

			SET @Start_Time = GETDATE();
			print '>> Truncting Table : bronze.erp_cust_az12';
			truncate table BRONZE.ERP_cust_az12;

			print '>> Inserting Data Into : bronze.erp_cust_az12';
			BULK INSERT BRONZE.ERP_cust_az12
			from 'C:\Users\DELL\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
			with (
				FirstRow = 2,
				FieldTerminator = ',',
				tablock
			) ;
			SET @End_Time = GETDATE();
			print '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@Start_Time,@End_Time) as nvarchar) + 'seconds' ;
			print '>> --------';

			SET @Start_Time = GETDATE();
			print '>> Truncting Table : bronze.erp_loc_a101';
			truncate table BRONZE.ERP_loc_a101;

			print '>> Inserting Data Into : bronze.erp_loc_a101';
			BULK INSERT BRONZE.ERP_loc_a101
			from 'C:\Users\DELL\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
			with (
					FirstRow = 2,
					FieldTerminator = ',',
					tablock
			);
			SET @End_Time = GETDATE();
			print '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@Start_Time,@End_Time) as nvarchar) + 'seconds' ;
			print '>> --------';


			SET @Start_Time = GETDATE();
			print '>> Truncting Table : bronze.erp_px_cat_g1v2';
			truncate table BRONZE.ERP_px_cat_g1v2;
			print '>> Inserting Data Into : bronze.erp_px_cat_g1v2';
			BULK INSERT BRONZE.ERP_px_cat_g1v2
			from 'C:\Users\DELL\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
			with (
				FirstRow = 2,
				FieldTerminator = ',',
				tablock
			);
			SET @End_Time = GETDATE();
			print '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@Start_Time,@End_Time) as nvarchar) + 'seconds' ;
			print '>> --------';

			SET @Batch_End_Time = GETDATE();
			print '=================================='
			print 'Loading Bronze Layer is completed';
			print ' - Total Load Duration: ' + cast (datediff(second ,@batch_Start_Time , @batch_End_Time) as nvarchar) + 'seconds'
			print '===================================='

		end try
		begin catch
		      print '=============================================='
			  print 'ERROR OCCURED DURING LOADING BRONZE LAYER'
			  print 'Error Message' + ERROR_MESSAGE();
			  print 'Error Message' + cast (ERROR_NUMBER() AS NVARCHAR);
			  print 'Error Message' + cast (ERROR_STATE() AS NVARCHAR);
			  print '=============================================='

		end catch

end
