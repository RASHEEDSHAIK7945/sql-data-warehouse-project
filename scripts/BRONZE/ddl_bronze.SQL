/*

=====================================================================================================================
DDL SCRIPT : CREATE BRONZE TABLES

=====================================================================================================================
SCRIPT PURPOSE :
              THIS SCRIPT CREATES TABLES IN THE THE 'BRONZE' SCHEMA , DROPPING EXISTING TABLES THEY ALREADY EXIST.
              RUN THIS SCRIPT TO RE-DEFINE THE DDL STRUCTURE OF 'BRONZE' TABLES
              
=====================================================================================================================

*/

 IF OBJECT_ID ('BRONZE.CRM_cust_info','U') IS NOT NULL
  DROP TABLE BRONZE.CRM_cust_info;
  GO

  CREATE TABLE BRONZE.CRM_cust_info (
   cst_id               int,
   cst_key              nvarchar(50),
   cst_firstname        nvarchar(50),
   cst_lastname         nvarchar(50),
   cst_marital_status   nvarchar(50),
   cst_gndr             nvarchar(50),
   cst_create_date      date

   );
   
   GO

    IF OBJECT_ID ('BRONZE.CRM_prd_info','U') IS NOT NULL
     DROP TABLE BRONZE.CRM_prd_info;
     GO

     CREATE TABLE BRONZE.CRM_prd_info (
     prd_id           int,
     prd_key          nvarchar(50),
     prd_nm           nvarchar(50),
     prd_cost         int,
     prd_line         nvarchar(50),
     prd_start_dt     datetime,
     prd_end_dt       datetime
  );

  GO

   IF OBJECT_ID ('BRONZE.CRM_sales_details','U') IS NOT NULL
     DROP TABLE BRONZE.CRM_sales_details;
     GO

     CREATE TABLE BRONZE.CRM_sales_details (
     sls_ord_num          nvarchar(50),
     sls_prd_key          nvarchar(50),
     sls_cust_id          int,
     sls_order_dt         int,
     sls_ship_dt          int,
     sls_due_dt           int,
     sls_sales            int,
     sls_quantity         int,
     sls_price            int
  );

  GO

   IF OBJECT_ID ('BRONZE.ERP_loc_a101','U') IS NOT NULL
     DROP TABLE BRONZE.ERP_loc_a101;
     GO

     CREATE TABLE BRONZE.ERP_loc_a101 (
     cid           nvarchar(50),
     cntry         nvarchar(50)
  );

  GO 

     IF OBJECT_ID ('BRONZE.ERP_cust_az12','U') IS NOT NULL
     DROP TABLE BRONZE.ERP_cust_az12;
     GO

      CREATE TABLE BRONZE.ERP_cust_az12 (
      cid          nvarchar(50),
      bdate        date,
      gen          nvarchar(50)
  );

  GO 

     IF OBJECT_ID ('BRONZE.ERP_px_cat_g1v2','U') IS NOT NULL
     DROP TABLE BRONZE.ERP_px_cat_g1v2;
     GO

      CREATE TABLE BRONZE.ERP_px_cat_g1v2 (
      id             nvarchar(50),
      cat            nvarchar(50),
      subcat         nvarchar(50),
      maintenance    nvarchar(50)
  );
     

  

  
  
     


  
     
   







   
  
