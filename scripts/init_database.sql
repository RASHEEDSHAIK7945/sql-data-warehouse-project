/*
-----------------------------------------------------------------------------------------

CREATE DATABASE AND SCHEMAS

-----------------------------------------------------------------------------------------
Script Purpose :
          This script create a new database named 'DATAWAREHOUSE' after checking if it already exists.
          If the database exists , it is dropped and recreated . Additional , the script sets up three schemas
          within the database : 'BRONZE' , 'SILVER' , and 'GOLD' .

WARNING:
      Running this script will drop the entire 'DATAWAREHOUSE' database if it exists.
      All data in the database will be permanently deleted. Proceed with caution
      and ensure you have proper backups before running this script.
*/

USE master;
GO

--Drop and recreate the 'DATAWAREHOUSE' database
if exists (select 1 from sys.databases where name = 'DATAWAREHOUSE')
begin
      alter database DATAWAREHOUSE set single_user with rollback immediate;
end;
go

--create the 'DATAWAREHOUSE' database
create database DATAWAREHOUSE;
go

use DATAWAREHOUSE;
go

---- create Schemas
create Schema BRONZE;
go

create Schema SILVER;
go

create Schema GOLD;
go
