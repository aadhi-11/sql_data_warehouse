/*
=============================================================
Create Data Warehouse Database and Schemas
=============================================================
Purpose:
    This script rebuilds the 'DataWarehouse' database and
    initializes the Medallion Architecture schemas:
    - bronze
    - silver
    - gold

Behavior:
    - If the database already exists, it is forced into
      SINGLE_USER mode and dropped.
    - A fresh database is created.
    - Required schemas are initialized.

WARNING:
    Executing this script will permanently delete the
    'DataWarehouse' database and all contained data.
    Ensure backups are taken before execution.
=============================================================
*/

USE master;
GO

/*------------------------------------------------------------
 Drop existing DataWarehouse database (if present)
------------------------------------------------------------*/
IF EXISTS (
    SELECT 1
    FROM sys.databases
    WHERE name = 'DataWarehouse'
)
BEGIN
    ALTER DATABASE DataWarehouse
    SET SINGLE_USER
    WITH ROLLBACK IMMEDIATE;

    DROP DATABASE DataWarehouse;
END
GO

/*------------------------------------------------------------
 Create fresh DataWarehouse database
------------------------------------------------------------*/
CREATE DATABASE DataWarehouse;
GO

/*------------------------------------------------------------
 Switch context to DataWarehouse
------------------------------------------------------------*/
USE DataWarehouse;
GO

/*------------------------------------------------------------
 Create Medallion Architecture schemas
------------------------------------------------------------*/
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
