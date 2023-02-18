-- Clean integers (INT64) replacing NULL with -999999
CREATE OR REPLACE FUNCTION keepcoding.clean_integer (integer_64 INT64) 
RETURNS INT64
AS (
    (SELECT IFNULL(integer_64, -999999))
);