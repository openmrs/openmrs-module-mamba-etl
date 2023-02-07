
CREATE OR ALTER FUNCTION dbo.fn_staging_calculate_age(@earliest_date AS DATE, @latest_date AS DATE)
RETURNS INT
AS
BEGIN

	DECLARE @age INT
	DECLARE @earliest_date_as_yyyyddmm INT
	DECLARE @latest_date_as_yyyyddmm INT
	DECLARE @yyyyddmm_formatting_code INT = 112
	DECLARE @coefficient_for_converting_to_years INT = 10000

	SET @earliest_date_as_yyyyddmm = CONVERT ( CHAR(8), @earliest_date, @yyyyddmm_formatting_code)
	SET @latest_date_as_yyyyddmm = CONVERT ( CHAR(8), @latest_date, @yyyyddmm_formatting_code)
	
	SET @age = (@latest_date_as_yyyyddmm - @earliest_date_as_yyyyddmm) / @coefficient_for_converting_to_years
	
	RETURN @age

END