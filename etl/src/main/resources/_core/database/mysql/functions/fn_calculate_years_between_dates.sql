
CREATE OR ALTER FUNCTION dbo.fn_calculate_years_between_dates(@earliest_date AS DATE, @latest_date AS DATE)
RETURNS INT
WITH RETURNS NULL ON NULL INPUT
AS
BEGIN

	DECLARE @average_days_in_a_year FLOAT = 365.25
	DECLARE @total_days INT
	DECLARE @years INT

	SET @total_days = DATEDIFF(DAY, @earliest_date, @latest_date)
	SET @years = @total_days / @average_days_in_a_year
	
	RETURN @years

END