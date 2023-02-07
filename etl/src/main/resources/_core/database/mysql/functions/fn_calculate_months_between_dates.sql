
CREATE OR ALTER FUNCTION dbo.fn_calculate_months_between_dates(@earliest_date AS DATE, @latest_date AS DATE)
RETURNS INT
WITH RETURNS NULL ON NULL INPUT
AS
BEGIN

	DECLARE @average_days_in_a_month FLOAT = 30.436875E
	DECLARE @total_days INT
	DECLARE @months INT

	SET @total_days = DATEDIFF(DAY, @earliest_date, @latest_date)
	SET @months = @total_days / @average_days_in_a_month
	
	RETURN @months

END