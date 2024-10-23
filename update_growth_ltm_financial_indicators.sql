CREATE OR REPLACE PROCEDURE update_growth_ltm_financial_indicators()
BEGIN
	-- выполняем логирование процедуры
    SET @startDaily := NOW();
    SET @nameDaily := 'update_growth_ltm_financial_indicators';
    INSERT INTO _procedure_calls (name, start, finish) VALUES (@nameDaily, @startDaily, NULL);
    
    -- Отключаем строгие проверки (при необходимости)
    SET @original_sql_mode := @@sql_mode;
    SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION,ALLOW_INVALID_DATES';
	
   	UPDATE ltm_financial_indicators AS lfi
    JOIN ( 
			select  company_id,
				    start_date,
				    end_date,
			 		ROUND((totalRevenue / lag(totalRevenue) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as revenue_growth_1y,
			 		ROUND((totalRevenue / lag(totalRevenue, 3) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as revenue_growth_3y,
			 		ROUND((totalRevenue / lag(totalRevenue, 5) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as revenue_growth_5y,
			 		ROUND((totalRevenue / lag(totalRevenue, 10) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as revenue_growth_10y,
			 		ROUND((POWER(ABS(totalRevenue / lag(totalRevenue, 3) over(PARTITION by company_id order by end_date)), 1/3) - 1) * 100, 2) as revenue_cagr_3y,
			 		ROUND((POWER(ABS(totalRevenue / lag(totalRevenue, 5) over(PARTITION by company_id order by end_date)), 1/5) - 1) * 100, 2) as revenue_cagr_5y,
			 		ROUND((POWER(ABS(totalRevenue / lag(totalRevenue, 10) over(PARTITION by company_id order by end_date)), 1/10) - 1) * 100, 2) as revenue_cagr_10y,
			 		ROUND((ebitda / lag(ebitda) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as ebitda_growth_1y,
			 		ROUND((ebitda / lag(ebitda, 3) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as ebitda_growth_3y,
			 		ROUND((ebitda / lag(ebitda, 5) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as ebitda_growth_5y,
			 		ROUND((ebitda / lag(ebitda, 10) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as ebitda_growth_10y,
			 		ROUND((POWER(ABS(ebitda / lag(ebitda, 3) over(PARTITION by company_id order by end_date)), 1/3) - 1) * 100, 2) as ebitda_cagr_3y,
			 		ROUND((POWER(ABS(ebitda / lag(ebitda, 5) over(PARTITION by company_id order by end_date)), 1/5) - 1) * 100, 2) as ebitda_cagr_5y,
			 		ROUND((POWER(ABS(ebitda / lag(ebitda, 10) over(PARTITION by company_id order by end_date)), 1/10) - 1) * 100, 2) as ebitda_cagr_10y,
			 		ROUND((operatingIncome / lag(operatingIncome) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as operatingIncome_growth_1y,
			 		ROUND((operatingIncome / lag(operatingIncome, 3) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as operatingIncome_growth_3y,
			 		ROUND((operatingIncome / lag(operatingIncome, 5) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as operatingIncome_growth_5y,
			 		ROUND((operatingIncome / lag(operatingIncome, 10) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as operatingIncome_growth_10y,
			 		ROUND((POWER(ABS(operatingIncome / lag(operatingIncome, 3) over(PARTITION by company_id order by end_date)), 1/3) - 1) * 100, 2) as operatingIncome_cagr_3y,
			 		ROUND((POWER(ABS(operatingIncome / lag(operatingIncome, 5) over(PARTITION by company_id order by end_date)), 1/5) - 1) * 100, 2) as operatingIncome_cagr_5y,
			 		ROUND((POWER(ABS(operatingIncome / lag(operatingIncome, 10) over(PARTITION by company_id order by end_date)), 1/10) - 1) * 100, 2) as operatingIncome_cagr_10y,
			 		ROUND((netIncome / lag(netIncome) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as netIncome_growth_1y,
			 		ROUND((netIncome / lag(netIncome, 3) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as netIncome_growth_3y,
			 		ROUND((netIncome / lag(netIncome, 5) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as netIncome_growth_5y,
			 		ROUND((netIncome / lag(netIncome, 10) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as netIncome_growth_10y,
			 		ROUND((POWER(ABS(netIncome / lag(netIncome, 3) over(PARTITION by company_id order by end_date)), 1/3) - 1) * 100, 2) as netIncome_cagr_3y,
			 		ROUND((POWER(ABS(netIncome / lag(netIncome, 5) over(PARTITION by company_id order by end_date)), 1/5) - 1) * 100, 2) as netIncome_cagr_5y,
			 		ROUND((POWER(ABS(netIncome / lag(netIncome, 10) over(PARTITION by company_id order by end_date)), 1/10) - 1) * 100, 2) as netIncome_cagr_10y,
			 		ROUND(netIncome / commonStockSharesOutstanding, 2) as eps,
			 		ROUND((netIncome / commonStockSharesOutstanding / lag(netIncome / commonStockSharesOutstanding) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as eps_growth_1y,
			 		ROUND((netIncome / commonStockSharesOutstanding / lag(netIncome / commonStockSharesOutstanding, 3) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as eps_growth_3y,
			 		ROUND((netIncome / commonStockSharesOutstanding / lag(netIncome / commonStockSharesOutstanding, 5) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as eps_growth_5y,
			 		ROUND((netIncome / commonStockSharesOutstanding / lag(netIncome / commonStockSharesOutstanding, 10) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as eps_growth_10y,
			 		ROUND((POWER(ABS(netIncome / commonStockSharesOutstanding / lag(netIncome / commonStockSharesOutstanding, 3) over(PARTITION by company_id order by end_date)), 1/3) - 1) * 100, 2) as eps_cagr_3y,
			 		ROUND((POWER(ABS(netIncome / commonStockSharesOutstanding / lag(netIncome / commonStockSharesOutstanding, 5) over(PARTITION by company_id order by end_date)), 1/5) - 1) * 100, 2) as eps_cagr_5y,
			 		ROUND((POWER(ABS(netIncome / commonStockSharesOutstanding / lag(netIncome / commonStockSharesOutstanding, 10) over(PARTITION by company_id order by end_date)), 1/10) - 1) * 100, 2) as eps_cagr_10y,
			 		ROUND((debt / lag(debt) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as debt_growth_1y,
			 		ROUND((debt/ lag(debt, 3) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as debt_growth_3y,
			 		ROUND((debt / lag(debt, 5) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as debt_growth_5y,
			 		ROUND((debt / lag(debt, 10) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as debt_growth_10y,
			 		ROUND((POWER(ABS(debt / lag(debt, 3) over(PARTITION by company_id order by end_date)), 1/3) - 1) * 100, 2) as debt_cagr_3y,
			 		ROUND((POWER(ABS(debt / lag(debt, 5) over(PARTITION by company_id order by end_date)), 1/5) - 1) * 100, 2) as debt_cagr_5y,
			 		ROUND((POWER(ABS(debt / lag(debt, 10) over(PARTITION by company_id order by end_date)), 1/10) - 1) * 100, 2) as debt_cagr_10y,
			 		ROUND((netDebt / lag(netDebt) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as netDebt_growth_1y,
			 		ROUND((netDebt/ lag(netDebt, 3) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as netDebt_growth_3y,
			 		ROUND((netDebt / lag(netDebt, 5) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as netDebt_growth_5y,
			 		ROUND((netDebt / lag(netDebt, 10) over(PARTITION by company_id order by end_date) - 1) * 100, 2) as netDebt_growth_10y,
			 		ROUND((POWER(ABS(netDebt / lag(netDebt, 3) over(PARTITION by company_id order by end_date)), 1/3) - 1) * 100, 2) as netDebt_cagr_3y,
			 		ROUND((POWER(ABS(netDebt / lag(netDebt, 5) over(PARTITION by company_id order by end_date)), 1/5) - 1) * 100, 2) as netDebt_cagr_5y,
			 		ROUND((POWER(ABS(netDebt / lag(netDebt, 10) over(PARTITION by company_id order by end_date)), 1/10) - 1) * 100, 2) as netDebt_cagr_10y
			FROM ltm_financial_indicators lfi 
			) AS sub on lfi.company_id = sub.company_id and lfi.start_date = sub.start_date and lfi.end_date = sub.end_date
    SET lfi.revenue_growth_1y = sub.revenue_growth_1y,
		lfi.revenue_growth_3y = sub.revenue_growth_3y,
		lfi.revenue_growth_5y = sub.revenue_growth_5y,
		lfi.revenue_growth_10y = sub.revenue_growth_10y,
		lfi.revenue_cagr_3y = sub.revenue_cagr_3y,
		lfi.revenue_cagr_5y = sub.revenue_cagr_5y,
		lfi.revenue_cagr_10y = sub.revenue_cagr_10y,
		lfi.ebitda_growth_1y = sub.ebitda_growth_1y,
		lfi.ebitda_growth_3y = sub.ebitda_growth_3y,
		lfi.ebitda_growth_5y = sub.ebitda_growth_5y,
		lfi.ebitda_growth_10y = sub.ebitda_growth_10y,
		lfi.ebitda_cagr_3y = sub.ebitda_cagr_3y,
		lfi.ebitda_cagr_5y = sub.ebitda_cagr_5y,
		lfi.ebitda_cagr_10y = sub.ebitda_cagr_10y,
		lfi.operatingIncome_growth_1y = sub.operatingIncome_growth_1y,
		lfi.operatingIncome_growth_3y = sub.operatingIncome_growth_3y,
		lfi.operatingIncome_growth_5y = sub.operatingIncome_growth_5y,
		lfi.operatingIncome_growth_10y = sub.operatingIncome_growth_10y,
		lfi.operatingIncome_cagr_3y = sub.operatingIncome_cagr_3y,
		lfi.operatingIncome_cagr_5y = sub.operatingIncome_cagr_5y,
		lfi.operatingIncome_cagr_10y = sub.operatingIncome_cagr_10y,
		lfi.netIncome_growth_1y = sub.netIncome_growth_1y,
		lfi.netIncome_growth_3y = sub.netIncome_growth_3y,
		lfi.netIncome_growth_5y = sub.netIncome_growth_5y,
		lfi.netIncome_growth_10y = sub.netIncome_growth_10y,
		lfi.netIncome_cagr_3y = sub.netIncome_cagr_3y,
		lfi.netIncome_cagr_5y = sub.netIncome_cagr_5y,
		lfi.netIncome_cagr_10y = sub.netIncome_cagr_10y,
		lfi.eps_growth_1y = sub.eps_growth_1y,
		lfi.eps_growth_3y = sub.eps_growth_3y,
		lfi.eps_growth_5y = sub.eps_growth_5y,
		lfi.eps_growth_10y = sub.eps_growth_10y,
		lfi.eps_cagr_3y = sub.eps_cagr_3y,
		lfi.eps_cagr_5y = sub.eps_cagr_5y,
		lfi.eps_cagr_10y = sub.eps_cagr_10y,
		lfi.debt_growth_1y = sub.debt_growth_1y,
		lfi.debt_growth_3y = sub.debt_growth_3y,
		lfi.debt_growth_5y = sub.debt_growth_5y,
		lfi.debt_growth_10y = sub.debt_growth_10y,
		lfi.debt_cagr_3y = sub.debt_cagr_3y,
		lfi.debt_cagr_5y = sub.debt_cagr_5y,
		lfi.debt_cagr_10y = sub.debt_cagr_10y,
		lfi.netDebt_growth_1y = sub.netDebt_growth_1y,
		lfi.netDebt_growth_3y = sub.netDebt_growth_3y,
		lfi.netDebt_growth_5y = sub.netDebt_growth_5y,
		lfi.netDebt_growth_10y = sub.netDebt_growth_10y,
		lfi.netDebt_cagr_3y = sub.netDebt_cagr_3y,
		lfi.netDebt_cagr_5y = sub.netDebt_cagr_5y,
		lfi.netDebt_cagr_10y = sub.netDebt_cagr_10y;

    -- Обновление логирования процедуры после завершения
	UPDATE _procedure_calls SET finish = NOW() WHERE name = @nameDaily AND start = @startDaily;
END;


call update_growth_ltm_financial_indicators();