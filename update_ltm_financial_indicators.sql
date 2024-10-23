CREATE OR REPLACE PROCEDURE update_ltm_financial_indicators()
BEGIN
	-- выполняем логирование процедуры
    SET @startDaily := NOW();
    SET @nameDaily := 'update_ltm_financial_indicators';
    INSERT INTO _procedure_calls (name, start, finish) VALUES (@nameDaily, @startDaily, NULL);
    
    -- Отключаем строгие проверки (при необходимости)
    SET @original_sql_mode := @@sql_mode;
    SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION,ALLOW_INVALID_DATES';
   
	UPDATE ltm_financial_indicators AS lfi
    JOIN (
    	SELECT company_id,
		start_date,
		end_date,
		operatingIncome + depreciation as oibda,  
		ROUND(ebit * (1 - incomeTaxExpense/incomeBeforeTax)) as nopat,
		ROUND(netIncome / commonStockSharesOutstanding, 2) as eps,
		cashAndEquivalents - (totalCurrentLiabilities - totalCurrentAssets) as excess_cash,
		longTermDebt + totalStockholderEquity as invested_capital,
		totalAssets - totalCurrentLiabilities as employed_capital,
		ROUND(totalStockholderEquity / commonStockSharesOutstanding, 2) as book_value_per_share,
		totalCashFromOperatingActivities + totalCashflowsFromInvestingActivities + totalCashFromFinancingActivities as net_cash_flow,
		totalCashFromOperatingActivities - capitalExpenditures - 
			((SELECT nonCurrentLiabilitiesTotal from financial_statement_quarter fsq where lfi.start_date = fsq.date and lfi.company_id = fsq.company_id) -
				(SELECT nonCurrentLiabilitiesTotal from financial_statement_quarter fsq where lfi.end_date = fsq.date and lfi.company_id = fsq.company_id))
					as excess_cash_flow,
		capitalExpenditures - depreciation as net_capex 
	from ltm_financial_indicators lfi
    ) AS sub on lfi.company_id = sub.company_id and lfi.start_date = sub.start_date and lfi.end_date = sub.end_date
    SET lfi.oibda = sub.oibda,
    	lfi.nopat = sub.nopat,
    	lfi.eps = sub.eps,
    	lfi.excess_cash = sub.excess_cash,
    	lfi.invested_capital = sub.invested_capital,
    	lfi.employed_capital = sub.employed_capital,
    	lfi.book_value_per_share = sub.book_value_per_share,
    	lfi.net_cash_flow = sub.net_cash_flow,
    	lfi.excess_cash_flow = sub.excess_cash_flow,
    	lfi.net_capex = sub.net_capex;
    
    SET SESSION sql_mode = @original_sql_mode;
    	-- Обновление логирования процедуры после завершения
	UPDATE _procedure_calls SET finish = NOW() WHERE name = @nameDaily AND start = @startDaily;
END; 	
