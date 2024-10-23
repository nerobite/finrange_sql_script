CREATE OR REPLACE PROCEDURE `updateFinancialStatement`()
BEGIN
    set @start := NOW();
    set @name := 'updateFinancialStatement';
    insert into _procedure_calls (name, start, finish) value (@name, @start, null);
	
   	-- Отключаем строгие проверки (при необходимости)
    SET @original_sql_mode := @@sql_mode;
    SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION,ALLOW_INVALID_DATES';
    SET sql_mode = '';
   
    UPDATE financial_statement_quarter
	SET 
	    debt = COALESCE(longTermDebt, 0) + COALESCE(shortTermDebt, 0),
	    netDebt = COALESCE(debt, 0) - COALESCE(IF(cash is null, cashAndEquivalents, cash), 0),
	    ebitda = COALESCE(incomeBeforeTax, 0) + COALESCE(interestExpense, 0) + COALESCE(interestIncome, 0) + COALESCE(depreciation, 0),
	    ebit = COALESCE(incomeBeforeTax, 0) + COALESCE(interestExpense, 0) + COALESCE(interestIncome, 0),
	    netDebtEbitda = IF(ebitda IS NULL OR ebitda = 0, 0, netDebt / ebitda),
	    netCashFlow = COALESCE(totalCashFromOperatingActivities, 0) + COALESCE(totalCashFromFinancingActivities, 0) + COALESCE(totalCashflowsFromInvestingActivities, 0),
	    earningsPerShare = IF(commonStockSharesOutstanding IS NULL OR commonStockSharesOutstanding = 0, 0, netIncome / commonStockSharesOutstanding),
	    capitalExpendituresTotalRevenue = IF(totalAssets IS NULL OR totalRevenue = 0, 0, ROUND(capitalExpenditures / totalRevenue * 100, 2)),
	    freeCashFlow = COALESCE(totalCashFromOperatingActivities, 0) - COALESCE(capitalExpenditures, 0),
	    oibda = COALESCE(operatingIncome, 0) + COALESCE(depreciation, 0),
	    nopat = ROUND(ebit * (1 - incomeTaxExpense / incomeBeforeTax)),
	    excess_cash = IF(cash is null, cashAndEquivalents, cash) - (0.5 * totalRevenue),
	    netWorkingCapital = IF(netWorkingCapital is null, COALESCE(totalCurrentAssets, 0) - COALESCE(totalCurrentLiabilities, 0), netWorkingCapital),
	    net_capex = ROUND(COALESCE(capitalExpenditures, 0) - COALESCE(depreciation, 0));
          
	UPDATE financial_statement_quarter as fsq
    JOIN(	
        SELECT company_id, `date`,
        ROUND(totalCashFromOperatingActivities - capitalExpenditures - 
						nonCurrentLiabilitiesTotal - 
						lag(nonCurrentLiabilitiesTotal) over(partition by company_id order by `date`)) as excess_cash_flow
	FROM financial_statement_quarter fsq
        ) as sub on sub.company_id = fsq.company_id and sub.`date` = fsq.`date`
    SET fsq.excess_cash_flow = sub.excess_cash_flow;    
        
    UPDATE financial_statement_annual
	SET 
	    debt = COALESCE(longTermDebt, 0) + COALESCE(shortTermDebt, 0),
	    netDebt = COALESCE(debt, 0) - COALESCE(cash, 0),
	    ebitda = COALESCE(incomeBeforeTax, 0) + COALESCE(interestExpense, 0) + COALESCE(interestIncome, 0) + COALESCE(depreciation, 0),
	    netDebtEbitda = IF(ebitda IS NULL OR ebitda = 0, 0, netDebt / ebitda),
	    netCashFlow = COALESCE(totalCashFromOperatingActivities, 0) + COALESCE(totalCashFromFinancingActivities, 0) + COALESCE(totalCashflowsFromInvestingActivities, 0),
	    earningsPerShare = IF(commonStockSharesOutstanding IS NULL OR commonStockSharesOutstanding = 0, 0, netIncome / commonStockSharesOutstanding),
	    capitalExpendituresTotalRevenue = IF(totalAssets IS NULL OR totalRevenue = 0, 0, ROUND(capitalExpenditures / totalRevenue * 100, 2)),
	    freeCashFlow = COALESCE(totalCashFromOperatingActivities, 0) - COALESCE(capitalExpenditures, 0);
	
    SET SESSION sql_mode = @original_sql_mode;
   
    UPDATE _procedure_calls SET finish=NOW() where name=@name and start=@start;
END

call updateFinancialStatement();