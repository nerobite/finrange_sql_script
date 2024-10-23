CREATE OR REPLACE PROCEDURE `update_financial_statement_quarter`()
BEGIN

    set @start := NOW();
    set @name := 'update_financial_statement_quarter';
    SET sql_mode = '';
    insert into _procedure_calls (name, start, finish) value (@name, @start, null);
   
   	-- Отключаем строгие проверки (при необходимости)
    SET @original_sql_mode := @@sql_mode;
    SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION,ALLOW_INVALID_DATES';
	
	UPDATE financial_statement_quarter as fsq
    JOIN(	
        SELECT company_id, `date`,
                operatingIncome + depreciation as oibda,
                IF(ebitda is null, incomeBeforeTax + interestExpense + interestIncome + depreciation, ebitda) as ebitda,
                IF(ebit is null, incomeBeforeTax + interestExpense + interestIncome, ebit) as ebit,
                ROUND(ebit * (1 - incomeTaxExpense / incomeBeforeTax)) as nopat,
                ROUND(IF(earningsPerShare is null, netIncome / NULLIF(commonStockSharesOutstanding, 0), earningsPerShare), 2) as earningsPerShare,
                IF(cash is null, cashAndEquivalents, cash) - (0.5 * totalRevenue) as excess_cash,
                IF(debt is null, longTermDebt + shortTermDebt, debt) as debt,
                IF(netDebt  is null, debt -  IF(cash is null, cashAndEquivalents, cash), netDebt) as netDebt,
                IF(netWorkingCapital is null, totalCurrentAssets - totalCurrentLiabilities, netWorkingCapital) as netWorkingCapital,
                ROUND(capitalExpenditures - depreciation) as net_capex,
                IF(netCashFlow is null, totalCashFromOperatingActivities + totalCashFromFinancingActivities +
						                totalCashflowsFromInvestingActivities, netCashFlow) as netCashFlow,
                ROUND(totalCashFromOperatingActivities - capitalExpenditures - 
						nonCurrentLiabilitiesTotal - 
						lag(nonCurrentLiabilitiesTotal) over(partition by company_id order by `date`)) as excess_cash_flow,
				IF(freeCashFlow is null, totalCashFromOperatingActivities - capitalExpenditures, freeCashFlow) as freeCashFlow
        FROM financial_statement_quarter fsq
        ) as sub on sub.company_id = fsq.company_id and sub.`date` = fsq.`date` 
	SET fsq.oibda = sub.oibda,	
	    fsq.ebitda = sub.ebitda,
	    fsq.ebit = sub.ebit,
	    fsq.nopat = sub.nopat,
	    fsq.earningsPerShare = sub.earningsPerShare,
	    fsq.excess_cash = sub.excess_cash,
	    fsq.debt = sub.debt,
	    fsq.netDebt = sub.netDebt,
	    fsq.netWorkingCapital = sub.netWorkingCapital,
	    fsq.net_capex = sub.net_capex,
	    fsq.netCashFlow = sub.netCashFlow,
	    fsq.excess_cash_flow = sub.excess_cash_flow,
	    fsq.freeCashFlow = sub.freeCashFlow;
	   
    SET SESSION sql_mode = @original_sql_mode;
  
    update _procedure_calls SET finish=NOW() where name = @name and start = @start;
END;


CALL update_financial_statement_quarter();
	
