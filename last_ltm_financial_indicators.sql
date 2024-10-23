CREATE OR REPLACE PROCEDURE last_ltm_financial_indicators()
BEGIN
	-- выполняем логирование процедуры
    SET @startDaily := NOW();
    SET @nameDaily := 'last_ltm_financial_indicators';
    INSERT INTO _procedure_calls (name, start, finish) VALUES (@nameDaily, @startDaily, NULL);
    
    -- Отключаем строгие проверки (при необходимости)
    SET @original_sql_mode := @@sql_mode;
    SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION,ALLOW_INVALID_DATES';
   
	UPDATE ltm_financial_indicators AS lfi
    JOIN ( SELECT company_id,
    		`date`,			
			IF(accountsPayable IS NULL, 0, accountsPayable) AS accountsPayable,
			commonStockSharesOutstanding, 
			IF(inventory IS NULL, 0, inventory) AS inventory,
			IF(longTermDebt IS NULL, 0, longTermDebt) AS longTermDebt,
			IF(netReceivables IS NULL, 0, netReceivables) AS netReceivables,
			IF(nonCurrentAssetsTotal IS NULL, 0, nonCurrentAssetsTotal) AS nonCurrentAssetsTotal,
			IF(nonCurrentLiabilitiesTotal IS NULL, 0, nonCurrentLiabilitiesTotal) AS nonCurrentLiabilitiesTotal,
			IF(propertyPlantEquipment IS NULL, 0, propertyPlantEquipment) AS propertyPlantEquipment,
			IF(retainedEarnings IS NULL, 0, retainedEarnings) AS retainedEarnings,
			IF(shortTermDebt IS NULL, 0, shortTermDebt) AS shortTermDebt,
			IF(shortTermInvestments IS NULL, 0, shortTermInvestments) AS shortTermInvestments,
			IF(totalAssets IS NULL, 0, totalAssets) AS totalAssets,
			IF(totalCurrentAssets IS NULL, 0, totalCurrentAssets) AS totalCurrentAssets,
			IF(totalCurrentLiabilities IS NULL, 0, totalCurrentLiabilities) AS totalCurrentLiabilities,
			IF(totalLiab IS NULL, 0, totalLiab) AS totalLiab,
			IF(totalStockholderEquity IS NULL, 0, totalStockholderEquity) AS totalStockholderEquity,
			IF(cashAndEquivalents IS NULL, IFNULL(cash, 0), cashAndEquivalents) AS cashAndEquivalents,
			IF(netDebt IS NULL, longTermDebt + shortTermDebt - (IF(cashAndEquivalents IS NULL, IFNULL(cash, 0), cashAndEquivalents)), netDebt) AS netDebt,
			IF(debt IS NULL, longTermDebt + shortTermDebt, debt) AS debt, 
			totalCurrentAssets - totalCurrentLiabilities as net_working_capital
		FROM financial_statement_quarter fsq
		) AS sub on lfi.company_id = sub.company_id and lfi.end_date = sub.date
    SET lfi.accountsPayable = sub.accountsPayable,
    	lfi.commonStockSharesOutstanding = sub.commonStockSharesOutstanding,
    	lfi.inventory = sub.inventory,
    	lfi.longTermDebt = sub.longTermDebt,
    	lfi.netReceivables = sub.netReceivables,
    	lfi.nonCurrentAssetsTotal = sub.nonCurrentAssetsTotal,
    	lfi.nonCurrentLiabilitiesTotal = sub.nonCurrentLiabilitiesTotal,
    	lfi.propertyPlantEquipment = sub.propertyPlantEquipment,
    	lfi.retainedEarnings = sub.retainedEarnings,
    	lfi.shortTermDebt = sub.shortTermDebt,
    	lfi.shortTermInvestments = sub.shortTermInvestments,
    	lfi.totalAssets = sub.totalAssets,
    	lfi.totalCurrentAssets = sub.totalCurrentAssets,
    	lfi.totalCurrentLiabilities = sub.totalCurrentLiabilities,
    	lfi.totalLiab = sub.totalLiab,
    	lfi.totalStockholderEquity = sub.totalStockholderEquity,
    	lfi.cashAndEquivalents = sub.cashAndEquivalents,
    	lfi.netDebt = sub.netDebt,
    	lfi.debt = sub.debt,
    	lfi.net_working_capital = sub.net_working_capital;
    	-- Обновление логирования процедуры после завершения
	UPDATE _procedure_calls SET finish = NOW() WHERE name = @nameDaily AND start = @startDaily;
END;


call last_ltm_financial_indicators();