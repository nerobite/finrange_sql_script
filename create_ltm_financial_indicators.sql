CREATE OR REPLACE PROCEDURE create_ltm_financial_indicators()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE n INT DEFAULT 1;
    DECLARE max_rn INT;
    DECLARE current_company_id INT;

    -- Объявляем курсор для обхода всех компаний
    DECLARE company_cursor CURSOR FOR 
        SELECT DISTINCT company_id FROM financial_statement_quarter;

    -- Обработка конца данных в курсоре
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
   
	-- выполняем логирование процедуры до открытия курсора
    SET @startDaily := NOW();
    SET @nameDaily := 'create_ltm_financial_indicators';
    INSERT INTO _procedure_calls (name, start, finish) VALUES (@nameDaily, @startDaily, NULL);
  
    -- Открываем курсор
    OPEN company_cursor;

     -- Начало цикла обработки компаний
    company_loop: LOOP
        FETCH company_cursor INTO current_company_id;
        IF done THEN
            LEAVE company_loop;
        END IF;
        -- Получаем максимальное значение rn для текущей компании
        SELECT MAX(rn) INTO max_rn 
        FROM (
            SELECT ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY date DESC) AS rn
            FROM financial_statement_quarter fsq
            JOIN companies c on fsq.company_id = c.id
            WHERE fsq.date >= DATE_SUB(CURDATE(), INTERVAL 10 YEAR) and company_id = current_company_id
        ) AS ranked;

        -- Сбрасываем начальное значение n для каждой компании
        SET n = 1;

        -- Цикл для группировки данных по 4 строки для текущей компании
        WHILE n + 3 <= max_rn DO
            INSERT INTO ltm_financial_indicators (company_id,
				start_date,
				end_date,
				currency_symbol,
				operatingIncome,
				capitalExpenditures,
				depreciation,
				salePurchaseOfStock,
				totalCashFromFinancingActivities,
				totalCashFromOperatingActivities,
				totalCashflowsFromInvestingActivities,
				costOfRevenue,
				ebit,
				grossProfit,
				incomeBeforeTax,
				incomeTaxExpense,
				interestExpense,
				interestIncome,
				netIncome,
				nonOperatingIncomeNetOther,
				totalOperatingExpenses,
				totalRevenue,
				ebitda,
				freeCashFlow)
            SELECT 
                company_id,
                MIN(date) AS start_date,
                MAX(date) AS end_date,
                MAX(currency_symbol) AS currency_symbol,
                SUM(IF(operatingIncome IS NULL, 0, operatingIncome)) AS operatingIncome,
                SUM(IF(capitalExpenditures IS NULL, 0, capitalExpenditures)) AS capitalExpenditures,
                SUM(IF(depreciation IS NULL, 0, depreciation)) AS depreciation,
                SUM(IF(salePurchaseOfStock IS NULL, 0, salePurchaseOfStock)) AS salePurchaseOfStock,
                SUM(IF(totalCashFromFinancingActivities IS NULL, 0, totalCashFromFinancingActivities)) AS totalCashFromFinancingActivities,
                SUM(IF(totalCashFromOperatingActivities IS NULL, investments, totalCashFromOperatingActivities)) AS totalCashFromOperatingActivities,
                SUM(IF(totalCashflowsFromInvestingActivities IS NULL, 0, totalCashflowsFromInvestingActivities)) AS totalCashflowsFromInvestingActivities,
                SUM(IF(costOfRevenue IS NULL, 0, costOfRevenue)) AS costOfRevenue,
                SUM(IF(ebit IS NULL, incomeBeforeTax + interestExpense + interestIncome, ebit)) AS ebit,
                SUM(IF(grossProfit IS NULL, 0, grossProfit)) AS grossProfit,
                SUM(IF(incomeBeforeTax IS NULL, 0, incomeBeforeTax)) AS incomeBeforeTax,
                SUM(IF(incomeTaxExpense IS NULL, 0, incomeTaxExpense)) AS incomeTaxExpense,
                SUM(IF(interestExpense IS NULL, 0, interestExpense)) AS interestExpense,
                SUM(IF(interestIncome IS NULL, 0, interestIncome)) AS interestIncome,
                SUM(IF(netIncome IS NULL, 0, netIncome)) AS netIncome,
                SUM(IF(nonOperatingIncomeNetOther IS NULL, 0, nonOperatingIncomeNetOther)) AS nonOperatingIncomeNetOther,
                SUM(IF(totalOperatingExpenses IS NULL, 0, totalOperatingExpenses)) AS totalOperatingExpenses,
                SUM(IF(totalRevenue IS NULL, 0, totalRevenue)) AS totalRevenue,
                SUM(IF(ebitda IS NULL, incomeBeforeTax + interestExpense + interestIncome + depreciation, ebitda)) AS ebitda, 
                SUM(IF(freeCashFlow IS NULL, 0, freeCashFlow)) AS freeCashFlow
            FROM 
                (
                SELECT fsq.*, ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY date DESC) AS rn
                FROM financial_statement_quarter fsq
                JOIN companies c on fsq.company_id = c.id
                WHERE fsq.date >= DATE_SUB(CURDATE(), INTERVAL 10 YEAR) and company_id = current_company_id
            ) AS ranked
            WHERE 
                company_id = current_company_id AND rn BETWEEN n AND n + 3
            GROUP BY 
                company_id
            ON DUPLICATE KEY UPDATE
				currency_symbol = VALUES(currency_symbol),
				operatingIncome = VALUES(operatingIncome),
				capitalExpenditures = VALUES(capitalExpenditures),
				depreciation = VALUES(depreciation),
				salePurchaseOfStock = VALUES(salePurchaseOfStock),
				totalCashFromFinancingActivities = VALUES(totalCashFromFinancingActivities),
				totalCashFromOperatingActivities = VALUES(totalCashFromOperatingActivities),
				totalCashflowsFromInvestingActivities = VALUES(totalCashflowsFromInvestingActivities),
				costOfRevenue = VALUES(costOfRevenue),
				ebit = VALUES(ebit),
				grossProfit = VALUES(grossProfit),
				incomeBeforeTax = VALUES(incomeBeforeTax),
				incomeTaxExpense = VALUES(incomeTaxExpense),
				interestExpense = VALUES(interestExpense),
				interestIncome = VALUES(interestIncome),
				netIncome = VALUES(netIncome),
				nonOperatingIncomeNetOther = VALUES(nonOperatingIncomeNetOther),
				totalOperatingExpenses = VALUES(totalOperatingExpenses),
				totalRevenue = VALUES(totalRevenue),
				ebitda = VALUES(ebitda),
				freeCashFlow = VALUES(freeCashFlow);            
            -- Увеличиваем n на 1 для следующей итерации
            SET n = n + 1;
        END WHILE;

    END LOOP;
    -- Закрываем курсор
    CLOSE company_cursor;
    -- Обновление логирования процедуры после завершения
	UPDATE _procedure_calls SET finish = NOW() WHERE name = @nameDaily AND start = @startDaily;
    -- дабавляем значения на конец отчетного периода 
    CALL last_ltm_financial_indicators();
        -- обновляем
   	CALL FillCommonStockSharesOutstanding();
    -- обновляем расчетные показатели
    CALL update_ltm_financial_indicators();
    -- обновляем метрики роста компании
    CALL update_growth_ltm_financial_indicators(); 
END;


-----тесты

call create_ltm_financial_indicators();

TRUNCATE table ltm_financial_indicators; 

SHOW PROCESSLIST;


call update_ltm_financial_indicators();

call last_ltm_financial_indicators();

