CREATE OR REPLACE PROCEDURE FillCommonStockSharesOutstanding()
BEGIN
    DECLARE done INT DEFAULT 0;
    DECLARE curr_company_id INT;
    DECLARE curr_date DATE;
    DECLARE curr_cshu DOUBLE;
    DECLARE prev_cshu DOUBLE;
    
    DECLARE cur CURSOR FOR 
        SELECT company_id, end_date, commonStockSharesOutstanding 
        FROM ltm_financial_indicators
        ORDER BY company_id, end_date;
        
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
	
   	-- выполняем логирование процедуры
    SET @startDaily := NOW();
    SET @nameDaily := 'FillCommonStockSharesOutstanding';
    INSERT INTO _procedure_calls (name, start, finish) VALUES (@nameDaily, @startDaily, NULL);
   	-- меняем значения null в столбце commonStockSharesOutstanding на предыдущее не пустое значение
    OPEN cur;

    -- Инициализация цикла по записям
    read_loop: LOOP
        FETCH cur INTO curr_company_id, curr_date, curr_cshu;
        IF done THEN
            LEAVE read_loop;
        END IF;

        -- Если текущий commonStockSharesOutstanding не NULL, обновляем prev_cshu
        IF curr_cshu IS NOT NULL THEN
            SET prev_cshu = curr_cshu;
        ELSE
            -- Обновляем NULL значение на последнее ненулевое
            UPDATE ltm_financial_indicators
            SET commonStockSharesOutstanding = prev_cshu
            WHERE company_id = curr_company_id
              AND end_date = curr_date;
        END IF;

    END LOOP;

    CLOSE cur;
       	-- Обновление логирования процедуры после завершения
	UPDATE _procedure_calls SET finish = NOW() WHERE name = @nameDaily AND start = @startDaily;
END

$$
DELIMITER ;


CALL FillCommonStockSharesOutstanding();