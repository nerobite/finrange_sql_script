-- процедура для расчета дивидендного гепа
CREATE OR REPLACE DEFINER = `developer`@`%` PROCEDURE CalculateDividendMetrics()
BEGIN
    -- создаем временные переменные
    DECLARE done INT DEFAULT FALSE;
    DECLARE company INT;
    DECLARE two_date DATE; 
    DECLARE close_before DECIMAL(10, 2);
    DECLARE open_after DECIMAL(10, 2);
    DECLARE dividend_gap DECIMAL(10, 2);
    DECLARE recovery_days INT DEFAULT 0;
    DECLARE recovery_date DATE; 
    DECLARE max_drawdown DECIMAL(10, 2) DEFAULT 0;
    DECLARE min_price DECIMAL(10, 2);

    -- Объявляем курсор
    DECLARE cur CURSOR FOR 
        SELECT t.company_id, t.`date`, t.`close`, t.`open`, t.dividend_gap
        FROM (
            WITH cte1 AS (
                SELECT p.company_id, p.`date`, p.`close`
                FROM prices p
                JOIN dividend_sources ds ON p.company_id = ds.company_id AND p.`date` = ds.t2Date 
            ),
            cte2 AS (
                SELECT p.company_id, p.`date`, p.`open`, ds.t2Date
                FROM prices p
                JOIN dividend_sources ds ON p.company_id = ds.company_id AND p.`date` = IF(WEEKDAY(ds.t2Date + INTERVAL 1 DAY) NOT IN (5), ds.t2Date + INTERVAL 1 DAY, ds.t2Date + INTERVAL 3 DAY)
            )
            SELECT cte1.company_id, cte2.t2Date AS date, cte1.`close`, cte2.`open`, ROUND((cte2.`open` - cte1.close) / cte1.close * 100, 2) AS dividend_gap
            FROM cte1
            JOIN cte2 ON cte1.company_id = cte2.company_id AND cte1.`date` = cte2.t2Date
        ) t;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
		-- Проверка на выполнение процедуры только в субботу
    IF WEEKDAY(CURDATE()) = 5 THEN
	    -- выполняем логирование процедуры до открытия курсора
	    SET @startDaily := NOW();
	    SET @nameDaily := 'CalculateDividendMetrics';
	    INSERT INTO _procedure_calls (name, start, finish) VALUES (@nameDaily, @startDaily, NULL);
	
	    OPEN cur;
	
	    read_loop: LOOP
	        FETCH cur INTO company, two_date, close_before, open_after, dividend_gap;
	        IF done THEN
	            LEAVE read_loop;
	        END IF;  
	
	        SET sql_mode = '';
	        IF dividend_gap >= 0 THEN
	            SET recovery_days = 0;
	            SET max_drawdown = 0;
	        ELSE
	            -- Закрытие дивидендного гэпа, дни
	            SELECT MIN(p.date) INTO recovery_date
	            FROM prices p
	            WHERE p.company_id = company AND p.date > two_date AND p.close >= close_before;
	
	            IF recovery_date IS NOT NULL THEN
	                SET recovery_days = WorkdaysBetween(two_date, recovery_date);
	                -- Максимальная просадка после дивидендного гэпа
	                SELECT MIN(p.close) INTO min_price
	                FROM prices p
	                WHERE p.company_id = company AND p.date BETWEEN two_date AND recovery_date;
	
	                IF min_price IS NOT NULL THEN
	                    SET max_drawdown = (min_price - close_before) / close_before * 100;
	                END IF; 
	            ELSE
	                SET recovery_days = NULL; -- если цена не восстановилась
	                -- Максимальная просадка после дивидендного гэпа
	                SELECT MIN(p.close) INTO min_price
	                FROM prices p
	                WHERE p.company_id = company AND p.date BETWEEN two_date AND CURDATE();
	
	                IF min_price IS NOT NULL THEN
	                    SET max_drawdown = (min_price - close_before) / close_before * 100;
	                END IF;
	            END IF;
	        END IF;   
	
	        -- Сохранение результатов с обновлением существующих записей
	        INSERT INTO dividend_gap (company_id, two_date, close_before, open_after, dividend_gap, recovery_days, max_drawdown)
	        VALUES (company, two_date, close_before, open_after, dividend_gap, recovery_days, max_drawdown)
	        ON DUPLICATE KEY UPDATE
	            close_before = VALUES(close_before),
	            open_after = VALUES(open_after),
	            dividend_gap = VALUES(dividend_gap),
	            recovery_days = VALUES(recovery_days),
	            max_drawdown = VALUES(max_drawdown);
	    END LOOP;
	    CLOSE cur;
	
	    -- Обновление логирования процедуры после завершения
	    UPDATE _procedure_calls SET finish = NOW() WHERE name = @nameDaily AND start = @startDaily;
    END IF;
END;



-- функция для расчета количества рабочих дней между датами, исключая выходные 
CREATE OR REPLACE FUNCTION WorkdaysBetween(start_date DATE, end_date DATE)
RETURNS INT
BEGIN
    DECLARE total_days INT;
    DECLARE total_weeks INT;
    DECLARE start_day_of_week INT;
    DECLARE end_day_of_week INT;
    DECLARE working_days INT;

    -- Подсчет всех дней между датами
    SET total_days = DATEDIFF(end_date, start_date);

    -- Количество полных недель между датами
    SET total_weeks = total_days DIV 7;

    -- Определение дня недели для начальной и конечной дат
    SET start_day_of_week = WEEKDAY(start_date);
    SET end_day_of_week = WEEKDAY(end_date);

    -- Вычисление количества рабочих дней
    SET working_days = total_weeks * 5 + 
                       CASE 
                           WHEN start_day_of_week <= end_day_of_week THEN 
                               LEAST(end_day_of_week, 4) - LEAST(start_day_of_week, 4)
                           ELSE 
                               5 - LEAST(start_day_of_week, 4) + LEAST(end_day_of_week, 4)
                       END;

    RETURN working_days;
END;

