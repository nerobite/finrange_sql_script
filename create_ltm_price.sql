CREATE OR REPLACE PROCEDURE create_ltm_price()
BEGIN
    -- выполняем логирование процедуры
    SET @startDaily := NOW();
    SET @nameDaily := 'create_ltm_price';
    INSERT INTO _procedure_calls (name, start, finish) VALUES (@nameDaily, @startDaily, NULL);
    
    -- вносим данные о ценах в таблицу price_temp_templ на момент выхода отчета, кроме последнего, который выдается на последнюю известную цену
    INSERT IGNORE INTO price_temp_table (company_id, date, close)
    SELECT lfi.company_id, lfi.end_date, p.close
	FROM ltm_financial_indicators lfi
	JOIN prices p 
	    ON p.company_id = lfi.company_id 
	    AND p.`date` = (
	        SELECT MIN(p2.`date`)
	        FROM prices p2
	        WHERE p2.company_id = lfi.company_id
	          AND p2.`date` >= lfi.end_date)
    ON DUPLICATE KEY UPDATE close = p.close;
    -- завершаем логирование
    UPDATE _procedure_calls SET finish = NOW() WHERE name = @nameDaily AND start = @startDaily;
END;

truncate table price_temp_table;

call create_ltm_price();