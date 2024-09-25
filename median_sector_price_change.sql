CREATE OR REPLACE PROCEDURE `median_sector_price_change`()
BEGIN
	-- выполняем логирование процедуры
	set @startDaily := NOW();
    set @nameDaily := 'median_sector_price_change';
    insert into _procedure_calls (name, start, finish) value (@nameDaily, @startDaily, null);
   
	-- Отключаем строгие проверки (при необходимости)
    SET @original_sql_mode := @@sql_mode;
    SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION,ALLOW_INVALID_DATES';
    
    -- заносим данные в таблицу 
    UPDATE median_sector_metrics AS mim
    JOIN ( SELECT t.*
    	   FROM(
    	   		WITH data AS (
				    SELECT fp.company_id, c.sector_id, c.exchange_id,
				           ((MAX(CASE WHEN period = 'current' THEN `close` END) - 
				             MAX(CASE WHEN period = 'day' THEN `close` END)) /
				             MAX(CASE WHEN period = 'day' THEN `close` END)) * 100 AS percentage_1d_change,
				           ((MAX(CASE WHEN period = 'current' THEN `close` END) - 
				             MAX(CASE WHEN period = 'week' THEN `close` END)) /
				             MAX(CASE WHEN period = 'week' THEN `close` END)) * 100 AS percentage_1w_change,
				           ((MAX(CASE WHEN period = 'current' THEN `close` END) - 
				             MAX(CASE WHEN period = 'month' THEN `close` END)) /
				             MAX(CASE WHEN period = 'month' THEN `close` END)) * 100 AS percentage_1m_change,
				           ((MAX(CASE WHEN period = 'current' THEN `close` END) - 
				             MAX(CASE WHEN period = '3month' THEN `close` END)) /
				             MAX(CASE WHEN period = '3month' THEN `close` END)) * 100 AS percentage_3m_change,
				           ((MAX(CASE WHEN period = 'current' THEN `close` END) - 
				             MAX(CASE WHEN period = '6month' THEN `close` END)) /
				             MAX(CASE WHEN period = '6month' THEN `close` END)) * 100 AS percentage_6m_change,
				           ((MAX(CASE WHEN period = 'current' THEN `close` END) - 
				             MAX(CASE WHEN period = 'year' THEN `close` END)) /
				             MAX(CASE WHEN period = 'year' THEN `close` END)) * 100 AS percentage_year_change,
				           ((MAX(CASE WHEN period = 'current' THEN `close` END) - 
				             MAX(CASE WHEN period = '1year' THEN `close` END)) /
				             MAX(CASE WHEN period = '1year' THEN `close` END)) * 100 AS percentage_1year_change,
				           ((MAX(CASE WHEN period = 'current' THEN `close` END) - 
				             MAX(CASE WHEN period = '3year' THEN `close` END)) /
				             MAX(CASE WHEN period = '3year' THEN `close` END)) * 100 AS percentage_3year_change,
				           STDDEV_POP((MAX(CASE WHEN period = 'current' THEN `close` END) - 
					         MAX(CASE WHEN period = 'week' THEN `close` END)) /
					         MAX(CASE WHEN period = 'week' THEN `close` END)) over(partition by c.sector_id, c.exchange_id) AS stddev_1w_change,
					       STDDEV_POP((MAX(CASE WHEN period = 'current' THEN `close` END) - 
					         MAX(CASE WHEN period = 'month' THEN `close` END)) /
					         MAX(CASE WHEN period = 'month' THEN `close` END)) over(partition by c.sector_id, c.exchange_id) AS stddev_1m_change
				    FROM fs_prices fp
				    JOIN companies c ON fp.company_id = c.id
				    GROUP BY company_id, c.sector_id, c.exchange_id
				),
				ranked_data as(
					SELECT d.*,
					       ROW_NUMBER() OVER (PARTITION BY d.sector_id, d.exchange_id ORDER BY percentage_1d_change) AS rn_percentage_1d_change,
					       ROW_NUMBER() OVER (PARTITION BY d.sector_id, d.exchange_id ORDER BY percentage_1w_change) AS rn_percentage_1w_change,
					       ROW_NUMBER() OVER (PARTITION BY d.sector_id, d.exchange_id ORDER BY percentage_1m_change) AS rn_percentage_1m_change,
					       ROW_NUMBER() OVER (PARTITION BY d.sector_id, d.exchange_id ORDER BY percentage_3m_change) AS rn_percentage_3m_change,
					       ROW_NUMBER() OVER (PARTITION BY d.sector_id, d.exchange_id ORDER BY percentage_6m_change) AS rn_percentage_6m_change,
					       ROW_NUMBER() OVER (PARTITION BY d.sector_id, d.exchange_id ORDER BY percentage_year_change) AS rn_percentage_year_change,
					       ROW_NUMBER() OVER (PARTITION BY d.sector_id, d.exchange_id ORDER BY percentage_1year_change) AS rn_percentage_1year_change,
					       ROW_NUMBER() OVER (PARTITION BY d.sector_id, d.exchange_id ORDER BY percentage_3year_change) AS rn_percentage_3year_change,
					       ROW_NUMBER() OVER (PARTITION BY d.sector_id, d.exchange_id ORDER BY stddev_1w_change) AS rn_stddev_1w_change,
					       ROW_NUMBER() OVER (PARTITION BY d.sector_id, d.exchange_id ORDER BY stddev_1m_change) AS rn_stddev_1m_change,
					       COUNT(*) OVER (PARTITION BY d.sector_id, d.exchange_id) AS total_rows
					FROM data d)
				SELECT rd.sector_id, rd.exchange_id,
								ROUND(AVG(CASE WHEN rn_percentage_1d_change = FLOOR((total_rows + 1) / 2) 
										                OR (total_rows % 2 = 0 AND rn_percentage_1d_change IN (total_rows / 2, total_rows / 2 + 1)) 
										                THEN percentage_1d_change END), 2) AS percentage_1d_change_median_sector,
								ROUND(AVG(CASE WHEN rn_percentage_1w_change = FLOOR((total_rows + 1) / 2) 
										                OR (total_rows % 2 = 0 AND rn_percentage_1w_change IN (total_rows / 2, total_rows / 2 + 1)) 
										                THEN percentage_1w_change END), 2) AS percentage_1w_change_median_sector,
								ROUND(AVG(CASE WHEN rn_percentage_1m_change = FLOOR((total_rows + 1) / 2) 
										                OR (total_rows % 2 = 0 AND rn_percentage_1m_change IN (total_rows / 2, total_rows / 2 + 1)) 
										                THEN percentage_1m_change END), 2) AS percentage_1m_change_median_sector,
								ROUND(AVG(CASE WHEN rn_percentage_3m_change = FLOOR((total_rows + 1) / 2) 
										                OR (total_rows % 2 = 0 AND rn_percentage_3m_change IN (total_rows / 2, total_rows / 2 + 1)) 
										                THEN percentage_3m_change END), 2) AS percentage_3m_change_median_sector,
								ROUND(AVG(CASE WHEN rn_percentage_6m_change = FLOOR((total_rows + 1) / 2) 
										                OR (total_rows % 2 = 0 AND rn_percentage_6m_change IN (total_rows / 2, total_rows / 2 + 1)) 
										                THEN percentage_6m_change END), 2) AS percentage_6m_change_median_sector,
								ROUND(AVG(CASE WHEN rn_percentage_year_change = FLOOR((total_rows + 1) / 2) 
										                OR (total_rows % 2 = 0 AND rn_percentage_year_change IN (total_rows / 2, total_rows / 2 + 1)) 
										                THEN percentage_year_change END), 2) AS percentage_year_change_median_sector,
								ROUND(AVG(CASE WHEN rn_percentage_1year_change = FLOOR((total_rows + 1) / 2) 
										                OR (total_rows % 2 = 0 AND rn_percentage_1year_change IN (total_rows / 2, total_rows / 2 + 1)) 
										                THEN percentage_1year_change END), 2) AS percentage_1year_change_median_sector,
								ROUND(AVG(CASE WHEN rn_percentage_3year_change = FLOOR((total_rows + 1) / 2) 
										                OR (total_rows % 2 = 0 AND rn_percentage_3year_change IN (total_rows / 2, total_rows / 2 + 1)) 
										                THEN percentage_3year_change END), 2) AS percentage_3year_change_median_sector,
								ROUND(AVG(CASE WHEN rn_stddev_1w_change = FLOOR((total_rows + 1) / 2) 
										                OR (total_rows % 2 = 0 AND rn_stddev_1w_change IN (total_rows / 2, total_rows / 2 + 1)) 
										                THEN stddev_1w_change END), 4) * 100 AS stddev_1w_change_median_sector,
								ROUND(AVG(CASE WHEN rn_stddev_1m_change = FLOOR((total_rows + 1) / 2) 
										                OR (total_rows % 2 = 0 AND rn_stddev_1m_change IN (total_rows / 2, total_rows / 2 + 1)) 
										                THEN stddev_1m_change END), 4) * 100 AS stddev_1m_change_median_sector	                
				FROM ranked_data rd
				GROUP BY rd.sector_id, rd.exchange_id
				ORDER BY rd.sector_id, rd.exchange_id )t ) AS sub on mim.sector_id = sub.sector_id AND mim.exchange_id = sub.exchange_id
	SET mim.percentage_1d_change_median_sector = sub.percentage_1d_change_median_sector,
		mim.percentage_1w_change_median_sector = sub.percentage_1w_change_median_sector,
		mim.percentage_1m_change_median_sector = sub.percentage_1m_change_median_sector,
		mim.percentage_3m_change_median_sector = sub.percentage_3m_change_median_sector,
		mim.percentage_6m_change_median_sector = sub.percentage_6m_change_median_sector,
		mim.percentage_year_change_median_sector = sub.percentage_year_change_median_sector,
		mim.percentage_1year_change_median_sector = sub.percentage_1year_change_median_sector,
		mim.percentage_3year_change_median_sector = sub.percentage_3year_change_median_sector,
		mim.stddev_1w_change_median_sector = sub.stddev_1w_change_median_sector,
		mim.stddev_1m_change_median_sector = sub.stddev_1m_change_median_sector;
    	-- Обновление логирования процедуры после завершения
	UPDATE _procedure_calls SET finish = NOW() WHERE name = @nameDaily AND start = @startDaily;
END;

CALL median_sector_price_change();