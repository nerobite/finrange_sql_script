call Update_RS_historical_annual_metrics();

CREATE OR REPLACE PROCEDURE Update_RS_historical_annual_metrics()
BEGIN

    -- выполняем логирование процедуры
    SET @startDaily := NOW();
    SET @nameDaily := 'Update_RS_historical_annual_metrics';
    INSERT INTO _procedure_calls (name, start, finish) VALUES (@nameDaily, @startDaily, NULL);
    
    -- Отключаем строгие проверки (при необходимости)
    SET @original_sql_mode := @@sql_mode;
    SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION,ALLOW_INVALID_DATES';

	CREATE TEMPORARY TABLE temp_price_index (
	    company_id INT,
	    year YEAR(4),
	    price_max DOUBLE,
	    price_min DOUBLE,
	    index_max DOUBLE,
	    index_min DOUBLE
	);
	
	INSERT INTO temp_price_index(company_id, year, price_max)
	SELECT p.company_id, 
	       YEAR(p.`date`) AS year, 
	       p.`close` AS price_max
	FROM prices p
	JOIN (
	    SELECT p1.company_id, YEAR(p1.`date`) AS year, MAX(p1.`date`) AS max_date
	    FROM prices p1
	    JOIN companies c ON p1.company_id = c.id
	    GROUP BY p1.company_id, YEAR(p1.`date`)
	) AS max_dates 
	ON p.company_id = max_dates.company_id 
	AND YEAR(p.`date`) = max_dates.year 
	AND p.`date` = max_dates.max_date;
	
	UPDATE temp_price_index AS tpi
	JOIN (
	    SELECT p.company_id, 
	           YEAR(p.`date`) AS year, 
	           p.`close` AS price_min
	    FROM prices p
	    JOIN (
	        SELECT company_id, YEAR(`date`) AS year, MIN(`date`) AS min_date
	        FROM prices p2
	        JOIN companies c ON p2.company_id = c.id
	        GROUP BY company_id, YEAR(`date`)
	    ) AS min_dates 
	    ON p.company_id = min_dates.company_id 
	    AND YEAR(p.`date`) = min_dates.year 
	    AND p.`date` = min_dates.min_date
	) AS min_d 
	ON tpi.company_id = min_d.company_id 
	AND tpi.year = min_d.year
	SET tpi.price_min = min_d.price_min;
	
	UPDATE temp_price_index AS tpi
	JOIN companies as c on c.id = tpi.company_id
	JOIN (
			SELECT ip.index_id, YEAR(ip.`date`) as year,
	                       si.country as country,
	                       ip.`close`  as index_max
	        FROM indexes_prices ip 
	        JOIN stock_indexes si ON ip.index_id = si.id
	        JOIN (
	            SELECT ip2.index_id, YEAR(ip2.`date`) as year, MAX(ip2.`date`) as max_date
	            FROM indexes_prices ip2 
	            GROUP BY ip2.index_id, YEAR(ip2.`date`)
	        	) as max_index ON ip.index_id = max_index.index_id 
			        AND YEAR(ip.`date`) = max_index.year 
			        AND ip.`date` = max_index.max_date
	        WHERE si.ticker NOT IN ('DJA', 'IXIC')) as mxi ON c.country = mxi.country AND tpi.year = mxi.year
	SET tpi.index_max = mxi.index_max;
	
	UPDATE temp_price_index AS tpi
	JOIN companies as c on c.id = tpi.company_id
	JOIN (
		SELECT ip.index_id, YEAR(ip.`date`) as year,
	           si.country as country,
	           ip.`close`  as index_min
	    FROM indexes_prices ip 
	    JOIN stock_indexes si ON ip.index_id = si.id
	    JOIN (
	        SELECT ip2.index_id, YEAR(ip2.`date`) as year, MIN(ip2.`date`) as min_date
	        FROM indexes_prices ip2 
	        GROUP BY ip2.index_id, YEAR(ip2.`date`)
	    	) as min_index ON ip.index_id = min_index.index_id 
			    AND YEAR(ip.`date`) = min_index.year 
			    AND ip.`date` = min_index.min_date
	    WHERE si.ticker NOT IN ('DJA', 'IXIC')) mni ON c.country = mni.country AND tpi.year = mni.year
	SET tpi.index_min = mni.index_min;
	
	UPDATE historical_annual_metrics ham
	JOIN (
	    SELECT company_id, year, 
	           ROUND(((index_max - index_min) / NULLIF(index_min, 0)) * 100, 2) AS index_dynamics
	    FROM temp_price_index
	) AS tpi 
	ON tpi.company_id = ham.company_id 
	AND ham.period_end_date = CONCAT_WS('-', tpi.year, '12', '31')
	SET ham.index_dynamics = tpi.index_dynamics;
	
	UPDATE historical_annual_metrics ham
	JOIN (
	    SELECT company_id, year, 
	           ROUND(((price_max - price_min) / NULLIF(price_min, 0)) * 100, 2) AS price_dynamics
	    FROM temp_price_index
	) AS tpi 
	ON tpi.company_id = ham.company_id 
	AND ham.period_end_date = CONCAT_WS('-', tpi.year, '12', '31')
	SET ham.price_dynamics = tpi.price_dynamics;
	
	UPDATE historical_annual_metrics ham
	JOIN (
	SELECT company_id, year, 
	           ROUND(((price_max - lag(price_max, 3) over(partition by company_id order by year))/ 
	           		NULLIF(lag(price_max, 3) over(partition by company_id order by year), 0)) * 100, 2) AS price_dynamics_3y
	    FROM temp_price_index
	) AS tpi 
	ON tpi.company_id = ham.company_id 
	AND ham.period_end_date = CONCAT_WS('-', tpi.year, '12', '31')
	SET ham.price_dynamics_3y = tpi.price_dynamics_3y;
	    
	UPDATE historical_annual_metrics ham
	JOIN (
	    SELECT company_id, year, 
	           ROUND((price_max / NULLIF(price_min, 0)) / (index_max / NULLIF(index_min, 0)), 4) AS RS
	    FROM temp_price_index
	) AS tpi 
	ON tpi.company_id = ham.company_id 
	AND ham.period_end_date = CONCAT_WS('-', tpi.year, '12', '31')
	SET ham.RS = tpi.RS;

	DROP TEMPORARY TABLE temp_price_index;
    -- Восстановление первоначального sql_mode
    SET SESSION sql_mode = @original_sql_mode;
    
    -- Обновление логирования процедуры после завершения
    UPDATE _procedure_calls SET finish = NOW() WHERE name = @nameDaily AND start = @startDaily;
END;