SHOW CREATE PROCEDURE dividendsUpdateGraphFromSource; 

-- старая процедура
CREATE OR REPLACE PROCEDURE `dividendsUpdateGraphFromSource`()
BEGIN
    INSERT INTO dividend_graphs (company_id, year, payout, payoutRatio, dividend, yield, sourceData)
    SELECT company_id, year, 0, 0, dividend, yield, sourceData
    FROM (
             SELECT company_id,
                    year,
                    ROUND(SUM(dividend), 4) AS dividend,
                    ROUND(SUM(yield), 4) AS yield,
                    sourceData
             FROM dividend_sources
             WHERE t2Date IS NOT NULL
             GROUP BY company_id, year, sourceData
         ) AS t
    ON DUPLICATE KEY UPDATE
                         dividend = VALUES(dividend),
                         yield = VALUES(yield),
                         sourceData = VALUES(sourceData);
END

-- старая таблица
CREATE TABLE `dividend_graphs` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `company_id` bigint(20) unsigned NOT NULL,
  `year` year(4) NOT NULL,
  `payout` double DEFAULT NULL,
  `payoutRatio` double DEFAULT NULL,
  `dividend` double DEFAULT NULL,
  `yield` double DEFAULT NULL,
  `sourceData` varchar(56) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `updated_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_company_year_source` (`company_id`,`year`,`sourceData`),
  KEY `dividend_graphs_company_id_index` (`company_id`),
  KEY `dividend_graphs_year_index` (`year`)
) ENGINE=InnoDB AUTO_INCREMENT=137781 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


#########################################################################################################################################
-- новая процедура
CREATE OR REPLACE DEFINER = `developer`@`%` PROCEDURE `dividendsUpdateGraphFromSource`()
BEGIN
    -- выполняем логирование процедуры
    SET @startDaily := NOW();
    SET @nameDaily := 'dividendsUpdateGraphFromSource';
    INSERT INTO _procedure_calls (name, start, finish) VALUES (@nameDaily, @startDaily, NULL);
	
    -- Отключаем строгие проверки (при необходимости)
    SET @original_sql_mode := @@sql_mode;
    SET SESSION sql_mode = 'NO_ENGINE_SUBSTITUTION,ALLOW_INVALID_DATES';
   
   	START TRANSACTION;
   
    -- Вставка данных с обновлением существующих записей
    INSERT INTO dividend_graphs (company_id, year, Divi_year, dividend, yield, PDR, Div_yield_year, DFCFR, DPR, DGR, DGR_3Y, DGR_5Y, DGR_10Y, Div_CAGR_3Y, Div_CAGR_5Y, Div_CAGR_10Y)
	SELECT  
	    t.company_id,
	    t.`year`,
	    t.Divi_year,
	    t.dividend,
	    t.yield,
	    t.PDR,
	    t.Div_yield_year,
	    t.DFCFR,
	    t.DPR,
	    ROUND((dividend / lag(dividend) OVER (PARTITION BY t.company_id ORDER BY t.`year`) - 1) * 100, 2) AS DGR,
	    ROUND((dividend / lag(dividend, 3) OVER (PARTITION BY t.company_id ORDER BY t.`year`) - 1) * 100, 2) AS DGR_3Y,
	    ROUND((dividend / lag(dividend, 5) OVER (PARTITION BY t.company_id ORDER BY t.`year`) - 1) * 100, 2) AS DGR_5Y,
	    ROUND((dividend / lag(dividend, 10) OVER (PARTITION BY t.company_id ORDER BY t.`year`) - 1) * 100, 2) AS DGR_10Y,
	    ROUND((POWER(dividend / lag(dividend, 3) OVER (PARTITION BY t.company_id ORDER BY t.`year`), 1/3) - 1) * 100, 2) AS Div_CAGR_3Y,
	    ROUND((POWER(dividend / lag(dividend, 5) OVER (PARTITION BY t.company_id ORDER BY t.`year`), 1/5) - 1) * 100, 2) AS Div_CAGR_5Y,
	    ROUND((POWER(dividend / lag(dividend, 10) OVER (PARTITION BY t.company_id ORDER BY t.`year`), 1/10) - 1) * 100, 2) AS Div_CAGR_10Y
	FROM (
	    SELECT 
	        ds.company_id,
	        ds.`year`,
	        ROUND(SUM(ds.dividend) * fsa.commonStockSharesOutstanding, 2) AS Divi_year,
	        SUM(ds.dividend) AS dividend,
	        SUM(ds.yield) AS yield,
	        ROUND(AVG(p.adj_close) / SUM(ds.dividend), 2) AS PDR,
	        ROUND(SUM(ds.dividend) / AVG(p.adj_close) * 100, 2) AS Div_yield_year,
	        ROUND(SUM(ds.dividend) * fsa.commonStockSharesOutstanding / fsa.freeCashFlow * 100, 2) AS DFCFR,
	        ROUND(SUM(ds.dividend) * fsa.commonStockSharesOutstanding / fsa.netIncome * 100, 2) AS DPR
	    FROM dividend_sources ds
	    LEFT JOIN prices p ON ds.company_id = p.company_id AND ds.t2Date = p.`date` 
	    LEFT JOIN financial_statement_annual fsa ON ds.company_id = fsa.company_id AND ds.`year` = fsa.`year`
	    GROUP BY ds.company_id, ds.`year`
	) t
	ON DUPLICATE KEY UPDATE
		company_id = company_id,
		`year` = `year`,
	    Divi_year = VALUES(Divi_year),
	    dividend = VALUES(dividend),
	    yield = VALUES(yield),
	    PDR = VALUES(PDR),
	    Div_yield_year = VALUES(Div_yield_year),
	    DFCFR = VALUES(DFCFR),
	    DPR = VALUES(DPR),
	    DGR = VALUES(DGR),
	    DGR_3Y = VALUES(DGR_3Y),
	    DGR_5Y = VALUES(DGR_5Y),
	    DGR_10Y = VALUES(DGR_10Y),
	    Div_CAGR_3Y = VALUES(Div_CAGR_3Y),
	    Div_CAGR_5Y = VALUES(Div_CAGR_5Y),
	    Div_CAGR_10Y = VALUES(Div_CAGR_10Y);
	
	COMMIT;

	START TRANSACTION;
    -- Обновление данных по средней доходности
    UPDATE dividend_graphs AS dt
    JOIN (
        SELECT
            company_id,
            `year`,
            ROUND(AVG(Div_yield_year) OVER (PARTITION BY company_id ORDER BY `year` ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS `3y_Avg_Div_yield`,
            ROUND(AVG(Div_yield_year) OVER (PARTITION BY company_id ORDER BY `year` ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 2) AS `5y_Avg_Div_yield`,
            ROUND(AVG(Div_yield_year) OVER (PARTITION BY company_id ORDER BY `year` ROWS BETWEEN 9 PRECEDING AND CURRENT ROW), 2) AS `10y_Avg_Div_yield`
        FROM dividend_graphs
    ) AS sub ON dt.company_id = sub.company_id AND dt.`year` = sub.`year`
    SET dt.`3y_Avg_Div_yield` = sub.`3y_Avg_Div_yield`,
        dt.`5y_Avg_Div_yield` = sub.`5y_Avg_Div_yield`,
        dt.`10y_Avg_Div_yield` = sub.`10y_Avg_Div_yield`;
    
    -- Обновление данных по среднему росту дивидендов
    UPDATE dividend_graphs AS dg
	JOIN ( 
		SELECT 
			t.company_id,
			t.year,
			t.avg_yield_industry,
			t.avg_3y_yield_industry,
			t.avg_5y_yield_industry,
			t.avg_10y_yield_industry,
			t.avg_yield_sector,
			t.avg_3y_yield_sector,
			t.avg_5y_yield_sector,
			t.avg_10y_yield_sector,
			t.avg_yield_industry - lag(t.avg_yield_industry) OVER (PARTITION BY t.company_id ORDER BY t.year) AS avg_growth_div_yer_industry,
			t.avg_yield_industry - lag(t.avg_yield_industry, 3) OVER (PARTITION BY t.company_id ORDER BY t.year) AS avg_growth_div_3yer_industry,
			t.avg_yield_industry - lag(t.avg_yield_industry, 5) OVER (PARTITION BY t.company_id ORDER BY t.year) AS avg_growth_div_5yer_industry,
			t.avg_yield_industry - lag(t.avg_yield_industry, 10) OVER (PARTITION BY t.company_id ORDER BY t.year) AS avg_growth_div_10yer_industry,
			t.avg_yield_sector - lag(t.avg_yield_sector) OVER (PARTITION BY t.company_id ORDER BY t.year) AS avg_growth_div_yer_sector,
			t.avg_yield_sector - lag(t.avg_yield_sector, 3) OVER (PARTITION BY t.company_id ORDER BY t.year) AS avg_growth_div_3yer_sector,
			t.avg_yield_sector - lag(t.avg_yield_sector, 5) OVER (PARTITION BY t.company_id ORDER BY t.year) AS avg_growth_div_5yer_sector,
			t.avg_yield_sector - lag(t.avg_yield_sector, 10) OVER (PARTITION BY t.company_id ORDER BY t.year) AS avg_growth_div_10yer_sector
		FROM (
			SELECT dg.company_id,
				dg.year,
				ROUND(AVG(dg.Div_yield_year) OVER (PARTITION BY c.industry_id, c.country_id, dg.year), 2) AS avg_yield_industry,
				ROUND(AVG(dg.`3y_Avg_Div_yield`) OVER (PARTITION BY c.industry_id, c.country_id, dg.year), 2) AS avg_3y_yield_industry,
				ROUND(AVG(dg.`5y_Avg_Div_yield`) OVER (PARTITION BY c.industry_id, c.country_id, dg.year), 2) AS avg_5y_yield_industry,
				ROUND(AVG(dg.`10y_Avg_Div_yield`) OVER (PARTITION BY c.industry_id, c.country_id, dg.year), 2) AS avg_10y_yield_industry,
				ROUND(AVG(dg.Div_yield_year) OVER (PARTITION BY c.sector_id, c.country_id, dg.year), 2) AS avg_yield_sector,
				ROUND(AVG(dg.`3y_Avg_Div_yield`) OVER (PARTITION BY c.sector_id, c.country_id, dg.year), 2) AS avg_3y_yield_sector,
				ROUND(AVG(dg.`5y_Avg_Div_yield`) OVER (PARTITION BY c.sector_id, c.country_id, dg.year), 2) AS avg_5y_yield_sector,
				ROUND(AVG(dg.`10y_Avg_Div_yield`) OVER (PARTITION BY c.sector_id, c.country_id, dg.year), 2) AS avg_10y_yield_sector
			FROM dividend_graphs dg
			JOIN companies c ON dg.company_id = c.id
		) t
	) sub ON dg.company_id = sub.company_id AND dg.`year` = sub.`year`
	SET dg.avg_yield_industry = sub.avg_yield_industry,
		dg.avg_3y_yield_industry = sub.avg_3y_yield_industry,
		dg.avg_5y_yield_industry = sub.avg_5y_yield_industry,
		dg.avg_10y_yield_industry = sub.avg_10y_yield_industry,
		dg.avg_yield_sector = sub.avg_yield_sector,
		dg.avg_3y_yield_sector = sub.avg_3y_yield_sector,
		dg.avg_5y_yield_sector = sub.avg_5y_yield_sector,
		dg.avg_10y_yield_sector = sub.avg_10y_yield_sector,
		dg.avg_growth_div_yer_industry = sub.avg_growth_div_yer_industry,
		dg.avg_growth_div_3yer_industry = sub.avg_growth_div_3yer_industry,
		dg.avg_growth_div_5yer_industry = sub.avg_growth_div_5yer_industry,
		dg.avg_growth_div_10yer_industry = sub.avg_growth_div_10yer_industry,
		dg.avg_growth_div_yer_sector = sub.avg_growth_div_yer_sector,
		dg.avg_growth_div_3yer_sector = sub.avg_growth_div_3yer_sector,
		dg.avg_growth_div_5yer_sector = sub.avg_growth_div_5yer_sector,
		dg.avg_growth_div_10yer_sector = sub.avg_growth_div_10yer_sector;
	
	-- количество непрерывных лет выплат дивидендов и количесте непрерывных лет роста дивов
	UPDATE dividend_graphs  set continuous_div_start_year = 0;
    UPDATE dividend_graphs, (
        with cte as (
			SELECT company_id,
		         year,
		         SUM(year_without_div) OVER (PARTITION BY company_id ORDER BY year desc) year_without_div,
		         SUM(year_without_g_div) OVER (PARTITION BY company_id ORDER BY year desc) as year_without_g_div
			from (
					select company_id,
			                 year,
			                 year - COALESCE(LEAD(year) over (PARTITION BY company_id ORDER BY YEAR desc), 0) != 1 as year_without_div,
			                 dividend - COALESCE(lag(dividend) over(PARTITION by company_id order by year), 0) < 0 as year_without_g_div
					from dividend_graphs
					where year <= (YEAR(NOW()) - 1) and dividend > 0)t)
		select company_id, IF(SUM(!year_without_div)>0, SUM(!year_without_div) + 1, 0) as cdy,
		IF(SUM(!year_without_div)>0 and SUM(!year_without_g_div)>0, IF(SUM(!year_without_div) >= SUM(!year_without_g_div), SUM(!year_without_g_div)+1, SUM(!year_without_div)+1), 0) as cgdy
		from cte
		group by 1) div_year_date
    SET continuous_div_start_year = div_year_date.cdy,
   		continuous_div_grow_year = div_year_date.cgdy
    where dividend_graphs.company_id = div_year_date.company_id;
	
	COMMIT;
	
	START TRANSACTION;
	
	-- рассчет dsi
	UPDATE dividend_graphs, (
	with cte1 as( -- рассчет 𝑌𝑐 
		SELECT dg.company_id, 
			MAX(CASE 
				when ds.num is null and continuous_div_start_year > 0 then IF(continuous_div_start_year > 7, 7, continuous_div_start_year)
				when ds.num > 0  and continuous_div_start_year > 0 then IF(continuous_div_start_year > 7, 8, continuous_div_start_year+1)
				else 0
			END) AS Y_c
		FROM dividend_graphs dg
		left join (
			SELECT ds.company_id,
				ROW_NUMBER() OVER(PARTITION BY ds.company_id) as num
			from dividend_sources ds 
			where YEAR(ds.year) = YEAR(CURDATE())) ds on dg.company_id = ds.company_id
		WHERE dg.year <= YEAR(NOW()) - 1
		group by dg.company_id),
	cte2 as ( -- рассчет G𝑐 
		SELECT t.company_id,
			CASE 
				when t.G_c_2 is not null then t.G_c_2
				else t.G_c_1
			END as G_c
		from (
				SELECT DISTINCT dg.company_id,
						CASE 
							when dg.continuous_div_grow_year > 0 and dg.continuous_div_grow_year <=7 then continuous_div_grow_year
							else 0
						END AS G_c_1,
						big_join.G_c as G_c_2
				from dividend_graphs dg
				left join (
				with cte as (
					SELECT company_id,
				         year,
				         SUM(year_without_div) OVER (PARTITION BY company_id ORDER BY year desc) year_without_div,
				         SUM(year_without_r_div) OVER (PARTITION BY company_id ORDER BY year desc) as year_without_r_div,
				         SUM(year_zero_g_div) OVER (PARTITION BY company_id ORDER BY year desc) as year_zero_g_div
					from (
							select company_id,
				                 year,
				                 year - COALESCE(LEAD(year) over (PARTITION BY company_id ORDER BY YEAR desc), 0) != 1 as year_without_div,
				                 dividend - COALESCE(lag(dividend) over(PARTITION by company_id order by year), 0) >= 0.1 * dividend  as year_without_r_div,
				                 dividend - COALESCE(lag(dividend) over(PARTITION by company_id order by year), 0) = 0 as year_zero_g_div
							from dividend_graphs dg
							where (dg.`year` BETWEEN (YEAR(CURDATE()) - 7) AND YEAR(CURDATE())) and dividend > 0) t),
				cte2 as (select company_id, IF(SUM(!year_without_div)>0, SUM(!year_without_div) + 1, 0) as cdy,
				 			IF(SUM(!year_without_div) >= 7 and SUM(!year_without_r_div) = 1, SUM(!year_without_r_div), 0) as crdy,
				 			IF(SUM(!year_without_div)>=7 and SUM(year_zero_g_div) = 7, 7, 0) as zero_g_d
						from cte
						group by 1)
				select company_id, 
						CASE 
							when cdy >=7 and (crdy = 1 or zero_g_d = 7) then 3.5
						END as G_c
				from cte2
				) as big_join on dg.company_id = big_join.company_id) t)
	select cte1.company_id,
			CASE 
				when cur_div.company_id is null then ROUND(((cte1.Y_c + cte2.G_c) / 14) * 0.7, 2)
				else ROUND((cte1.Y_c + cte2.G_c) / 14, 2)
			END as dsi
	from cte1
	join cte2 on cte1.company_id = cte2.company_id
	left join (SELECT company_id
				FROM dividend_sources ds 
				WHERE recordDate BETWEEN (CURDATE() - interval 1 year) and CURDATE() 
				group by company_id) as cur_div on 	cte1.company_id = cur_div.company_id ) div_dsi
	SET dividend_graphs.dsi = div_dsi.dsi
	where dividend_graphs.company_id = div_dsi.company_id;
		
    COMMIT;
    -- Восстанавливаем начальные настройки режима SQL
    SET SESSION sql_mode = @original_sql_mode;

    -- Завершаем логирование процедуры
    SET @finishDaily := NOW();
    UPDATE _procedure_calls SET finish = @finishDaily WHERE name = @nameDaily AND start = @startDaily;
END;


###########################################################################################################################################

-- тесты

TRUNCATE table dividend_graphs; 

SELECT name, start, finish, TIMESTAMPDIFF(SECOND, start, finish) AS duration_seconds
FROM _procedure_calls
-- where name = 'dividendsUpdateGraphFromSource'
ORDER BY start DESC
-- LIMIT 10;

SELECT *
FROM _procedure_calls

call dividendsUpdateGraphFromSource();

call `formatDividends`();

-- drop table dividend_graphs;

-- новая таблица
CREATE TABLE `dividend_graphs` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `company_id` int(11) DEFAULT NULL,
  `year` year(4) DEFAULT NULL,
  `Divi_year` double DEFAULT NULL,
  `dividend` double DEFAULT NULL,
  `yield` double DEFAULT NULL,
  `PDR` double DEFAULT NULL,
  `Div_yield_year` double DEFAULT NULL,
  `DFCFR` double DEFAULT NULL,
  `DPR` double DEFAULT NULL,
  `3y_Avg_Div_yield` double DEFAULT NULL,
  `5y_Avg_Div_yield` double DEFAULT NULL,
  `10y_Avg_Div_yield` double DEFAULT NULL,
  `avg_growth_div_yer_industry` double DEFAULT NULL,
  `avg_growth_div_3yer_industry` double DEFAULT NULL,
  `avg_growth_div_5yer_industry` double DEFAULT NULL,
  `avg_growth_div_10yer_industry` double DEFAULT NULL,
  `avg_growth_div_yer_sector` double DEFAULT NULL,
  `avg_growth_div_3yer_sector` double DEFAULT NULL,
  `avg_growth_div_5yer_sector` double DEFAULT NULL,
  `avg_growth_div_10yer_sector` double DEFAULT NULL,
  `avg_yield_industry` double DEFAULT NULL,
  `avg_3y_yield_industry` double DEFAULT NULL,
  `avg_5y_yield_industry` double DEFAULT NULL,
  `avg_10y_yield_industry` double DEFAULT NULL,
  `avg_yield_sector` double DEFAULT NULL,
  `avg_3y_yield_sector` double DEFAULT NULL,
  `avg_5y_yield_sector` double DEFAULT NULL,
  `avg_10y_yield_sector` double DEFAULT NULL,
  `DGR` double DEFAULT NULL,
  `DGR_3Y` double DEFAULT NULL,
  `DGR_5Y` double DEFAULT NULL,
  `DGR_10Y` double DEFAULT NULL,
  `Div_CAGR_3Y` double DEFAULT NULL,
  `Div_CAGR_5Y` double DEFAULT NULL,
  `Div_CAGR_10Y` double DEFAULT NULL,
  `continuous_div_start_year` int(11) DEFAULT NULL,
  `continuous_div_grow_year` int(11) DEFAULT NULL,
  `dsi` double DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_company_id` (`company_id`),
  KEY `idx_year` (`year`),
  KEY `idx_company_year` (`company_id`,`year`)
) ENGINE=InnoDB AUTO_INCREMENT=589830 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
   
   
-- Создание индекса для столбца company_id
CREATE INDEX idx_company_id ON dividend_graphs (company_id);

-- Создание индекса для столбца year
CREATE INDEX idx_year ON dividend_graphs (year);

-- Создание составного индекса для столбцов company_id и year
CREATE INDEX idx_company_year ON dividend_graphs (company_id, year);

ALTER TABLE dividend_graphs ADD UNIQUE KEY uniq_company_year (company_id, year);
