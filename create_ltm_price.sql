CREATE OR REPLACE PROCEDURE create_ltm_price()
BEGIN
    -- выполняем логирование процедуры
    SET @startDaily := NOW();
    SET @nameDaily := 'create_ltm_price';
    INSERT INTO _procedure_calls (name, start, finish) VALUES (@nameDaily, @startDaily, NULL);
    
    -- вносим данные о ценах в таблицу price_temp_templ на момент выхода отчета, кроме последнего, который выдается на последнюю известную цену
    INSERT IGNORE INTO price_temp_table (company_id, date, close)
    SELECT t.company_id, t.end_date, t.close
    FROM (
        SELECT 
            fsa.company_id, 
            fsa.end_date,
            CASE 
                WHEN fsa.rn = 1 THEN sl.close
                ELSE sh.close
            END AS close,
            ROW_NUMBER() OVER (PARTITION BY fsa.company_id, fsa.end_date ORDER BY fsa.end_date) AS rnn
        FROM (
            SELECT lfi.*, 
                   ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY end_date DESC, start_date DESC) AS rn
            FROM ltm_financial_indicators lfi
        ) AS fsa
        LEFT JOIN (
            -- Последняя известная цена (на текущий момент времени)
            SELECT p1.company_id, p1.date AS date, p1.close
            FROM prices p1
            WHERE p1.date = (
                SELECT MAX(p2.date)
                FROM prices p2
                WHERE p2.company_id = p1.company_id
            )
        ) AS sl ON sl.company_id = fsa.company_id
        LEFT JOIN (
            -- Цена на дату отчета до или на ближайшие даты после него
            SELECT p1.company_id, p1.date AS date, p1.close
            FROM prices p1
        ) AS sh ON sh.company_id = fsa.company_id 
                 AND (sh.date = fsa.end_date 
                      OR sh.date = fsa.end_date - INTERVAL 1 DAY 
                      OR sh.date = fsa.end_date + INTERVAL 1 DAY
                      OR sh.date = fsa.end_date + INTERVAL 2 DAY 
                      OR sh.date = fsa.end_date + INTERVAL 3 DAY)
        ORDER BY fsa.company_id, fsa.end_date DESC
    ) t
    WHERE rnn = 1;
    -- завершаем логирование
    UPDATE _procedure_calls SET finish = NOW() WHERE name = @nameDaily AND start = @startDaily;
END;

