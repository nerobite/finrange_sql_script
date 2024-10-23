CREATE OR REPLACE PROCEDURE `calcYieldsForFutureDividends`()
BEGIN
    set @start := NOW();
    set @name := 'calcYieldsForFutureDividends';
    insert into _procedure_calls (name, start, finish) value (@name, @start, null);

    SET sql_mode = '';
    update
        dividend_sources ds
            join (  select company_id, `close` as price 
					from fs_prices fp
					where fp.period = 'current') as fp on ds.company_id = fp.company_id
    SET ds.price = fp.price, ds.yield = ROUND(dividend / fp.price * 100, 2)
    where ds.recordDate >= CURDATE();

    update _procedure_calls SET finish=NOW() where name=@name and start=@start;
END