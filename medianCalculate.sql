
CREATE OR REPLACE PROCEDURE `medianCalculate`()
BEGIN
    IF WEEKDAY(CURDATE()) = 6 THEN
        CALL median_company_5y_multipliers();
        CALL median_company_10y_multipliers();
        CALL median_industry_multipliers();
        CALL median_sector_multipliers();
    END IF;
END;


call medianCalculate();