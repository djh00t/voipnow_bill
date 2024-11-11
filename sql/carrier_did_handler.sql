/* 
DID Report - Reseller Level with Grouped 100 Number Ranges
did, range_start, range_end, reseller_id, E164_reseller_product, E164_reseller_range_size
*/
SET @prev_did := '', @prev_reseller := 0, @count := 0, @range_start := '', @range_end := '';

CREATE TEMPORARY TABLE IF NOT EXISTS sorted_dids AS (
    SELECT 
        did,
        reseller_id,
        E164_reseller_product,
        E164_reseller_range_size
    FROM 
        channel_did
    WHERE 
        reseller_id IS NOT NULL
    ORDER BY 
        reseller_id, CAST(did AS UNSIGNED)
);

CREATE TEMPORARY TABLE IF NOT EXISTS temp_ranges AS (
    SELECT 
        did,
        reseller_id,
        E164_reseller_product,
        E164_reseller_range_size,
        @count := IF(@prev_reseller = reseller_id AND CAST(did AS UNSIGNED) = CAST(@prev_did AS UNSIGNED) + 1, 
                     @count + 1, 
                     1) AS range_count,
        @range_start := IF(@count = 1, did, @range_start) AS range_start,
        @prev_did := did,
        @prev_reseller := reseller_id
    FROM sorted_dids
);

CREATE TEMPORARY TABLE IF NOT EXISTS full_ranges AS (
    SELECT 
        reseller_id,
        range_start,
        did AS range_end,
        E164_reseller_product,
        E164_reseller_range_size,
        100 AS range_size
    FROM temp_ranges
    WHERE range_count = 100
);

SELECT 
    sd.did,
    CASE 
        WHEN fr.range_start IS NOT NULL THEN fr.range_start
        ELSE NULL
    END AS range_start,
    CASE 
        WHEN fr.range_end IS NOT NULL THEN fr.range_end
        ELSE NULL
    END AS range_end,
    sd.reseller_id,
    sd.E164_reseller_product,
    sd.E164_reseller_range_size
FROM 
    sorted_dids sd
LEFT JOIN full_ranges fr ON sd.reseller_id = fr.reseller_id 
    AND CAST(sd.did AS UNSIGNED) BETWEEN CAST(fr.range_start AS UNSIGNED) AND CAST(fr.range_end AS UNSIGNED)
WHERE 
    fr.range_start IS NULL OR sd.did = fr.range_start
ORDER BY 
    sd.reseller_id, CAST(sd.did AS UNSIGNED);

DROP TEMPORARY TABLE IF EXISTS sorted_dids;
DROP TEMPORARY TABLE IF EXISTS temp_ranges;
DROP TEMPORARY TABLE IF EXISTS full_ranges;