/* 
DID Report - Client Level
did, range_start, range_end, client_id, E164_client_product, E164_client_range_size
*/
SET @prev_did := '', @prev_client := 0, @count := 0, @range_start := '';

-- Step 1: Identify sorted DIDs and filter out NULL client_id
CREATE TEMPORARY TABLE IF NOT EXISTS sorted_dids AS (
    SELECT 
        did,
        client_id,
        E164_client_product,
        E164_client_range_size
    FROM 
        channel_did
    WHERE 
        client_id IS NOT NULL  -- Filter out rows with NULL client_id
    ORDER BY 
        client_id, CAST(did AS UNSIGNED)
);

-- Step 2: Create temporary ranges for identifying 100-number ranges
CREATE TEMPORARY TABLE IF NOT EXISTS temp_ranges AS (
    SELECT 
        did,
        client_id,
        E164_client_product,
        E164_client_range_size,
        @count := IF(@prev_client = client_id AND CAST(did AS UNSIGNED) = CAST(@prev_did AS UNSIGNED) + 1, 
                     @count + 1, 
                     1) AS range_count,
        @range_start := IF(@count = 1, did, @range_start) AS range_start,
        @prev_did := did,
        @prev_client := client_id
    FROM sorted_dids
);

-- Step 3: Identify full 100-number ranges
CREATE TEMPORARY TABLE IF NOT EXISTS full_ranges AS (
    SELECT 
        client_id,
        range_start,
        did AS range_end,
        100 AS range_size
    FROM temp_ranges
    WHERE range_count = 100
);

-- Step 4: Update DIDs that are part of 100-number ranges
UPDATE channel_did cd
JOIN full_ranges fr
  ON cd.client_id = fr.client_id
  AND CAST(cd.did AS UNSIGNED) BETWEEN CAST(fr.range_start AS UNSIGNED) AND CAST(fr.range_end AS UNSIGNED)
SET cd.E164_client_product = 4, 
    cd.E164_client_range_size = 100;

-- Step 5: Update DIDs that are not part of 100-number ranges but still have a client_id
UPDATE channel_did cd
LEFT JOIN full_ranges fr
  ON cd.client_id = fr.client_id
  AND CAST(cd.did AS UNSIGNED) BETWEEN CAST(fr.range_start AS UNSIGNED) AND CAST(fr.range_end AS UNSIGNED)
SET cd.E164_client_product = 1, 
    cd.E164_client_range_size = 1
WHERE cd.client_id IS NOT NULL
  AND fr.range_start IS NULL;  -- Ensures only DIDs that are NOT part of 100-number ranges are updated

-- Step 6: Select and display the results, sorted by did
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
    sd.client_id,
    sd.E164_client_product,
    sd.E164_client_range_size
FROM 
    sorted_dids sd
LEFT JOIN full_ranges fr ON sd.client_id = fr.client_id 
    AND CAST(sd.did AS UNSIGNED) BETWEEN CAST(fr.range_start AS UNSIGNED) AND CAST(fr.range_end AS UNSIGNED)
WHERE 
    fr.range_start IS NULL OR sd.did = fr.range_start
ORDER BY 
    CAST(sd.did AS UNSIGNED);  -- Sort the result by DID in ascending order

-- Step 7: Cleanup temporary tables
DROP TEMPORARY TABLE IF EXISTS sorted_dids;
DROP TEMPORARY TABLE IF EXISTS temp_ranges;
DROP TEMPORARY TABLE IF EXISTS full_ranges;