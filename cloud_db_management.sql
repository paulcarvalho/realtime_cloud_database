------------------------------------------------------------------------------------------------------------------------------
-- Author: Paul Carvalho (paul.carvalho@noaa.gov)
-- 
-- Description: Shed tag filter for detections table. 
------------------------------------------------------------------------------------------------------------------------------

-- Find tagcodes with > 20000 detections and > 20 days between first and last detection at a particular general_location
SELECT TagCode, general_location, n_detects = COUNT(*), duration = DATEDIFF(day, MIN(DateTime_PST) , MAX(DateTime_PST)) -- n_detects = number of detections for each tagcode at a general_location; duration = number of days between the first and last detections
INTO #tmp_table1 -- insert into a temporary table for later use
FROM detects_with_locations -- use this View 
GROUP BY TagCode, general_location; -- aggregate by tagcode and general_location

SELECT TagCode, general_location, n_detects, duration
INTO #tmp_table2 -- insert into another temporary table for later use
FROM #tmp_table1
WHERE (n_detects > 20000 AND duration > 20); -- only get tagcode + general location when the number of detections is > 20000 and days between first/last detections is 20

-- Get all of the detections for shed tags identified in #tmp_table2
SELECT recv_ID, TagCode, DateTime_PST
INTO #tmp_table3
FROM detects
WHERE EXISTS(
            SELECT 1
            FROM #tmp_table2
            WHERE #tmp_table2.TagCode = detects.TagCode
);

-- Get the first and last detections
SELECT recv_ID, TagCode, DateTime_PST, 
    ROW_NUMBER() OVER (PARTITION BY TagCode ORDER BY DateTime_PST DESC) as rn1,
    ROW_NUMBER() OVER (PARTITION BY TagCode ORDER BY DateTime_PST ASC) as rn2
INTO #tmp_table4
FROM #tmp_table3

SELECT recv_ID, TagCode, DateTime_PST, rn1, rn2
INTO #tmp_table5
FROM #tmp_table4
WHERE (rn1 > 100 AND rn2 > 100)

-- Drop temporary tables
DROP TABLE #tmp_table1;
DROP TABLE #tmp_table2;
DROP TABLE #tmp_table3;
DROP TABLE #tmp_table4;
DROP TABLE #tmp_table5;

-- TEMPORARY CODE TO BE DELETED
SELECT * FROM #tmp_table4;
SELECT recv_ID, TagCode, DateTime_PST FROM #tmp_table3 ORDER BY TagCode, DateTime_PST;
SELECT COUNT(DISTINCT TagCode) FROM #tmp_table2; --42

SELECT TagCode, general_location, DateTime_PST
FROM detects_with_locations
WHERE TagCode = 'A555'
ORDER BY DateTime_PST;




-------------------------------------------------------------------------------------
-- create a column that lags the general location by one row
        cte_detects2 (recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm, location_lag)
        AS
        (
            SELECT recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm,
                LAG(general_location, 1) OVER (ORDER BY TagCode, DateTime_PST) AS location_lag
            FROM cte_detects1
        ),

        -- create a column that indicates movement between locations, but allows detections between certain locations without considering as movement
        cte_detects3 (recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm, location_lag, move_count) 
        AS
        (
            SELECT recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm, location_lag,
                CASE WHEN (general_location = location_lag) OR
                        (general_location = 'Sac_BlwGeorgiana2' AND location_lag = 'Sac_BlwGeorgiana') OR
                        (general_location = 'Sac_BlwGeorgiana' AND location_lag = 'Sac_BlwGeorgiana2') OR
                        (general_location = 'Georgiana_Slough1' AND location_lag = 'Georgiana_Slough2') OR
                        (general_location = 'Georgiana_Slough2' AND location_lag = 'Georgiana_Slough1') OR
                        (general_location = 'Benicia_east' AND location_lag = 'Benicia_west') OR
                        (general_location = 'Benicia_west' AND location_lag = 'Benicia_east')
                THEN 0
                ELSE 1
                END AS move_count
            FROM cte_detects2
        )

        -- create a movement ID number for unique movements
        SELECT recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm, location_lag, move_count,
            SUM(move_count) OVER (ORDER BY TagCode, DateTime_PST) AS move
        INTO #tmp_table2
        FROM cte_detects3;

        SELECT * FROM #tmp_table2;
        -- SELECT COUNT(TagCode) FROM cte_detects3;





-- get the min and max datetime and the number of rows for each movement ID
WITH cte_detects5 (move, min, max, n) 
AS(
    SELECT move,
        MIN(DateTime_PST) AS min,
        MAX(DateTime_PST) AS max,
        COUNT(DateTime_PST) AS n
    FROM stage_shed_tags1
    GROUP BY move
),

-- calculate difference between min and max in days and merge with full table
cte_detects6 (recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm, location_lag, move_count, move, min, max, n, days) 
AS
(
    SELECT recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm, location_lag, move_count, stage_shed_tags1.move, min, max, n,
        DATEDIFF(day, min, max) AS days
    FROM stage_shed_tags1
    JOIN cte_detects5
    ON stage_shed_tags1.[move] = cte_detects5.[move]
)

-- create a column that incrementally counts rows for each movement ID
--cte_detects7 (recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm, location_lag, move_count, move, min, max, n, days, n_count1, n_count2) 
--AS
--(
INSERT INTO stage_shed_tags2 (recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm, location_lag, move_count, move, min, max, n, days, n_count1, n_count2)
SELECT recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm, location_lag, move_count, move, min, max, n, days,
    ROW_NUMBER() OVER (PARTITION BY move
                       ORDER BY TagCode, DateTime_PST DESC) AS n_count1,
    ROW_NUMBER() OVER (PARTITION BY move
                       ORDER BY TagCode, DateTime_PST ASC) AS n_count2 
FROM cte_detects6;
--)

-- Create a temporary table for cte_detects7 so we don't have to rerun the CTEs above
--SELECT recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm, location_lag, move_count, move, min, max, n, days, n_count1, n_count2
--INTO #cte_detects7_tmp
--FROM cte_detects7;

 -- all shed tag detects
--WITH cte_detects8 (recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm, location_lag, move_count, move, min, max, n, days, n_count1, n_count2)
--AS
--(
    SELECT recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm, location_lag, move_count, move, min, max, n, days, n_count1, n_count2
    INTO #cte_detects8_tmp
    FROM stage_shed_tags2
    WHERE (days > 20 AND n > 20000) 
    OR (days > 20 AND n > 1000 AND TagCode IN (SELECT TagCode FROM shed_tags));
--)

-- Create a temporary table for cte_detects8 so we don't have to rerun the CTEs above
--SELECT recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm, location_lag, move_count, move, min, max, n, days, n_count1, n_count2
--INTO #cte_detects8_tmp
--FROM cte_detects8;

-- Delete shed tag detections that exist in detects table
DELETE FROM detects
WHERE EXISTS(
    SELECT 1
    FROM #cte_detects8_tmp cte8
    WHERE cte8.recv_ID = detects.recv_ID
    AND cte8.TagCode = detects.TagCode
    AND cte8.DateTime_Orig = detects.DateTime_Orig
    AND cte8.DateTime_PST = detects.DateTime_PST
    AND cte8.Temp = detects.Temp
    AND cte8.filename = detects.filename
);

-- get the first and last 100 detections for shed tags
--WITH cte_detects9 (recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm, location_lag, move_count, move, min, max, n, days, n_count1, n_count2) 
--AS
--(
SELECT recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm, location_lag, move_count, move, min, max, n, days, n_count1, n_count2
INTO #cte_detects9_tmp
FROM #cte_detects8_tmp
WHERE n_count1 < 101 OR n_count2 < 101;
--)

-- Create a temporary table for cte_detects9 so we don't have to rerun the CTEs above
--SELECT recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm, location_lag, move_count, move, min, max, n, days, n_count1, n_count2
--INTO #cte_detects9_tmp
--FROM cte_detects9;

-- Update shed_tag max if exists already
UPDATE shed_tags
SET shed_tags.max = cte9.max
FROM shed_tags st
INNER JOIN #cte_detects9_tmp cte9 
ON st.TagCode = cte9.TagCode
AND st.general_location = cte9.general_location
AND st.n = cte9.n
AND st.min = cte9.min
AND st.days = cte9.days;

-- Insert into shed_tags if it doesn't exist
INSERT INTO shed_tags (TagCode, general_location, n, max, min, days)
SELECT DISTINCT TagCode, general_location, n, max, min, days
FROM #cte_detects9_tmp cte9
WHERE NOT EXISTS(
    SELECT 1
    FROM shed_tags
    WHERE shed_tags.TagCode = cte9.TagCode
    AND shed_tags.general_location = cte9.general_location
    AND shed_tags.n = cte9.n
    AND shed_tags.max = cte9.max
    AND shed_tags.min = cte9.min
    AND shed_tags.days = cte9.days
);

-- all NON-shed tags detects
WITH cte_detects10 (recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm, location_lag, move_count, move, min, max, n, days, n_count1, n_count2) 
AS
(
    SELECT recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm, location_lag, move_count, move, min, max, n, days, n_count1, n_count2
    FROM stage_shed_tags2 cte7
    WHERE NOT EXISTS(
        SELECT 1
        FROM #cte_detects8_tmp cte8
        WHERE cte8.recv_ID = cte7.recv_ID
        AND cte8.location = cte7.location
        AND cte8.recv = cte7.recv
        AND cte8.DateTime_Orig = cte7.DateTime_Orig
        AND cte8.Temp = cte7.Temp
        AND cte8.filename = cte7.filename
        AND cte8.general_location = cte7.general_location
        AND cte8.latitude = cte7.latitude
        AND cte8.longitude = cte7.longitude
        AND cte8.rkm = cte7.rkm
    )
)

--cte_detects11 (recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm, location_lag, move_count, move, min, max, n, days, n_count1, n_count2)
--AS
--(
    SELECT recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm, location_lag, move_count, move, min, max, n, days, n_count1, n_count2
    INTO #cte_detects11_tmp
    FROM cte_detects10
    UNION
    SELECT recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm, location_lag, move_count, move, min, max, n, days, n_count1, n_count2
    FROM #cte_detects9_tmp
--)

-- Insert new detections and new shed tag code detections to the detects table
INSERT INTO detects (recv_ID, TagCode, DateTime_Orig, DateTime_PST, Temp, filename)
SELECT recv_ID, TagCode, DateTime_Orig, DateTime_PST, Temp, filename
FROM #cte_detects11_tmp cte11
WHERE NOT EXISTS(
    SELECT 1
    FROM detects
    WHERE detects.recv_ID = cte11.recv_ID
    AND detects.TagCode = cte11.TagCode
    AND detects.DateTime_Orig = cte11.DateTime_Orig
    AND detects.DateTime_PST = cte11.DateTime_PST
    AND detects.Temp = cte11.Temp
    AND detects.filename = cte11.filename
);

-- Remove temporary tables
DROP TABLE #cte_detects8_tmp;
DROP TABLE #cte_detects9_tmp;
DROP TABLE #cte_detects11_tmp;

-- Remove data from stage_detects
TRUNCATE TABLE stage_shed_tags1;
TRUNCATE TABLE stage_shed_tags2;
TRUNCATE TABLE stage_detects;


-- Filters might need to be applied retroactively becasuse data for certain tags are received late.
-- Create a view updates with detects with just the filtered stuff

SELECT COUNT(TagCode) FROM detects


------------------------------------------------------------------------------------------------------------------------------
-- SHED TAGCODE FILTER
--CREATE NONCLUSTERED INDEX [IX_detects_recvID] ON [dbo].[detects] (recv_ID);
--CREATE NONCLUSTERED INDEX [IX_detects_TagCode] ON [dbo].[detects] (TagCode);

-- -- get rows in detects where tagcode is in stage_detects table, and merge with receivers table
-- WITH cte_detects (recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm)
-- AS
-- (
--     SELECT detects.recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm
--     FROM detects
--     JOIN receivers
--     ON receivers.recv_ID = detects.recv_ID
--     WHERE EXISTS(
--         SELECT 1
--         FROM stage_detects
--         WHERE stage_detects.TagCode = detects.TagCode
--     )
-- ),

-- -- append stage detects to the old detections, union does not duplicate rows
-- cte_detects1 (recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm) 
-- AS
-- (
--     SELECT DISTINCT recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm
--     FROM cte_detects
--     UNION 
--     SELECT recv_ID, s.location, s.recv, s.DateTime_Orig, s.TagCode, s.DateTime_PST, s.Temp, s.filename, s.general_location, s.latitude, s.longitude, s.rkm
--     FROM stage_detects s
--     JOIN receivers
--     ON s.location = receivers.location
--     AND s.recv = receivers.recv
--     AND s.general_location = receivers.general_location
--     AND s.latitude = receivers.latitude
--     AND s.longitude = receivers.longitude
--     AND s.rkm = receivers.rkm
-- ),

-- -- create column where the general_location is lagged by 1 row, and order by TagCode and DateTime_PST
-- cte_detects2 (recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm, location_lag) 
-- AS
-- (
--     SELECT recv_ID, location, recv, DateTime_Orig, TagCode, DateTime_PST, Temp, filename, general_location, latitude, longitude, rkm,
--         LAG(general_location, 1) OVER (PARTITION BY TagCode ORDER BY DateTime_PST ASC) as location_lag
--     FROM cte_detects1
-- ),