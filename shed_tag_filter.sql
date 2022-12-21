------------------------------------------------------------------------------------------------------------------------------
-- Author: Paul Carvalho (paul.carvalho@noaa.gov)
-- 
-- Description: Shed tag filter for detections table. 
------------------------------------------------------------------------------------------------------------------------------

-- 1. Find tagcodes with > 20000 detections and > 20 days between first and last detection at a particular general_location
SELECT TagCode, general_location, n = COUNT(*), max = MAX(DateTime_PST), min = MIN(DateTime_PST), days = DATEDIFF(day, MIN(DateTime_PST) , MAX(DateTime_PST)) -- n_detects = number of detections for each tagcode at a general_location; duration = number of days between the first and last detections
INTO #tmp_table1 -- insert into a temporary table for later use
FROM detects_with_locations -- use this View 
GROUP BY TagCode, general_location; -- aggregate by tagcode and general_location

SELECT TagCode, general_location, n, max, min, days
INTO #tmp_table2 -- insert into another temporary table for later use
FROM #tmp_table1
WHERE (n > 20000 AND days > 20); -- only get tagcode + general location when the number of detections is > 20000 and days between first/last detections is 20

    -- should remove #tmp_table1 here

-- 2. Record information for shed tags identified in step 1.
INSERT INTO shed_tags(TagCode, general_location, n, max, min, days)
SELECT TagCode, general_location, n, max, min, days
FROM #tmp_table2;

-- 3. Get all of the detections for shed tags identified in #tmp_table2 and insert into a temporary table
SELECT recv_ID, TagCode, general_location, DateTime_PST
INTO #tmp_table3
FROM detects_with_locations d
WHERE EXISTS(
            SELECT 1
            FROM #tmp_table2
            WHERE #tmp_table2.TagCode = d.TagCode AND
            #tmp_table2.general_location = d.general_location
);

SELECT TOP(1000) recv_ID, TagCode, DateTime_PST FROM #tmp_table3

-- Just save first 1000 for tag code at particular general location

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

CREATE NONCLUSTERED INDEX IX_detects_recvID_dateTime ON detects
(
    [recv_ID]
)
INCLUDE ([DateTime_PST])
GO