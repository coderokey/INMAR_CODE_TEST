ANS:
WITH Daterange AS (
  SELECT CAST('2023-10-30' AS DATE) AS redemptionDate
  UNION ALL
  SELECT CAST('2023-10-31' AS DATE)
  UNION ALL
  SELECT CAST('2023-11-01' AS DATE)
  UNION ALL
  SELECT CAST('2023-11-02' AS DATE)
  UNION ALL
  SELECT CAST('2023-11-03' AS DATE)
  UNION ALL
  SELECT CAST('2023-11-04' AS DATE)
  UNION ALL
  SELECT CAST('2023-11-05' AS DATE)
),
CTE AS (
  SELECT
    rd.redemptionDate,
    rd.redemptionCount,
    rd.createDateTime,
    ROW_NUMBER() OVER (PARTITION BY rd.redemptionDate ORDER BY rd.createDateTime DESC) AS rn
  FROM
    `[Kesava].[INMAR].tblRedemptions-ByDay` rd
  JOIN 
    `[Kesava].[INMAR].tblRetailers` r
  ON 
    rd.retailerId = r.id
  WHERE
    r.retailerName = 'ABC Store'
    AND rd.redemptionDate BETWEEN '2023-10-30' AND '2023-11-05'
)
SELECT
  dr.redemptionDate,
  COALESCE(ct.redemptionCount, 0) AS redemptionCount
FROM 
  Daterange dr
LEFT JOIN 
  CTE ct
ON 
  dr.redemptionDate = ct.redemptionDate
  AND ct.rn = 1
ORDER BY 
  dr.redemptionDate;

2023-10-30	4274
2023-10-31	5003
2023-11-01	3930
2023-11-02	0
2023-11-03	3810
2023-11-04	5224
2023-11-05	3702

1.Which date had the least number of redemptions and what was the redemption count?
Date: 2023-11-02
Redemption Count: 0

2.Which date had the most number of redemptions and what was the redemption count?
Date: 2023-10-31
Redemption Count: 5003

3.What was the createDateTime for each redemptionCount in questions 1 and 2?
For 2023-11-02 (0 redemptions): No record exists.
For 2023-10-31 (5003 redemptions): 2023-11-06 11:00:00 UTC

4.Is there another method you can use to pull back the most recent redemption count, by redemption date, for the date range 2023-10-30 to 2023-11-05, for retailer "ABC Store"?
Yes We can use corelated subquery to get max(createDateTime) 
