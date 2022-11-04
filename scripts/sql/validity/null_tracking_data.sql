-- Returns rows which have null quantitative data for pitch tracking.

SELECT * FROM dbo.tracking
WHERE NOT ( 
	relspeed IS NULL OR 
	spinrate IS NULL OR
	relheight IS NULL OR
	relside IS NULL OR
	extension IS NULL OR
	vertbreak IS NULL OR
	inducedvertbreak IS NULL OR
	horzbreak IS NULL
    IS NOT NULL)
