SELECT ST_MapAlgebra(
t1.rast, 1,
t1.rast, 2,
'([rast2] + [rast1])',
'32BSI'
) AS rast
FROM berlin.cs_2013_berlin AS t1