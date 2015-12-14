/*
test abc
*/

CREATE TABLE [foo1.bar1]
USING bqe
OPTIONS ( 
	udf_resource "gs://my-gs/udf/myfun1.js" append_table "true" use_cache "false"
)
AS 
SELECT * from [a1.b1] where a = "AA" and b = 'BB';


CREATE TABLE [foo2.bar2]
USING bqe
OPTIONS ( udf_resource "gs://my-gs/udf/myfun2.js" )
AS 
SELECT a, b, c 
from [a1.b1];
