## bqe - bq Command-line tool extender


### Why ?

Try to provide full SQL expierence with invoking bq Command-line tool.

Let say you have a sql file `example.sql` like:

```SQL
CREATE TABLE [foo1.bar1]
USING bqe
OPTIONS ( udf_resource "gs://my-gs/udf/myfun1.js" )
AS 
SELECT * from [a1.b1] where a = "AA" and b = 'BB';


CREATE TABLE [foo2.bar2]
USING bqe
OPTIONS ( udf_resource "gs://my-gs/udf/myfun2.js" )
AS 
SELECT a, b, c 
from [a1.b1];
```

With `bqe`, simply run

```
bqe -f example.sql --dry
```

The SQL file will translate it into:


```
bq query --udf_resource 'gs://my-gs/udf/myfun1.js' --destination_table '[foo1.bar1]' u'SELECT * from [a1.b1] where a = "AA" and b = \'BB\''
bq query --udf_resource 'gs://my-gs/udf/myfun2.js' --destination_table '[foo2.bar2]' u'SELECT a, b, c \nfrom [a1.b1]'
```

### Warning 

Still at alpha stage, use at your own risk. And you will need other packages to run it probably.

Those packages are: sqlparse and shortuuid