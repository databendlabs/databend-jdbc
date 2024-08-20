## (Experimental) Databend Trino Connector User Guide

### Deploy Trino

Firstly, deploy Trino according to this
doc: [Deploying Trino](https://trino.io/docs/current/installation/deployment.html)

#### Install Experimental Databend Trino Connector

Currently, the Databend Trino connector is awaiting Trino community review: https://github.com/trinodb/trino/pull/23024

To try it out, you could download the target [zip](https://repo.databend.com/trino/trino-databend-454.zip) and unzip it to the `databend` directory in
`/data/trino/plugin`:

```bash
cp -r trino-databend-454/* /data/trino/plugin/databend
```

#### Build Databend Connector from scratch

To build the Databend connector from scratch, clone the Databend repository and build the connector:

```bash
 ./mvnw clean install -DskipTests
```

### Configuration

To configure the Databend connector, create a catalog properties file in `/data/trino/etc/catalog` named, for example,
`databend.properties`, to mount the Databend connector as the databend catalog. Create the file with the following
contents, replacing the connection properties as appropriate for your setup:

```properties
connector.name=databend
connection-url=jdbc:databend://host.default.databend.com:443?ssl=true
connection-user=trino_user
connection-password=password
```

### Type Mapping

#### Databend to Trino Type Mapping

| Databend          | Trino          | Notes |
|-------------------|----------------|-------|
| TINYINT UNSIGNED  | SMALLINT       |       |
| SMALLINT UNSIGNED | INTEGER        |       |
| INTEGER UNSIGNED  | BIGINT         |       |
| BIGINT UNSIGNED   | DECIMAL(20, 0) |       |
| BOOLEAN           | TINYINT        |       |
| STRING            | VARCHAR(n)     |       |
| VARCHAR(n)        | VARCHAR(n)     |       |
| JSON(VARIANT)     | JSON           |       |
| TINYINT           | TINYINT        |       |
| SMALLINT          | SMALLINT       |       |
| INTEGER           | INTEGER        |       |
| BIGINT            | BIGINT         |       |
| FLOAT/REAL        | REAL           |       |
| DOUBLE            | DOUBLE         |       |
| DECIMAL(S,P)      | DECIMAL(S,P)   |       |
| DATE              | DATE           |       |
| TIMESTAMP         | TIMESTAMP(n)   |       |

#### Trino to Databend Type Mapping

| Trino Type                  | Databend Type | Notes |
|-----------------------------|---------------|-------|
| BOOLEAN                     | TINYINT       |       |
| TINYINT                     | TINYINT       |       |
| SMALLINT                    | SMALLINT      |       |
| INTEGER                     | INTEGER       |       |
| BIGINT                      | BIGINT        |       |
| REAL                        | FLOAT32       |       |
| DOUBLE                      | DOUBLE        |       |
| VARCHAR(n)                  | VARCHAR(n)    |       |
| DATE                        | DATE          |       |
| TIMESTAMP(n)                | TIMESTAMP     |       |
| TIMESTAMP(n) WITH TIME ZONE | TIMESTAMP     |       |

### Table Functions

The connector provides specific table functions to access Databend.

```sql
query
(varchar) -> table
```

For example, query the databend catalog and group and concatenate all employee IDs by manager ID:

```sql
SELECT *
FROM TABLE(
        databend.system.query(
                query = > 'SELECT
      manager_id, GROUP_CONCAT(employee_id)
    FROM
      company.employees
    GROUP BY
      manager_id'
        )
     );
```

### SQL Support

The connector provides read access and write access to data and metadata in the Databend database.
In addition to the [globally available](https://trino.io/docs/current/language/sql-support.html#sql-globally-available)
and
[read operation](https://trino.io/docs/current/language/sql-support.html#sql-read-operations) statements, the connector
supports the following statements:

- [INSERT](https://trino.io/docs/current/sql/insert.html)
- [UPDATE](https://trino.io/docs/current/sql/update.html)
- [DELETE](https://trino.io/docs/current/sql/delete.html)
- [TRUNCATE](https://trino.io/docs/current/sql/truncate.html)
- [CREATE TABLE](https://trino.io/docs/current/sql/create-table.html)
- [CREATE TABLE AS](https://trino.io/docs/current/sql/create-table-as.html)
- [DROP TABLE](https://trino.io/docs/current/sql/drop-table.html)
- [CREATE SCHEMA](https://trino.io/docs/current/sql/create-schema.html)
- [DROP SCHEMA](https://trino.io/docs/current/sql/drop-schema.html)
- [SHOW SCHEMAS](https://trino.io/docs/current/sql/show-schemas.html)
- [SHOW CATALOGS](https://trino.io/docs/current/sql/show-catalogs.html)
- [Schema and table management](https://trino.io/docs/current/language/sql-support.html#sql-schema-table-management)

### Pushdown

The connector supports pushdown for a number of operations:

- [Join pushdown](https://trino.io/docs/current/optimizer/pushdown.html#join-pushdown)
- [Limit pushdown](https://trino.io/docs/current/optimizer/pushdown.html#limit-pushdown)
- [Top-N pushdown](https://trino.io/docs/current/optimizer/pushdown.html#topn-pushdown)

Aggregate pushdown for the following functions:

- [avg()](https://trino.io/docs/current/functions/aggregate.html#avg)
- [count()](https://trino.io/docs/current/functions/aggregate.html#count)
- [max()](https://trino.io/docs/current/functions/aggregate.html#max)
- [min()](https://trino.io/docs/current/functions/aggregate.html#min)
- [sum()](https://trino.io/docs/current/functions/aggregate.html#sum)
- [stddev()](https://trino.io/docs/current/functions/aggregate.html#stddev)
- [stddev_pop()](https://trino.io/docs/current/functions/aggregate.html#stddev_pop)
- [stddev_samp()](https://trino.io/docs/current/functions/aggregate.html#stddev_samp)
- [variance()](https://trino.io/docs/current/functions/aggregate.html#variance)
- [var_pop()](https://trino.io/docs/current/functions/aggregate.html#var_pop)
- [var_samp()](https://trino.io/docs/current/functions/aggregate.html#var_samp)

### Querying Databend

The Databend connector provides a schema for every Databend database. You can see the available Databend databases by
running SHOW SCHEMAS:

```sql
SHOW
SCHEMAS FROM databend;
```

If you have a Databend database named web, you can view the tables in this database by running SHOW TABLES:

```sql
SHOW
TABLES FROM databend.web;
```

You can see a list of the columns in the clicks table in the web database using either of the following:

```sql
DESCRIBE databend.web.clicks;
SHOW
COLUMNS FROM databend.web.clicks;
```

Finally, you can access the clicks table in the web database:

```sql
SELECT *
FROM databend.web.clicks;
```

If you used a different name for your catalog properties file, use that catalog name instead of databend in the above
examples.


### Quick Start Databend Catelog
Download [trino-cli-454-executable.jar](https://repo1.maven.org/maven2/io/trino/trino-cli/454/trino-cli-454-executable.jar), rename it to trino, make it executable with chmod +x, and run it to show the version of the CLI:
```bash
./trino --version
```

Running Cli

```bash
./trino http://trino.example.com:8080
```


#### Use Databend Catelog
`show catalogs` can show all the catalog already register into trino:

```sql
trino> show catalogs;
Catalog
databend
jmx
memory
system
tpcds
tpch
(6 rows)

Query 20240819_164319_00002_gr6r2, FINISHED, 1 node
Splits: 7 total, 7 done (100.00%)
4.89 [0 rows, OB] [0 rows/s, OB/s]
```

Now we can use the `databend` catalog to query SQL:

trino> use databend.default;
USE
trino:default> show tables;
Table
access_logs
agg
airbyte_integration_dnvcdtfusz
airbyte_integration_okxvhbcoes
base
bend_products
bigcorps
...

```sql
trino:default> create table trino_1 (id int, bb boolean, vc varchar(100), db double);
Query 20240819_164816_00007_gr6r2 failed: line 1:1: Table 'databend.default.trino_1' already exists
create table trino_1 (id int, bb boolean, vc varchar(100), db double);

trino:default> drop table trino_1;
DROP TABLE
trino:default> create table trino_1 (id int, bb boolean, vc varchar(100), db double);
CREATE TABLE
trino:default> insert into trino_1 values(2,1,'string1',1.11);
INSERT: 1 row
Query 20240819_164946_00010_gr6r2, FINISHED, 1 node
Splits: 11 total, 11 done (100.00%)
17.57 [0 rows, OB] [0 rows/s, OB/s]
trino:default> select * from trino_1;
 id | bb |    vc     |  db
----+----+------------+------
  2 |  1 | string1 | 1.11
(1 row)

Query 20240819_165109_00012_gr6r2, FINISHED, 1 node
Splits: 1 total, 1 done (100.00%)
10.27 [1 rows, OB] [0 rows/s, OB/s]
trino:default> delete from trino_1 where id=1;
DELETE: 1 row
Query 20240819_165206_00013_gr6r2, FINISHED, 1 node
Splits: 1 total, 1 done (100.00%)
7.87 [0 rows, OB] [0 rows/s, OB/s]
trino:default>
```