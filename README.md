# PySpark Tutorial — Comprehensive Reference Guide

A hands-on PySpark tutorial built on **Databricks**, covering data reading, transformations, aggregations, joins, window functions, UDFs, data writing, and Spark SQL — all demonstrated against real-world datasets.

---

## Project Structure

```
Spark_tutorial/
├── Spark tutorial.ipynb   # Main Databricks notebook (PySpark + Spark SQL)
├── BigMart Sales.csv       # Retail sales dataset (~870 KB, ~8,500+ rows)
├── drivers.json            # JSON dataset (~181 KB) used for JSON reading examples
└── README.md               # This file
```

| File | Format | Size | Purpose |
|---|---|---|---|
| `Spark tutorial.ipynb` | Jupyter / Databricks Notebook | ~71 KB | All PySpark code examples |
| `BigMart Sales.csv` | CSV | ~870 KB | Primary dataset — BigMart retail sales |
| `drivers.json` | JSON | ~181 KB | Secondary dataset — driver records for JSON reading demo |

---

## Datasets

### BigMart Sales (`BigMart Sales.csv`)

A retail sales dataset with the following columns:

| Column | Description | Example Values |
|---|---|---|
| `Item_Identifier` | Unique product ID | FDA15, DRC01, FDN15 |
| `Item_Weight` | Weight of the product | 9.3, 5.92, 17.5 |
| `Item_Fat_Content` | Fat content category | Low Fat, Regular, LF |
| `Item_Visibility` | Product display area percentage | 0.016, 0.019 |
| `Item_Type` | Product category | Dairy, Soft Drinks, Meat, etc. |
| `Item_MRP` | Maximum Retail Price | 249.81, 48.27 |
| `Outlet_Identifier` | Store ID | OUT049, OUT018 |
| `Outlet_Establishment_Year` | Year the store was established | 1999, 2009 |
| `Outlet_Size` | Store size | Small, Medium, High, *null* |
| `Outlet_Location_Type` | Store location tier | Tier 1, Tier 2, Tier 3 |
| `Outlet_Type` | Store type | Supermarket Type1, Grocery Store |
| `Item_Outlet_Sales` | Sales of the product at the outlet | 3735.14, 443.42 |

### Drivers (`drivers.json`)

A multi-line JSON dataset used to demonstrate JSON data ingestion with `multiLine` option.

---

## 🧠 Topics Covered

### 1. Data Reading

#### Reading JSON
```python
df_json = spark.read.format('json') \
    .option('inferSchema', True) \
    .option('header', True) \
    .option('multiLine', True) \
    .load('/path/to/drivers.json')
```
- Uses `inferSchema` for automatic type detection
- `multiLine=True` is required for pretty-printed JSON files

#### Reading CSV
```python
df = spark.read.format('csv') \
    .option('inferSchema', True) \
    .option('header', True) \
    .load('/path/to/BigMart Sales.csv')
```

#### Reading from a Table (Databricks)
```python
df2 = spark.table('workspace.default.big_mart_sales')
```

#### Display Methods
```python
df.show()       # Tabular text output (default 20 rows)
df.display()    # Databricks rich display
display(df)     # Alternative Databricks display
df.count()      # Returns total row count
```

---

### 2. Schema Definition

#### `printSchema()` — Inspect the schema
```python
df.printSchema()
```

#### DDL Schema (SQL-style string)
Define the schema as a SQL-like string and pass it via `.schema()`:
```python
my_ddl_schema = '''
    Item_Identifier STRING,
    Item_Weight STRING,
    Item_Fat_Content STRING,
    Item_Visibility DOUBLE,
    Item_Type STRING,
    Item_MRP DOUBLE,
    Outlet_Identifier STRING,
    Outlet_Establishment_Year INT,
    Outlet_Size STRING,
    Outlet_Location_Type STRING,
    Outlet_Type STRING,
    Item_Outlet_Sales DOUBLE
'''

df = spark.read.format('csv') \
    .schema(my_ddl_schema) \
    .option('header', True) \
    .load('/path/to/data.csv')
```

#### StructType Schema (Programmatic)
Use `StructType` and `StructField` for programmatic schema definition:
```python
from pyspark.sql.types import *

my_struct_schema = StructType([
    StructField('Item_Identifier', StringType(), True),
    StructField('Item_Weight', StringType(), True),
    StructField('Item_Fat_Content', StringType(), True),
    StructField('Item_Visibility', StringType(), True),
    StructField('Item_Type', StringType(), True),
    StructField('Item_MRP', StringType(), True),
    StructField('Outlet_Identifier', StringType(), True),
    StructField('Outlet_Establishment_Year', StringType(), True),
    StructField('Outlet_Size', StringType(), True),
    StructField('Outlet_Location_Type', StringType(), True),
    StructField('Outlet_Type', StringType(), True),
    StructField('Item_Outlet_Sales', StringType(), True)
])

df = spark.read.format('csv') \
    .schema(my_struct_schema) \
    .option('header', True) \
    .load('/path/to/data.csv')
```

> **Key Difference**: DDL schema uses a concise string syntax; StructType gives you full programmatic control (e.g., nullable flags, nested types).

---

### 3. Select

Select specific columns from a DataFrame:
```python
df.select('Item_Identifier', 'Item_Weight', 'Item_Fat_Content').display()
```

---

### 4. Alias

Rename a column in the output using `.alias()`:
```python
df.select(col('Item_Identifier').alias('Item_id')).display()
```

---

### 5. Filter / Where

#### Simple equality filter
```python
df.filter(col('Item_Fat_Content') == 'Regular').display()
```

#### Multiple conditions with `&` (AND)
```python
df.filter(
    (col('Item_Type') == 'Soft Drinks') & (col('Item_Weight') < 10.0)
).display()
```

#### Null check + `isin()`
```python
df.filter(
    (col('Outlet_Size').isNull()) &
    (col('Outlet_Location_Type').isin('Tier 1', 'Tier 2'))
).display()
```

---

### 6. withColumnRenamed

Rename an existing column:
```python
df.withColumnRenamed('Item_Weight', 'Item_wt').display()
```

---

### 7. withColumn

#### Add a new column with a constant value using `lit()`
```python
from pyspark.sql.functions import lit
df = df.withColumn('New_Column', lit(10))
```

#### Create a computed column
```python
df.withColumn('multiply', col('Item_Weight') * col('Item_MRP')).display()
```

#### Modify existing column data with `regexp_replace()`
```python
df.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'), 'Regular', 'reg')) \
  .withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'), 'Low Fat', 'LF')).display()
```

---

### 8. Type Casting

Change a column's data type using `.cast()`:
```python
df = df.withColumn('Item_Weight', col('Item_Weight').cast(StringType()))
df.printSchema()
```

---

### 9. Sort / OrderBy

#### Single column sort
```python
df.sort(col('Item_Weight').desc()).display()
df.sort(col('Item_Visibility').asc()).display()
```

#### Multi-column sort
```python
# Both descending
df.sort(desc('Item_Visibility'), desc('Item_MRP')).display()

# Mixed: descending + ascending
df.sort(desc('Item_Visibility'), asc('Item_MRP')).display()

# Using orderBy (equivalent to sort)
df.orderBy(col('Item_Visibility').desc(), col('Item_MRP').desc()).display()
```

> **Note**: `sort()` and `orderBy()` are functionally identical in PySpark.

---

### 10. Limit

Restrict the number of rows returned:
```python
df.limit(10).display()
```

---

### 11. Drop

#### Drop a single column
```python
df.drop('Item_Visibility').display()
```

#### Drop multiple columns
```python
df.drop('Item_Visibility', 'Item_Type').display()
```

---

### 12. Drop Duplicates

#### Drop all fully-duplicate rows
```python
df.drop_duplicates().display()
```

#### Drop duplicates based on specific column(s)
```python
df.dropDuplicates(subset=['Item_Type']).display()
# Equivalent alternatives:
# df.drop_duplicates(['Item_Type']).display()
# df.dropDuplicates(['Item_Type']).display()
```

#### `distinct()` — shorthand for `drop_duplicates()` on all columns
```python
df.distinct().display()
```

---

### 13. Union and UnionByName

#### Creating test DataFrames
```python
data1 = [('1', 'kad'), ('2', 'sid')]
schema1 = 'id STRING, name STRING'
df1 = spark.createDataFrame(data1, schema1)

data2 = [('3', 'rahul'), ('4', 'jas')]
schema2 = 'id STRING, name STRING'
df2 = spark.createDataFrame(data2, schema2)
```

#### `union()` — merges by column position
```python
df1.union(df2).display()
```
> ⚠️ **Pitfall**: If column orders differ between DataFrames, `union()` will still merge by position, causing data misalignment.

#### `unionByName()` — merges by column name (safer)
```python
df1.unionByName(df2).display()
```
> ✅ **Best Practice**: Use `unionByName()` when column orders may differ.

---

### 14. String Functions

#### `initcap()`, `upper()`, `lower()`
```python
df.select(initcap('Item_Type')).display()      # Title Case
df.select(lower('Item_Type')).display()         # lowercase
df.select(upper('Item_Type').alias('upper_Item_Type')).display()  # UPPERCASE
```

---

### 15. Date Functions

#### `current_date()` — add today's date
```python
df = df.withColumn('curr_date', current_date())
```

#### `date_add()` — add days
```python
df = df.withColumn('week_after', date_add('curr_date', 7))
```

#### `date_sub()` — subtract days
```python
df.withColumn('week_before', date_sub('curr_date', 7)).display()
# Equivalent using date_add with negative value:
df = df.withColumn('week_before', date_add('curr_date', -7))
```

#### `datediff()` — difference between two dates
```python
df = df.withColumn('datediff', datediff('week_after', 'curr_date'))
```

#### `date_format()` — format date as string
```python
df = df.withColumn('week_before', date_format('week_before', 'dd-MM-yyyy'))
```

---

### 16. Handling Nulls

#### Dropping nulls with `dropna()`
```python
df.dropna().display()                          # Drop rows with ANY null
df.dropna('all').display()                     # Drop rows where ALL columns are null
df.dropna('any').display()                     # Drop rows where ANY column is null
df.dropna(subset=['Outlet_Size']).display()    # Drop rows where specific column is null
```

#### Filling nulls with `fillna()`
```python
df.fillna('NotAvailable').display()                               # Fill all string nulls
df.fillna('NotAvailable', subset=['Outlet_Size']).display()       # Fill specific column
df.fillna({'Outlet_Size': 'NotAvailable'}).display()              # Dictionary syntax
```

---

### 17. Split and Indexing

#### `split()` — split a string column into an array
```python
df.withColumn('Outlet_Type', split('Outlet_Type', ' ')).display()
```

#### Index into the resulting array
```python
df.withColumn('Outlet_Type', split('Outlet_Type', ' ')[1]).display()
```

---

### 18. Explode

Convert array elements into separate rows:
```python
df_exp = df.withColumn('Outlet_Type', split('Outlet_Type', ' '))
df_exp.withColumn('Outlet_Type', explode('Outlet_Type')).display()
```

---

### 19. Array Contains

Check if an array column contains a specific value (returns Boolean):
```python
df_exp.withColumn('Type1_flag', array_contains('Outlet_Type', 'Type1')).display()
```

---

### 20. GroupBy and Aggregations

#### Single aggregation
```python
df.groupBy('Item_Type').agg(sum('Item_MRP')).display()
df.groupBy('Item_Type').agg(avg('Item_MRP')).display()
```

#### Multiple group-by columns with alias
```python
df.groupBy('Item_Type', 'Outlet_Size').agg(
    sum('Item_MRP').alias('Total_MRP')
).display()
```

#### Multiple aggregations
```python
df.groupBy('Item_Type', 'Outlet_Size').agg(
    sum('Item_MRP'),
    avg('Item_MRP')
).display()
```

---

### 21. Collect List

Aggregate values into an array per group:
```python
data = [('user1','book1'), ('user1','book2'),
        ('user2','book2'), ('user2','book4'),
        ('user3','book1')]
schema = 'user string, book string'
df_book = spark.createDataFrame(data, schema)

df_book.groupBy('user').agg(collect_list('book')).display()
```

**Output**: Each user gets an array of all their books.

---

### 22. Pivot

Transpose grouped data from rows to columns:
```python
df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()
```

This creates one column per unique `Outlet_Size` value, with the average `Item_MRP` as the cell values.

---

### 23. When / Otherwise (Conditional Logic)

#### Simple condition
```python
df = df.withColumn('veg_flag',
    when(col('Item_Type') == 'Meat', 'Non-Veg').otherwise('Veg')
)
```

#### Multiple chained conditions
```python
df.withColumn('veg_exp_flag',
    when((col('veg_flag') == 'Veg') & (col('Item_MRP') < 100), 'Veg_Inexpensive')
    .when((col('veg_flag') == 'Veg') & (col('Item_MRP') > 100), 'Veg_Expensive')
    .otherwise('Non_Veg')
).display()
```

---

### 24. Joins

#### Test data setup
```python
dataj1 = [('1','gaur','d01'), ('2','kit','d02'), ('3','sam','d03'),
           ('4','tim','d03'), ('5','aman','d05'), ('6','nad','d06')]
schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING'
df1 = spark.createDataFrame(dataj1, schemaj1)

dataj2 = [('d01','HR'), ('d02','Marketing'), ('d03','Accounts'),
           ('d04','IT'), ('d05','Finance')]
schemaj2 = 'dept_id STRING, department STRING'
df2 = spark.createDataFrame(dataj2, schemaj2)
```

#### Inner Join
Returns only matching rows from both DataFrames:
```python
df1.join(df2, df1.dept_id == df2.dept_id, 'inner').display()
```

#### Left Join
Returns all rows from the left DataFrame + matching rows from the right:
```python
df1.join(df2, df1['dept_id'] == df2['dept_id'], 'left').display()
```

#### Right Join
Returns all rows from the right DataFrame + matching rows from the left:
```python
df1.join(df2, df1['dept_id'] == df2['dept_id'], 'right').display()
```

#### Anti Join
Returns rows from the left DataFrame that have **no match** in the right:
```python
df1.join(df2, df1['dept_id'] == df2['dept_id'], 'anti').display()
```

| Join Type | Matched Rows | Unmatched Left | Unmatched Right |
|---|:---:|:---:|:---:|
| **Inner** | ✅ | ❌ | ❌ |
| **Left** | ✅ | ✅ (nulls for right) | ❌ |
| **Right** | ✅ | ❌ | ✅ (nulls for left) |
| **Anti** | ❌ | ✅ (only unmatched) | ❌ |

---

### 25. Window Functions

#### `row_number()` — assign sequential row numbers
```python
from pyspark.sql.window import Window

df.withColumn('rowCol',
    row_number().over(Window.orderBy('Item_Identifier'))
).display()
```

#### `rank()` vs `dense_rank()`
```python
df.withColumn('rank',
    rank().over(Window.orderBy(col('Item_Identifier').desc()))
).withColumn('denseRank',
    dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))
).display()
```

| Function | Behavior with ties |
|---|---|
| `rank()` | Skips numbers after ties (1, 2, 2, **4**) |
| `dense_rank()` | No gaps after ties (1, 2, 2, **3**) |

#### Window with `rowsBetween()` — running sum
```python
df.withColumn('dum',
    sum('Item_MRP').over(
        Window.orderBy('Item_Identifier')
              .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
).display()
```

#### `partitionBy()` — production-level windowing
```python
window_spec = Window.partitionBy('Item_Type').orderBy(col('Item_MRP').desc())
df.withColumn('Rank_Per_Category', row_number().over(window_spec)).display()
```
> Partitions data by category, enabling distributed parallel processing and per-group ranking.

---

### 26. Cumulative Sum

#### Basic cumsum (sums by category, not true cumulative)
```python
df.withColumn('cumsum',
    sum('Item_MRP').over(Window.orderBy('Item_Type'))
).display()
```

#### True cumulative sum using `rowsBetween()`
```python
df.withColumn('cumsum',
    sum('Item_MRP').over(
        Window.orderBy('Item_Type')
              .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
).display()
```

#### Total sum across all rows
```python
df.withColumn('totalsum',
    sum('Item_MRP').over(
        Window.orderBy('Item_Type')
              .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
).display()
```

| Frame | Effect |
|---|---|
| `unboundedPreceding → currentRow` | Running / cumulative sum |
| `unboundedPreceding → unboundedFollowing` | Grand total for every row |

---

### 27. User Defined Functions (UDF)

Create and apply custom Python functions to DataFrame columns:

```python
# 1. Define a Python function
def my_func(x):
    return x * x

# 2. Register it as a UDF
my_udf = udf(my_func)

# 3. Apply the UDF
df.withColumn('mynewcol', my_udf('Item_MRP')).display()
```

> **Note**: UDFs serialize data to Python and back, so they are slower than built-in Spark functions. Use built-in functions when possible.

---

### 28. Data Writing

#### Write to CSV
```python
df.write.format('csv') \
    .save('/path/to/Sales.csv')
```

#### Write Modes

| Mode | Behavior |
|---|---|
| `append` | Adds data to the existing directory |
| `overwrite` | Replaces all existing data |
| `error` / `errorifexists` | Throws an error if the path already exists (default) |
| `ignore` | Silently does nothing if the path already exists |

```python
# Append
df.write.format('csv').mode('append') \
    .save('/path/to/Sales.csv')

# Overwrite
df.write.format('csv').mode('overwrite') \
    .option('path', '/path/to/Sales.csv') \
    .save()

# Error (default)
df.write.format('csv').mode('error') \
    .option('path', '/path/to/Sales.csv') \
    .save()

# Ignore
df.write.format('csv').mode('ignore') \
    .option('path', '/path/to/Sales.csv') \
    .save()
```

#### Write to Parquet (Columnar Format)
```python
# Append
df.write.format('parquet').mode('append') \
    .option('path', '/path/to/output') \
    .save()

# Overwrite
df.write.format('parquet').mode('overwrite') \
    .option('path', '/path/to/output') \
    .save()
```

> **Why Parquet?** Columnar storage with compression, predicate pushdown, and schema evolution — ideal for analytical workloads.

---

### 29. Table — Managed and External Tables

Save a DataFrame as a Delta managed table:
```python
df.write.format('delta') \
    .mode('overwrite') \
    .saveAsTable('my_table')
```

| Table Type | Metadata | Data | Drop behavior |
|---|---|---|---|
| **Managed** | Spark-managed | Spark-managed | Deletes both metadata & data |
| **External** | Spark-managed | User-managed (external path) | Deletes metadata only |

---

### 30. Spark SQL

#### Create a Temp View
```python
df.createTempView('my_view')
```

#### Query using SQL magic (Databricks)
```sql
%sql
SELECT * FROM my_view
```

#### Query with filters in SQL
```sql
%sql
SELECT * FROM my_view
WHERE Item_Fat_Content = 'Low Fat' OR Item_Fat_Content = 'Lf'
```

#### Query via `spark.sql()` in Python
```python
df_sql = spark.sql("SELECT * FROM my_view WHERE Item_Fat_Content = 'Low Fat'")
df_sql.display()
```

---

## 📦 Key Imports Used

```python
from pyspark.sql.types import *          # StructType, StructField, StringType, etc.
from pyspark.sql.functions import *      # col, lit, when, sum, avg, explode, split, etc.
from pyspark.sql.window import Window    # Window specifications for window functions
```

---

## 🛠️ Environment

| Component | Detail |
|---|---|
| **Platform** | Databricks |
| **Language** | Python (PySpark) |
| **Notebook Format** | Jupyter / Databricks `.ipynb` |
| **Spark API** | DataFrame API + Spark SQL |

---

## 📌 Quick Reference Cheat Sheet

| Operation | Method |
|---|---|
| Read CSV | `spark.read.format('csv').option(...).load(path)` |
| Read JSON | `spark.read.format('json').option('multiLine', True).load(path)` |
| Read Table | `spark.table('catalog.schema.table')` |
| Show schema | `df.printSchema()` |
| Select columns | `df.select('col1', 'col2')` |
| Rename column | `df.withColumnRenamed('old', 'new')` |
| Add/modify column | `df.withColumn('name', expression)` |
| Filter rows | `df.filter(condition)` |
| Sort | `df.sort(col('x').desc())` or `df.orderBy(...)` |
| Limit rows | `df.limit(n)` |
| Drop columns | `df.drop('col1', 'col2')` |
| Drop duplicates | `df.dropDuplicates(subset=['col'])` |
| Union by position | `df1.union(df2)` |
| Union by name | `df1.unionByName(df2)` |
| Group + aggregate | `df.groupBy('col').agg(sum('val'))` |
| Pivot | `df.groupBy('a').pivot('b').agg(avg('c'))` |
| Conditional logic | `when(cond, val).otherwise(val)` |
| Join | `df1.join(df2, on, 'inner'/'left'/'right'/'anti')` |
| Window function | `func().over(Window.partitionBy(...).orderBy(...))` |
| UDF | `udf(python_func)` then `df.withColumn('c', my_udf('col'))` |
| Write CSV | `df.write.format('csv').mode('overwrite').save(path)` |
| Write Parquet | `df.write.format('parquet').mode('overwrite').save(path)` |
| Save as Table | `df.write.format('delta').saveAsTable('name')` |
| Temp View | `df.createTempView('view_name')` |
| Spark SQL | `spark.sql("SELECT * FROM view_name")` |
| Drop nulls | `df.dropna()` / `df.dropna(subset=['col'])` |
| Fill nulls | `df.fillna(value)` / `df.fillna({'col': value})` |
| Split string | `split('col', ' ')` |
| Explode array | `explode('col')` |
| Array contains | `array_contains('col', 'value')` |
| Collect list | `collect_list('col')` |
| Date add/sub | `date_add('col', n)` / `date_sub('col', n)` |
| Date diff | `datediff('col1', 'col2')` |
| Date format | `date_format('col', 'dd-MM-yyyy')` |
| Cast type | `col('x').cast(IntegerType())` |
| Regex replace | `regexp_replace(col('x'), 'pattern', 'replacement')` |

---

## 📝 License

This project is for educational / personal learning purposes.
