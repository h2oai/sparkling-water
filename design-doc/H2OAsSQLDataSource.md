# Using H2O Frame as Spark SQL data source

## Reading from H2O Frame
There are several alternatives how data frame can be created from H2O Frame. Let's suppose we have H2O Frame `frame`

The most straightforward way how to create dataframe from H2O Frame with default settings

```
val df = sqlContext.read.h2o(frame.key)
```

The general way how to load dataframe from H2OFrame is

```
val df = sqlContext.read.from("h2o").option("key",frame.key.toString).load()
```

or

```
val df = sqlContext.read.from("h2o").load(frame.key.toString)
```

## Saving to H2O Frame
Let's suppose we have DataFrame `df`

The most straightforward way how to save dataframe as H2O Frame with default settings

```
df.write.h2o("new_key")
```

The general way how to load dataframe from H2OFrame is

```
df.write.format("h2o").option("key","new_key").load()
```

or

```
df.write.format("h2o").load("new_key")
```

All three variants save dataframe as H2O Frame with key "new_key". They won't succeed if the H2O Frame with the same key already exists


### Specifying saving mode
There are four save modes - see http://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes

If "append" mode is used, the existing H2O Frame is deleted and new with the same key is created. The new H2OFrame contains union of
all rows from original H2O Frame and appended Data Frame.

If "overwrite" mode is used, the existing H2O Frame is deleted and new one is created with the same key.

If "error" mode is used, the exception is thrown if the H2O Frame with specified key already exists.

if "ignore" mode is used, nothing is changed if the H2OFrame with the specified key already exists.
## Options for writing as H2OFrame and reading from H2O Frame

If the key is specified as 'key' option and also in the load method, the option 'key' is preferred

```
val df = sqlContext.read.from("h2o").option("key","key_one").load("key_two")
```
Here "key_one" is used
