测试的时候发现：

Q1:
mongodb的版本不能低于 3.2 否则报错。
这里是用到了这个 DefaultMongoPartitioner
Exception in thread "main" java.lang.UnsupportedOperationException: The DefaultMongoPartitioner requires MongoDB >= 3.2

Q2:
读取数据的时候，需要指定schema否则的话报错：
Caused by: java.lang.NoSuchMethodError: org.apache.spark.sql.catalyst.analysis.TypeCoercion$.findTightestCommonType()Lscala/Function2;