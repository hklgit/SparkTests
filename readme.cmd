此project用于测试使用spark从hive读取数据然后写入到kafka，
需要的注意的问题就是jar的各种版本问题，spark用的是2.x的，相应的运行环境也要匹配，
如果使用1.6的环境来运行会报错：关于sparksession找不到的错误

本地依赖没有问题的话可以直接跑通。