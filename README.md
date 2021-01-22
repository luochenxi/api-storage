# Storage 
- [Action](https://docs.github.com/cn/actions/reference/events-that-trigger-workflows)
- [宽度workflow](/.github/workflows/main.yml)
- [经济数据workflow](/.github/workflows/economic.yml)

# dynamodb
它和以前理解`NoSql`有点不一样，`dynamodb` 可以理解为是通过`hash_key`和`sort_key`来定位数据，这两列联合唯一。
如果不做其它的操作（索引），查询和过滤数据比较难以理解。
- [深入探讨 Amazon DynamoDB 的设计模式、流复制和全局表](https://www.bilibili.com/video/av96377981/)
- [Query and GetItem on a DynamoDB Table | Step by Step Tutorial](https://www.youtube.com/watch?v=Nk_vjiv6_bE)

## dynamodb宽度数据结构


| 字段      | 类型    |  描述 |
| ------   |-----   | ----- |
| hash_key | hash_key    |  e.g. `BREADTH_US_COM` \[数据类型]\_\[所属国家]_\[行业类型] |
| date     | sort_key   |  date时间类型，排序键|
| o        | str     | 开盘|
| c        | str  | 收盘 |
| n        | str  | 名称 |
| symbol        | str  | 代码 |
| i18n        | str  | 国际化ID |
| data_type        | str  |  e.g. `BREADTH_US` 美股宽度 \[数据类型]_\[所属股市] |
| updatedAt        | timestamp  | 数据更新时间戳 |

- 通过`hash_key`和`date`来定位一条数据
- 通过`data_type`过滤某一类型的数据 如：`ECONOMIC_US_CPI` `STOCK_US`
- `dynamodb`的`GSI`索引很有必要了解一下

# Run

```shell
python3 breadth.py
python3 economic.py
```

# Features
宽度将来也需要移动平均线
```shell
ma20: Float
ma60: Float
ma120: Float
ema20: Float
ema60: Float
ema120: Float
```

```shell
# 导出数据
aws dynamodb scan --table-name TABLE_NAME > export.json
```

# 数据ID


# 国际化ID
```shell
# 宽度数据
breadth.breadth.TOTAL / COM...

# 股票数据
us.stock.APPL
us.etf.VTI

# 经济数据
economic.chart.US_ECONOMIC_WEI
economic.chart.US_ECONOMIC_FFTRUL
economic.chart.US_ECONOMIC_FEDFUNDS
economic.chart.US_ECONOMIC_INFLATION
economic.chart.ECONOMIC_USTREASURY_REALYIELD
economic.chart.US_SPDR_GOLD
economic.chart.US_SYSTEM_SOMA_HOLDINGS
```