# arana

[![LICENSE](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://github.com/arana-db/arana/blob/master/LICENSE)
[![codecov](https://codecov.io/gh/arana-db/arana/branch/master/graph/badge.svg)](https://codecov.io/gh/arana-db/arana)

![](./docs/pics/arana-main.png)

## 简介

Arana 是一个数据库代理。可以通过 sidecar 方式部署。

## 架构

## 功能

| 特性 | 完成情况 |
| -- | -- |
| 单库代理 | √ |
| 读写分离 | √ |
| 分片 | √ |
| 多租户 | √ |
| 分布式主键 | √ |
| 影子表 | √ |
| sql 执行追踪 | × |
| sql 执行指标 | × |
| sql 审计 | × |
| 分库分表 | × |
| sql 限流 | × |

## 启动方法

```
arana start -c ${configFilePath}
```

### 前提条件

+ MySQL server 5.7+

## 设计与实现

## 路线图

## 相关项目

- [tidb](https://github.com/pingcap/tidb) - The sql parser used

## 联系方式

## 贡献

## 开源协议

Arana software is licenced under the Apache License Version 2.0. See
the [LICENSE](https://github.com/arana-db/arana/blob/master/LICENSE) file for details.
