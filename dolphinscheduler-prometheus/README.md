# prometheus 监控接入

## 工具
- prometheus
- grafana
- 配置关键指标告警

## 接入内容
- jvm 和 actuator默认基本
- 应用健康状态（master\worker\api\alert)
- 设计自定义监控内容

## 入口
- worker: http://host:1235/dolphinscheduler/actuator/prometheus
- master: http://host:5679/dolphinscheduler/actuator/prometheus
- alert: http://host:50053/dolphinscheduler/actuator/prometheus
- api: http://host:12345/dolphinscheduler/actuator/prometheus