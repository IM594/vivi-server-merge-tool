# 合服预演实现说明

## 主要文件
- `app.py`：后端处理逻辑
- `templates/index.html`：前端上传页面与结果展示
- `tests/test_merge_logic.py`：核心规则单元测试

## 核心实现

### 1. 输入解析
- `parse_server_pairs`
  - 解析文本框中的区服对。
  - 自动去重，避免重复处理相同组合。

### 2. 服务器数据索引
- `build_server_info_map`
  - 根据 `区服ID` 构建快速索引。
- `get_server_info`
  - 读取指定区服的数值信息。

### 3. 计划表解析
- `parse_server_ids_from_cell`
  - 从 Excel 单元格中提取一个或多个区服 ID。
- `build_plan_rows`
  - 将 Excel 每一行转换为统一结构。
- `build_plan_groups`
  - 将共享成员的多行数据聚合为逻辑组。

### 4. 重组逻辑
- `regroup_for_requested_pair`
  - 根据输入对 `(A, B)` 进行抽组和重组。
  - 生成请求组和剩余组。

### 5. 预警逻辑
- `evaluate_primary_warning`
  - 执行常规预警判定。
- `evaluate_secondary_dau_warning`
  - 执行剩余组的二次 DAU 预警判定。
- `build_alert_row`
  - 构造预警结果行。

### 6. 输出整理
- `build_output_rows_from_groups`
  - 从逻辑组生成最终结果行。
- `merge_output_rows_by_target`
  - 对重复目标服进行合并、去重和拼接。

## 主流程

### 1. 上传文件
- 接收多个 CSV 和一个 XLSX。

### 2. 处理 CSV
- 合并、清洗、排序、生成排名。

### 3. 处理 XLSX
- 识别逻辑组。
- 按输入区服对执行重组。

### 4. 预警检测
- 先判断请求组的常规预警。
- 再判断剩余组的二次 DAU 预警。

### 5. 生成输出文件
- `alert_result.csv`
- `swapped_log.csv`
- `result_plan.xlsx`

### 6. 页面展示
- 返回日志
- 返回统计数字
- 返回下载入口

## 当前输出规则
- 逻辑组按来源锚点行排序。
- 请求组写回最早来源锚点行。
- 剩余组写回后续来源锚点行。
- 相同目标服的参与服会在最终输出时合并为一行。
