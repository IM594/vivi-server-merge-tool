# 代码实现分析

## 文件结构
*   `app.py`: Flask 后端核心逻辑。
*   `templates/index.html`: 前端上传界面。

## 核心函数与逻辑

### 1. `index()` - 主处理流程
*   **文件上传**：
    *   使用 `request.files.getlist('csv_files')` 接收多个 CSV。
    *   遍历并读取每个 CSV（跳过第一行索引 `header=1`），存入列表 `dfs`。
    *   使用 `pd.concat` 合并所有 DataFrame。
*   **预处理**：
    *   强制转换关键列为 numeric。
    *   `df.sort_values(by='前2名战力之和', ascending=False)`。
    *   添加 `真实排名` 列。
*   **输入解析**：
    *   `parse_server_pairs` 解析文本框输入的 ID 对。
*   **主循环 - 分类**：
    *   遍历 `input_pairs`。
    *   使用 `get_server_info` 查找 CSV 数据（`iloc[0]` 取排序后第一条）。
    *   判断三个 Primary Alert 条件。
    *   分流到 `alert_groups` 或 `normal_groups`。

### 2. 警报处理
*   **Primary Alert 输出**：
    *   将触发警报的原始 ID 对详情加入 `final_alert_rows`。
*   **Secondary Alert 检查**：
    *   遍历 `alert_groups`。
    *   `find_partner_in_xlsx`: 扫描 XLSX 行，查找 ID 对应的配对服（P1/P2）。
    *   检查 `[S1, S2, P1, P2]` 中是否有 `DAU <= 5`。
    *   如果触发，将 P1/P2 的详情也追加到 `final_alert_rows`，标记原因 "二次查询DAU<=5"。

### 3. 交换处理
*   **准备**：
    *   加载 XLSX (`load_workbook`)。
    *   构建 `server_row_map` (ServerID -> RowIndex) 以加速查找。
*   **循环交换**：
    *   遍历 `normal_groups`。
    *   查找 `S1`, `S2` 对应的行 `r1_idx`, `r2_idx`。
    *   **校验**：确保两者都存在且不在同一行。
    *   **记录**：写入 `swapped_log_data`。
    *   **执行交换**：
        *   读取 R1 的 Target/Part 值，读取 R2 的 Target/Part 值。
        *   在内存中通过逻辑判断（`if v == s1 then s2`）进行互换。
        *   **排序**：对新生成的配对 `[new_t, new_p]` 进行 `.sort()`，确保小号在前。
        *   回写到单元格 (`c1_t.value`, etc.)。
    *   **标色**：设置整行的 `fill` 为黄色。
    *   **更新索引**：`server_row_map[s1] = r2_idx`, `server_row_map[s2] = r1_idx`。

### 4. 输出生成
*   `alert_df.to_csv`: 生成警报文件。
*   `swapped_df.to_csv`: 生成交换日志。
*   `wb.save`: 保存修改后的 XLSX。
*   `render_template`: 返回页面，传递统计数据 `alert_count` 和 `actual_swapped_count`。

## 关键细节
*   **CSV 合并**：正确处理了多文件。
*   **XLSX 查找**：通过 `iter_rows` 遍历查找，假设了 Target 在前 Part 在后（或通过表头定位）。
*   **容错**：对 `get_server_info` 返回 None 的情况做了日志记录并跳过，防止崩溃。
