import os
import re
import sys
import pandas as pd
import numpy as np
import datetime
from flask import Flask, render_template, request, send_file, send_from_directory
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
import traceback
from collections import Counter, defaultdict

app = Flask(__name__)

# Determine if running as a script or frozen (PyInstaller)
if getattr(sys, 'frozen', False):
    BASE_DIR = sys._MEIPASS
    # When frozen, templates and static files are in sys._MEIPASS
    app = Flask(__name__, template_folder=os.path.join(BASE_DIR, 'templates'))
else:
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    app = Flask(__name__)

UPLOAD_FOLDER = os.path.join(os.getcwd(), 'uploads') # Use CWD for user-accessible folders
DOWNLOAD_FOLDER = os.path.join(os.getcwd(), 'downloads')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['DOWNLOAD_FOLDER'] = DOWNLOAD_FOLDER

ID_LIKE_COLUMNS = ['区服ID', 'DAU', '跨服ID', 'code', '总注册角色', '峰值在线', '当天付费账号数']

class ExecutionLogger:
    def __init__(self):
        self.logs = []
    
    def user(self, message, level='INFO'):
        self._add_log(level, message, 'user')
        
    def dev(self, message, level='DEBUG'):
        self._add_log(level, message, 'dev')
        
    def _add_log(self, level, message, category):
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        self.logs.append({
            'time': timestamp, 
            'level': level, 
            'msg': message,
            'category': category
        })

def parse_server_pairs(text):
    pairs = []
    seen = set()
    duplicates = []
    
    if not text:
        return pairs, duplicates
        
    lines = text.strip().split('\n')
    for line in lines:
        parts = line.replace('，', ',').split(',')
        if len(parts) >= 2:
            try:
                s1 = int(parts[0].strip())
                s2 = int(parts[1].strip())
                
                # Sort tuple to treat (A, B) same as (B, A)
                pair_key = tuple(sorted((s1, s2)))
                
                if pair_key in seen:
                    duplicates.append(f"{s1} ↔ {s2}")
                else:
                    seen.add(pair_key)
                    pairs.append((s1, s2))
            except ValueError:
                continue
    return pairs, duplicates

def parse_server_ids_from_cell(value):
    if value is None:
        return []
    if isinstance(value, (np.integer, int)):
        return [int(value)]
    if isinstance(value, float):
        if np.isnan(value):
            return []
        return [int(value)]

    text = str(value).strip()
    if not text:
        return []

    return [int(match) for match in re.findall(r'\d+', text)]

def build_server_info_map(df):
    if '区服ID' not in df.columns:
        return {}

    deduped_df = df.drop_duplicates(subset=['区服ID'], keep='first')
    return {int(row['区服ID']): row for _, row in deduped_df.iterrows()}

def get_server_info(server_info_source, server_id):
    if isinstance(server_info_source, dict):
        return server_info_source.get(server_id)

    row = server_info_source[server_info_source['区服ID'] == server_id]
    if row.empty:
        return None
    return row.iloc[0]

def build_plan_rows(ws, target_col_idx, part_col_idx):
    plan_rows = []

    for r_idx, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
        target_ids = parse_server_ids_from_cell(row[target_col_idx])
        participant_ids = parse_server_ids_from_cell(row[part_col_idx])

        if not target_ids and not participant_ids:
            continue

        target = target_ids[0] if target_ids else None
        members = []

        if target is not None:
            members.append(target)

        for server_id in target_ids[1:] + participant_ids:
            if server_id not in members:
                members.append(server_id)

        if not members:
            continue

        if target is None:
            target = members[0]

        plan_rows.append({
            'row_idx': r_idx,
            'target': target,
            'participants': [member for member in members if member != target],
            'members': members
        })

    return plan_rows

def _pick_group_target(component_rows, members):
    ordered_rows = sorted(component_rows, key=lambda item: item['row_idx'])
    ordered_targets = [row['target'] for row in ordered_rows if row['target'] in members and row['target'] is not None]

    if not ordered_targets:
        return min(members)

    counts = Counter(ordered_targets)
    for target in ordered_targets:
        if counts[target] == max(counts.values()):
            return target

    return min(members)

def build_plan_groups(plan_rows):
    if not plan_rows:
        return []

    normalized_rows = []
    for row in plan_rows:
        normalized_row = dict(row)
        if 'members' not in normalized_row:
            target = normalized_row.get('target')
            participants = list(normalized_row.get('participants', []))
            members = []
            if target is not None:
                members.append(target)
            for server_id in participants:
                if server_id not in members:
                    members.append(server_id)
            normalized_row['members'] = members
        normalized_rows.append(normalized_row)

    row_lookup = {index: row for index, row in enumerate(normalized_rows)}
    server_to_row_indexes = defaultdict(set)

    for index, row in row_lookup.items():
        for server_id in row['members']:
            server_to_row_indexes[server_id].add(index)

    groups = []
    visited = set()

    for start_index in row_lookup:
        if start_index in visited:
            continue

        pending = [start_index]
        component_rows = []
        component_members = set()

        while pending:
            current_index = pending.pop()
            if current_index in visited:
                continue

            visited.add(current_index)
            current_row = row_lookup[current_index]
            component_rows.append(current_row)
            component_members.update(current_row['members'])

            for server_id in current_row['members']:
                pending.extend(server_to_row_indexes[server_id] - visited)

        row_indices = sorted(row['row_idx'] for row in component_rows)
        groups.append({
            'target': _pick_group_target(component_rows, component_members),
            'members': sorted(component_members),
            'row_indices': row_indices,
            'anchor_row': row_indices[0]
        })

    groups.sort(key=lambda item: item['anchor_row'])
    return groups

def _clone_group(group):
    return {
        'target': group['target'],
        'members': list(group['members']),
        'row_indices': list(group['row_indices']),
        'anchor_row': group['anchor_row']
    }

def format_group_label(group):
    if not group or not group.get('members'):
        return "空"

    target = group['target']
    participants = [member for member in group['members'] if member != target]

    if participants:
        return f"{target} -> {','.join(str(item) for item in participants)}"
    return str(target)

def regroup_for_requested_pair(groups, s1, s2):
    working_groups = [_clone_group(group) for group in groups]
    group_indexes = [index for index, group in enumerate(working_groups) if s1 in group['members'] or s2 in group['members']]
    group_indexes = sorted(set(group_indexes))

    group_a_index = next((index for index, group in enumerate(working_groups) if s1 in group['members']), None)
    group_b_index = next((index for index, group in enumerate(working_groups) if s2 in group['members']), None)

    if group_a_index is None or group_b_index is None:
        return working_groups, {
            'status': 'missing',
            'missing_ids': [server_id for server_id, index in [(s1, group_a_index), (s2, group_b_index)] if index is None]
        }

    affected_indexes = sorted(set([group_a_index, group_b_index]))
    source_groups = [_clone_group(working_groups[index]) for index in affected_indexes]
    source_rows = sorted({row for group in source_groups for row in group['row_indices']})
    involved_members = sorted({member for group in source_groups for member in group['members']})
    requested_members = sorted({s1, s2})
    leftover_members = [member for member in involved_members if member not in requested_members]

    anchor_rows = source_rows or [group['anchor_row'] for group in source_groups]
    requested_anchor = anchor_rows[0]

    requested_group = {
        'target': min(requested_members),
        'members': requested_members,
        'row_indices': [requested_anchor],
        'anchor_row': requested_anchor
    }

    leftover_group = None
    if leftover_members:
        leftover_anchor = anchor_rows[1] if len(anchor_rows) > 1 else requested_anchor
        leftover_group = {
            'target': min(leftover_members),
            'members': leftover_members,
            'row_indices': [leftover_anchor],
            'anchor_row': leftover_anchor
        }

    new_groups = []
    replaced = False

    for index, group in enumerate(working_groups):
        if index in affected_indexes:
            if not replaced:
                new_groups.append(requested_group)
                if leftover_group:
                    new_groups.append(leftover_group)
                replaced = True
            continue
        new_groups.append(group)

    new_groups.sort(key=lambda item: item['anchor_row'])

    return new_groups, {
        'status': 'ok',
        'source_groups': source_groups,
        'source_rows': source_rows,
        'requested_group': requested_group,
        'leftover_group': leftover_group
    }

def evaluate_primary_warning(server_info_source, s1, s2, total_servers):
    row1 = get_server_info(server_info_source, s1)
    row2 = get_server_info(server_info_source, s2)
    missing_ids = []

    if row1 is None:
        missing_ids.append(s1)
    if row2 is None:
        missing_ids.append(s2)

    if row1 is None or row2 is None:
        return {
            'triggered': False,
            'reasons': [],
            'missing_ids': missing_ids
        }

    rank1 = row1['真实排名']
    rank2 = row2['真实排名']

    cond_rank_close = abs(rank1 - rank2) <= 5
    top_25_threshold = total_servers * 0.25
    cond_high_value = (
        rank1 <= top_25_threshold and rank2 <= top_25_threshold and
        row1['最高玩家累充金额'] >= 5000 and row2['最高玩家累充金额'] >= 5000
    )
    cond_power_close = abs(row1['前2名战力之和'] - row2['前2名战力之和']) <= 500000000

    reasons = []
    if cond_rank_close:
        reasons.append(f"排名接近(差{abs(rank1-rank2)})")
    if cond_high_value:
        reasons.append("高战高充(前25%)")
    if cond_power_close:
        reasons.append("战力接近(差<=5亿)")

    return {
        'triggered': bool(reasons),
        'reasons': reasons,
        'missing_ids': []
    }

def evaluate_secondary_dau_warning(server_info_source, leftover_members, primary_triggered):
    if not primary_triggered or not leftover_members:
        return {
            'triggered': False,
            'low_dau_ids': [],
            'reason': ''
        }

    low_dau_items = []

    for server_id in sorted(set(leftover_members)):
        row = get_server_info(server_info_source, server_id)
        if row is None:
            continue

        dau = int(row['DAU'])
        if dau < 5:
            low_dau_items.append((server_id, dau))

    if not low_dau_items:
        return {
            'triggered': False,
            'low_dau_ids': [],
            'reason': ''
        }

    reason = "剩余组存在低 DAU 区服: " + "; ".join(f"{server_id} DAU<5({dau})" for server_id, dau in low_dau_items)
    return {
        'triggered': True,
        'low_dau_ids': [server_id for server_id, _ in low_dau_items],
        'reason': reason
    }

def build_alert_row(row, group_id, reason, alert_type):
    row_dict = row.to_dict()
    row_dict['警报组ID'] = group_id
    row_dict['警报原因'] = reason
    row_dict['警报类型'] = alert_type

    for column in ID_LIKE_COLUMNS:
        if column not in row_dict:
            continue
        try:
            if isinstance(row_dict[column], (float, int, np.integer)):
                row_dict[column] = int(row_dict[column])
        except Exception:
            pass

    return row_dict

def build_output_rows_from_groups(groups):
    output_rows = []

    for group in groups:
        participants = [member for member in group['members'] if member != group['target']]
        output_rows.append({
            '目标服': group['target'],
            '参与服': ",".join(str(member) for member in participants),
            'anchor_row': group['anchor_row']
        })

    return output_rows

def merge_output_rows_by_target(rows):
    merged = {}
    order = []

    for row in rows:
        target = row.get('目标服')
        if target is None:
            continue

        if target not in merged:
            merged[target] = {
                '目标服': target,
                '参与服_set': set(),
                'anchor_row': row.get('anchor_row')
            }
            order.append(target)

        merged[target]['参与服_set'].update(parse_server_ids_from_cell(row.get('参与服')))

        anchor_row = row.get('anchor_row')
        if anchor_row is not None:
            current_anchor = merged[target]['anchor_row']
            merged[target]['anchor_row'] = anchor_row if current_anchor is None else min(current_anchor, anchor_row)

    merged_rows = []

    for target in order:
        merged_row = merged[target]
        participants = sorted(server_id for server_id in merged_row['参与服_set'] if server_id != target)
        output = {
            '目标服': target,
            '参与服': ",".join(str(server_id) for server_id in participants)
        }
        if merged_row['anchor_row'] is not None:
            output['anchor_row'] = merged_row['anchor_row']
        merged_rows.append(output)

    merged_rows.sort(key=lambda item: item.get('anchor_row', float('inf')))
    return merged_rows

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        logger = ExecutionLogger()
        try:
            logger.user("开始处理任务...")
            logger.dev("初始化请求参数解析")
            
            # 1. Save files
            csv_files = request.files.getlist('csv_files')
            xlsx_file = request.files['xlsx_file']
            pairs_text = request.form['pairs_text']

            if not csv_files or not xlsx_file:
                return "Missing files", 400

            xlsx_path = os.path.join(app.config['UPLOAD_FOLDER'], 'input.xlsx')
            xlsx_file.save(xlsx_path)
            logger.user("合服计划表 (XLSX) 上传成功")

            # 2. Process CSVs (Merge Multiple)
            logger.user(f"正在处理 {len(csv_files)} 个服务器数据文件...")
            dfs = []
            for i, file in enumerate(csv_files):
                if file.filename == '':
                    continue
                temp_path = os.path.join(app.config['UPLOAD_FOLDER'], f'input_{i}.csv')
                file.save(temp_path)
                try:
                    # 尝试读取 CSV，跳过第一行
                    df_temp = pd.read_csv(temp_path, header=1)
                    dfs.append(df_temp)
                    logger.dev(f"读取 CSV {file.filename} 成功，行数: {len(df_temp)}")
                except Exception as e:
                    logger.user(f"读取文件 {file.filename} 失败", 'ERROR')
                    logger.dev(f"CSV 读取异常: {str(e)}", 'ERROR')
            
            if not dfs:
                 return "没有有效的 CSV 文件", 400
                 
            df = pd.concat(dfs, ignore_index=True)
            logger.user(f"数据合并完成，共 {len(df)} 条记录")
            
            # Ensure numeric columns
            cols_to_numeric = ['区服ID', '前2名战力之和', '最高玩家累充金额', 'DAU', '跨服ID', 'code', '有效DAU', '当天付费账号数', '峰值在线', 'MAC_DAU', 'IP_DAU', '账号DAU', '总注册角色']
            for col in cols_to_numeric:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                    if col in ['区服ID', 'DAU', '跨服ID', 'code', '总注册角色', '峰值在线', '当天付费账号数']: # Explicitly cast ID-like or count-like fields to int
                         try:
                            df[col] = df[col].astype(int)
                         except:
                            pass # Keep as float if int conversion fails (e.g. too large or weird values)

            # Sort
            logger.dev("执行数据排序: 前2名战力之和 (降序)")
            df = df.sort_values(by='前2名战力之和', ascending=False).reset_index(drop=True)
            df['真实排名'] = df.index + 1
            total_servers = len(df)
            server_info_map = build_server_info_map(df)
            
            input_pairs, duplicates = parse_server_pairs(pairs_text)
            
            if duplicates:
                logger.user(f"发现并忽略 {len(duplicates)} 组重复检测对", 'WARN')
                if len(duplicates) <= 5:
                    for dup in duplicates:
                        logger.dev(f"忽略重复: {dup}", 'WARN')
                else:
                    logger.dev(f"重复列表 (前5个): {', '.join(duplicates[:5])}...", 'WARN')
            
            logger.user(f"解析输入：共 {len(input_pairs)} 组有效检测区服")

            logger.dev("加载 XLSX 并解析现有逻辑组合")
            wb = load_workbook(xlsx_path)
            ws = wb.active
            
            header_row = [cell.value for cell in ws[1]]
            try:
                target_col_idx = header_row.index('目标服') 
                part_col_idx = header_row.index('参与服')
            except ValueError:
                target_col_idx = 0
                part_col_idx = 1

            plan_groups = build_plan_groups(build_plan_rows(ws, target_col_idx, part_col_idx))
            logger.user(f"识别到 {len(plan_groups)} 个现有逻辑组合")

            alert_groups = []
            secondary_alert_groups = []
            final_alert_rows = []
            swapped_log_data = []
            changed_anchor_rows = set()
            fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")

            logger.dev("开始执行重组与双阶段预警")
            for s1, s2 in input_pairs:
                primary_warning = evaluate_primary_warning(server_info_map, s1, s2, total_servers)
                if primary_warning['missing_ids']:
                    for missing_id in primary_warning['missing_ids']:
                        logger.user(f"警告：区服 {missing_id} 缺少数值数据，本次仅执行重组，不参与预警判定", 'WARN')

                plan_groups, regroup_result = regroup_for_requested_pair(plan_groups, s1, s2)
                if regroup_result['status'] != 'ok':
                    logger.user(f"警告：区服对 {s1}, {s2} 未能在计划表中找到完整来源组合，已跳过重组", 'WARN')
                    continue

                requested_group = regroup_result['requested_group']
                leftover_group = regroup_result['leftover_group']
                changed_anchor_rows.update(group['anchor_row'] for group in [requested_group, leftover_group] if group)

                reason_text = "；".join(primary_warning['reasons']) if primary_warning['reasons'] else "未触发常规预警"
                logger.user(
                    f"已重组 {s1} + {s2}：请求组 {format_group_label(requested_group)}；"
                    f"剩余组 {format_group_label(leftover_group)}；常规预警：{reason_text}"
                )

                if primary_warning['triggered']:
                    primary_reason = "; ".join(primary_warning['reasons'])
                    alert_groups.append({'ids': [s1, s2], 'reason': primary_reason})
                    logger.user(f"发现常规预警：{s1} 和 {s2} - {primary_reason}", 'WARN')

                    for server_id in [s1, s2]:
                        row = get_server_info(server_info_map, server_id)
                        if row is not None:
                            final_alert_rows.append(
                                build_alert_row(row, f"Primary_{min(s1, s2)}_{max(s1, s2)}", primary_reason, '常规预警')
                            )

                secondary_warning = evaluate_secondary_dau_warning(
                    server_info_map,
                    leftover_group['members'] if leftover_group else [],
                    primary_warning['triggered']
                )
                if secondary_warning['triggered'] and leftover_group:
                    secondary_alert_groups.append({
                        'ids': list(leftover_group['members']),
                        'reason': secondary_warning['reason']
                    })
                    logger.user(
                        f"触发二次 DAU 预警：剩余组 {format_group_label(leftover_group)} - {secondary_warning['reason']}",
                        'WARN'
                    )

                    for server_id in leftover_group['members']:
                        row = get_server_info(server_info_map, server_id)
                        if row is not None:
                            final_alert_rows.append(
                                build_alert_row(
                                    row,
                                    f"Secondary_{min(s1, s2)}_{max(s1, s2)}",
                                    secondary_warning['reason'],
                                    '二次DAU预警'
                                )
                            )

                source_groups = regroup_result['source_groups']
                swapped_log_data.append({
                    '合并申请': f"{s1}+{s2}",
                    '原始行号1': ",".join(str(row) for row in source_groups[0]['row_indices']) if source_groups else '-',
                    '原始行号2': ",".join(str(row) for row in source_groups[1]['row_indices']) if len(source_groups) > 1 else '-',
                    'Before1': format_group_label(source_groups[0]) if source_groups else '空',
                    'After1': format_group_label(requested_group),
                    'Before2': format_group_label(source_groups[1]) if len(source_groups) > 1 else '同组拆分',
                    'After2': format_group_label(leftover_group),
                    '状态': '已重组'
                })

            logger.user(
                f"处理完成：发现 {len(alert_groups)} 组常规预警，"
                f"{len(secondary_alert_groups)} 组二次 DAU 预警，"
                f"成功重组 {len(swapped_log_data)} 组"
            )

            # Create Alert CSV with optimized formatting
            if final_alert_rows:
                alert_df = pd.DataFrame(final_alert_rows)
                
                # Define columns to keep
                # Base cols from user requirement
                base_cols_to_keep = [
                    '区服ID', 'DAU', '近3日收入', '近7日收入', 
                    '第一名战力', '第二名战力', '第三名战力', 
                    '前2名战力之和', '前3名战力之和', 
                    '前十平均战力', '前十平均等级', '最高玩家累充金额'
                ]
                # Added cols by logic
                added_cols_to_keep = ['真实排名', '警报类型', '警报组ID', '警报原因']
                
                all_keep_cols = added_cols_to_keep + base_cols_to_keep
                
                # Filter columns: intersection of what we want and what exists
                final_cols = [c for c in all_keep_cols if c in alert_df.columns]
                
                alert_df = alert_df[final_cols]
                
                # Add empty rows between groups for visual separation
                # Convert to list of dicts to easily insert rows
                # Sort by Group ID to ensure they are contiguous, and then by Real Rank
                alert_df.sort_values(by=['警报组ID', '真实排名'], ascending=[True, True], inplace=True)
                
                output_rows = []
                current_group = None
                
                for _, row in alert_df.iterrows():
                    if current_group is not None and row['警报组ID'] != current_group:
                        # Insert empty row (dict with all empty strings to prevent float promotion)
                        empty_row = {c: "" for c in alert_df.columns}
                        output_rows.append(empty_row)
                    
                    output_rows.append(row.to_dict())
                    current_group = row['警报组ID']
                
                final_df = pd.DataFrame(output_rows)
                output_csv_path = os.path.join(app.config['DOWNLOAD_FOLDER'], 'alert_result.csv')
                final_df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
            else:
                pd.DataFrame().to_csv(os.path.join(app.config['DOWNLOAD_FOLDER'], 'alert_result.csv'), index=False)

            final_plan_rows = merge_output_rows_by_target(build_output_rows_from_groups(plan_groups))

            for r_idx in range(2, ws.max_row + 1):
                ws.cell(row=r_idx, column=target_col_idx + 1).value = None
                ws.cell(row=r_idx, column=part_col_idx + 1).value = None

            for row in final_plan_rows:
                anchor_row = row.get('anchor_row')
                if anchor_row is None or anchor_row > ws.max_row:
                    continue

                ws.cell(row=anchor_row, column=target_col_idx + 1).value = row['目标服']
                ws.cell(row=anchor_row, column=part_col_idx + 1).value = row['参与服'] or None

                if anchor_row in changed_anchor_rows:
                    for cell in ws[anchor_row]:
                        cell.fill = fill

            if swapped_log_data:
                swapped_df = pd.DataFrame(swapped_log_data)
                output_swapped_path = os.path.join(app.config['DOWNLOAD_FOLDER'], 'swapped_log.csv')
                swapped_df.to_csv(output_swapped_path, index=False, encoding='utf-8-sig')
            else:
                pd.DataFrame().to_csv(os.path.join(app.config['DOWNLOAD_FOLDER'], 'swapped_log.csv'), index=False)

            output_xlsx_path = os.path.join(app.config['DOWNLOAD_FOLDER'], 'result_plan.xlsx')
            wb.save(output_xlsx_path)
            logger.user("所有任务处理完成！", 'SUCCESS')

            return render_template('index.html', 
                                   success=True, 
                                   logs=logger.logs,
                                   alert_csv='alert_result.csv', 
                                   swapped_csv='swapped_log.csv',
                                   result_xlsx='result_plan.xlsx',
                                   alert_count=len(alert_groups),
                                   secondary_alert_count=len(secondary_alert_groups),
                                   swap_count=len(swapped_log_data),
                                   alert_preview=alert_groups,
                                   secondary_alert_preview=secondary_alert_groups,
                                   swap_preview=swapped_log_data)

        except Exception as e:
            traceback.print_exc()
            return f"Error: {str(e)}", 500

    return render_template('index.html')

@app.route('/download/<filename>')
def download_file(filename):
    return send_from_directory(app.config['DOWNLOAD_FOLDER'], filename, as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
