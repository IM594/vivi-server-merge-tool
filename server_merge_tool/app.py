import os
import pandas as pd
from flask import Flask, render_template, request, send_file, send_from_directory
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
import traceback

app = Flask(__name__)
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'uploads')
DOWNLOAD_FOLDER = os.path.join(BASE_DIR, 'downloads')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['DOWNLOAD_FOLDER'] = DOWNLOAD_FOLDER

def parse_server_pairs(text):
    pairs = []
    if not text:
        return pairs
    lines = text.strip().split('\n')
    for line in lines:
        parts = line.replace('，', ',').split(',')
        if len(parts) >= 2:
            try:
                s1 = int(parts[0].strip())
                s2 = int(parts[1].strip())
                pairs.append((s1, s2))
            except ValueError:
                continue
    return pairs

def get_server_info(df, server_id):
    # df index is reset, but we want to find row by '区服ID'
    # server_id should be int
    row = df[df['区服ID'] == server_id]
    if row.empty:
        return None
    return row.iloc[0]

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        try:
            # 1. Save files
            csv_files = request.files.getlist('csv_files')
            xlsx_file = request.files['xlsx_file']
            pairs_text = request.form['pairs_text']

            if not csv_files or not xlsx_file:
                return "Missing files", 400

            xlsx_path = os.path.join(app.config['UPLOAD_FOLDER'], 'input.xlsx')
            xlsx_file.save(xlsx_path)

            # 2. Process CSVs (Merge Multiple)
            dfs = []
            for i, file in enumerate(csv_files):
                if file.filename == '':
                    continue
                # Save temporarily or read directly? Reading directly is cleaner if not too huge.
                # But for consistency let's save.
                temp_path = os.path.join(app.config['UPLOAD_FOLDER'], f'input_{i}.csv')
                file.save(temp_path)
                
                # Note: Row 0 is index, Row 1 is Header. So header=1.
                try:
                    df_temp = pd.read_csv(temp_path, header=1)
                    dfs.append(df_temp)
                except Exception as e:
                    print(f"Error reading CSV {file.filename}: {e}")
            
            if not dfs:
                 return "No valid CSV files uploaded", 400
                 
            df = pd.concat(dfs, ignore_index=True)
            print(f"DEBUG: Merged {len(dfs)} CSV files. Total rows: {len(df)}")
            
            # Ensure numeric columns
            cols_to_numeric = ['区服ID', '前2名战力之和', '最高玩家累充金额', 'DAU']
            for col in cols_to_numeric:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # Sort by '前2名战力之和' descending
            df = df.sort_values(by='前2名战力之和', ascending=False).reset_index(drop=True)
            # Add rank (1-based)
            df['真实排名'] = df.index + 1
            total_servers = len(df)
            
            input_pairs = parse_server_pairs(pairs_text)
            print(f"DEBUG: Parsed {len(input_pairs)} pairs from input.")
            
            alert_groups = [] # List of tuples (id1, id2, reason)
            normal_groups = [] # List of tuples (id1, id2)
            
            # 3. Primary Alert Check
            for s1, s2 in input_pairs:
                print(f"DEBUG: Checking pair ({s1}, {s2})")
                row1 = get_server_info(df, s1)
                row2 = get_server_info(df, s2)
                
                if row1 is None:
                    print(f"WARNING: Server {s1} not found in CSV!")
                if row2 is None:
                    print(f"WARNING: Server {s2} not found in CSV!")

                if row1 is None or row2 is None:
                    # If server not found in CSV, what to do? Assume Normal or Skip?
                    # Let's log it but maybe skip processing for now to avoid crash
                    continue
                    
                rank1 = row1['真实排名']
                rank2 = row2['真实排名']
                
                # Conditions
                # a. Rank diff <= 5
                cond_a = abs(rank1 - rank2) <= 5
                
                # b. Both in Top 25% AND Both MaxRecharge >= 5000
                top_25_threshold = total_servers * 0.25
                cond_b = (rank1 <= top_25_threshold and rank2 <= top_25_threshold and
                          row1['最高玩家累充金额'] >= 5000 and row2['最高玩家累充金额'] >= 5000)
                          
                # c. Power sum diff <= 1,000,000,000
                power1 = row1['前2名战力之和']
                power2 = row2['前2名战力之和']
                cond_c = abs(power1 - power2) <= 1000000000
                
                if cond_a or cond_b or cond_c:
                    reasons = []
                    if cond_a: reasons.append("排名差距<=5")
                    if cond_b: reasons.append("前25%且累充>=5000")
                    if cond_c: reasons.append("战力差<=10亿")
                    alert_groups.append({'ids': [s1, s2], 'reason': "; ".join(reasons), 'type': 'primary'})
                else:
                    normal_groups.append((s1, s2))
            
            # 4. Secondary Alert Check (Load XLSX)
            # Load XLSX for lookup
            wb = load_workbook(xlsx_path)
            ws = wb.active
            
            # Build a map of ServerID -> (RowIndex, ColIndex(Target/Part))
            # Assuming columns: '目标服' (A), '参与服' (B)
            # Headers are likely row 1. Data starts row 2.
            # Let's find column indices by name just in case
            header_row = [cell.value for cell in ws[1]]
            try:
                target_col_idx = header_row.index('目标服') # 0-based
                part_col_idx = header_row.index('参与服')
            except ValueError:
                # Fallback if header not found, assume A=0, B=1
                target_col_idx = 0
                part_col_idx = 1

            # Helper to find partner in XLSX
            def find_partner_in_xlsx(server_id):
                # Returns (row_idx, partner_id)
                # Only scanning rows
                for r_idx, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
                    t_id = row[target_col_idx]
                    p_id = row[part_col_idx]
                    
                    if t_id == server_id:
                        return r_idx, p_id
                    if p_id == server_id:
                        return r_idx, t_id
                return None, None

            final_alert_rows = [] # List of dicts (row data from CSV + Extra info)
            
            # Add Primary Alerts to output first
            for group in alert_groups:
                # Each group is a "Alert Set"
                group_id = f"Group_{group['ids'][0]}_{group['ids'][1]}"
                for sid in group['ids']:
                    r = get_server_info(df, sid)
                    if r is not None:
                        r_dict = r.to_dict()
                        r_dict['Alert_Group'] = group_id
                        r_dict['Alert_Reason'] = group['reason']
                        final_alert_rows.append(r_dict)
                        
                # Perform Secondary Check
                # For s1 and s2, find their partners in XLSX
                s1, s2 = group['ids'][0], group['ids'][1]
                
                partners = []
                _, p1 = find_partner_in_xlsx(s1)
                if p1: partners.append(p1)
                
                _, p2 = find_partner_in_xlsx(s2)
                if p2: partners.append(p2)
                
                # Check DAU for s1, s2, p1, p2
                # Actually requirement says: "For these two pairs... check each number's DAU <= 5"
                # "If ANY <= 5, trigger alert"
                to_check = [s1, s2] + partners
                triggered_secondary = False
                
                for cid in to_check:
                    cr = get_server_info(df, cid)
                    if cr is not None and cr['DAU'] <= 5:
                        triggered_secondary = True
                        break
                
                if triggered_secondary:
                    # Add partners to alert output if triggered
                    # Requirement: "Add these alerts to the query alerts"
                    for pid in partners:
                        pr = get_server_info(df, pid)
                        if pr is not None:
                            pr_dict = pr.to_dict()
                            pr_dict['Alert_Group'] = group_id
                            pr_dict['Alert_Reason'] = "二次查询DAU<=5"
                            # Avoid duplicates if possible, but user said "accumulate"
                            final_alert_rows.append(pr_dict)

            # Create Alert CSV
            if final_alert_rows:
                alert_df = pd.DataFrame(final_alert_rows)
                output_csv_path = os.path.join(app.config['DOWNLOAD_FOLDER'], 'alert_result.csv')
                alert_df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
            else:
                # Create empty CSV if no alerts
                pd.DataFrame().to_csv(os.path.join(app.config['DOWNLOAD_FOLDER'], 'alert_result.csv'), index=False)

            # 5. Swap Servers (Normal Groups)
            fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
            
            swapped_log_data = [] # List of dicts for logging swapped pairs

            # We need to modify the workbook
            # Reload to be safe or use existing 'ws'
            # We need cell access, iter_rows with values_only=True gave us values.
            # Let's iterate to find rows for swapping.
            
            # Build map for fast lookup: ServerID -> RowIndex (1-based for openpyxl)
            # Re-scanning might be slow if huge, but file likely manageable.
            # Let's just scan linearly for each swap to be robust? Or build map.
            server_row_map = {}
            for r_idx, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
                t_id = row[target_col_idx]
                p_id = row[part_col_idx]
                if t_id: server_row_map[t_id] = r_idx
                if p_id: server_row_map[p_id] = r_idx
                
            actual_swapped_count = 0
            
            for s1, s2 in normal_groups:
                r1_idx = server_row_map.get(s1)
                r2_idx = server_row_map.get(s2)
                
                if r1_idx and r2_idx and r1_idx != r2_idx:
                    actual_swapped_count += 1
                    # Log before swap
                    swapped_log_data.append({
                        'Swap_Pair_ID_1': s1,
                        'Swap_Pair_ID_2': s2,
                        'Original_Row_Index_1': r1_idx,
                        'Original_Row_Index_2': r2_idx,
                        'Status': 'Swapped'
                    })
                    
                    # Perform Swap
                    # We need to identify where s1 is in r1 (Target or Part)
                    # And where s2 is in r2
                    
                    # Get current values
                    # Row 1
                    c1_t = ws.cell(row=r1_idx, column=target_col_idx+1)
                    c1_p = ws.cell(row=r1_idx, column=part_col_idx+1)
                    v1_t = c1_t.value
                    v1_p = c1_p.value
                    
                    # Row 2
                    c2_t = ws.cell(row=r2_idx, column=target_col_idx+1)
                    c2_p = ws.cell(row=r2_idx, column=part_col_idx+1)
                    v2_t = c2_t.value
                    v2_p = c2_p.value
                    
                    # Replace s1 with s2 in Row 1's values
                    new_v1_t = s2 if v1_t == s1 else v1_t
                    new_v1_p = s2 if v1_p == s1 else v1_p
                    
                    # Replace s2 with s1 in Row 2's values
                    new_v2_t = s1 if v2_t == s2 else v2_t
                    new_v2_p = s1 if v2_p == s2 else v2_p
                    
                    # Re-order pairs so smaller is Target
                    pair1 = [new_v1_t, new_v1_p]
                    # Filter None
                    pair1 = [x for x in pair1 if x is not None]
                    if len(pair1) == 2:
                        pair1.sort()
                        c1_t.value = pair1[0]
                        c1_p.value = pair1[1]
                    
                    pair2 = [new_v2_t, new_v2_p]
                    pair2 = [x for x in pair2 if x is not None]
                    if len(pair2) == 2:
                        pair2.sort()
                        c2_t.value = pair2[0]
                        c2_p.value = pair2[1]
                        
                    # Color rows
                    for cell in ws[r1_idx]:
                        cell.fill = fill
                    for cell in ws[r2_idx]:
                        cell.fill = fill

                    # Update map
                    server_row_map[s1] = r2_idx
                    server_row_map[s2] = r1_idx
                
                else:
                    print(f"WARNING: Cannot swap ({s1}, {s2}). Not found in XLSX or same row. r1={r1_idx}, r2={r2_idx}")

            # Create Swapped Log CSV
            if swapped_log_data:
                swapped_df = pd.DataFrame(swapped_log_data)
                output_swapped_path = os.path.join(app.config['DOWNLOAD_FOLDER'], 'swapped_log.csv')
                swapped_df.to_csv(output_swapped_path, index=False, encoding='utf-8-sig')
            else:
                pd.DataFrame().to_csv(os.path.join(app.config['DOWNLOAD_FOLDER'], 'swapped_log.csv'), index=False)

            output_xlsx_path = os.path.join(app.config['DOWNLOAD_FOLDER'], 'result_plan.xlsx')
            wb.save(output_xlsx_path)

            return render_template('index.html', 
                                   success=True, 
                                   alert_csv='alert_result.csv', 
                                   swapped_csv='swapped_log.csv',
                                   result_xlsx='result_plan.xlsx',
                                   alert_count=len(alert_groups),
                                   swap_count=actual_swapped_count)

        except Exception as e:
            traceback.print_exc()
            return f"Error: {str(e)}", 500

    return render_template('index.html')

@app.route('/download/<filename>')
def download_file(filename):
    return send_from_directory(app.config['DOWNLOAD_FOLDER'], filename, as_attachment=True)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
