import json
import os
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

try:
    import paramiko
except Exception:
    paramiko = None


HOST = '10.7.129.239'
USERNAME = 'rsp'
PASSWORD = '123qweASD'
REMOTE_DIR = '/home/rsp/air_pollution_collector/uri_aplogger/output/'
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
LOCAL_PATH = os.path.join(BASE_DIR, 'merged_data_latest.csv')
NOTES_PATH = os.path.join(BASE_DIR, 'user_notes.csv')
LABELS_PATH = os.path.join(BASE_DIR, 'user_labels.csv')
THRESHOLDS_PATH = os.path.join(BASE_DIR, 'warning_thresholds.json')
LABEL_PREFIXES_PATH = os.path.join(BASE_DIR, 'label_prefixes.json')
LABEL_PRESETS_PATH = os.path.join(BASE_DIR, 'label_presets.json')
USE_LOCAL_DATA = True
OFFLINE_SENTINEL = -9999
DEFAULT_WINDOW_MINUTES = 30
MAX_MAIN_PLOTS = 5
DEFAULT_PREFIXES = ['None', 'Error', 'Issue', 'Maintenance', 'Measurement', 'Nitrogen', 'Transit']
DEFAULT_LABEL_PRESETS = [
    {'name': 'Measurement', 'minutes': 30},
    {'name': 'Nitrogen', 'minutes': 15},
    {'name': 'Transit', 'minutes': 10},
    {'name': 'Error', 'minutes': None},
    {'name': 'Issue', 'minutes': None},
    {'name': 'Maintenance', 'minutes': None},
]

st.set_page_config(layout='wide')

DEFAULT_STATE = {
    'data_loaded': False,
    'current_data': None,
    'current_filename': None,
    'timestamp_col': None,
    'plot_columns': [],
    'sensor_groups': {},
    'sensor_param_map': {},
    'available_sensors': [],
    'refresh_interval': 1.0,
    'auto_refresh': True,
    'alive_timeout': 5,
    'window_minutes': DEFAULT_WINDOW_MINUTES,
    'notes': pd.DataFrame(columns=['timestamp', 'note']),
    'labels': pd.DataFrame(columns=['prefix', 'name', 'start_time', 'end_time', 'planned_end_time', 'is_active', 'alarm_triggered']),
    'comm_status': 'offline',
    'comm_message': 'Not connected',
    'last_comm_time': None,
    'last_update_time': None,
    'thresholds': {},
    'selected_main_plots': [],
    'plot_limits': {},
    'live_data': None,
    'live_filename': None,
    'view_mode': 'latest',
    'uploaded_file_name': None,
    'uploaded_data': None,
    'label_prefixes': DEFAULT_PREFIXES.copy(),
    'label_presets': DEFAULT_LABEL_PRESETS.copy(),
    'label_prefix_choice': 'None',
    'label_preset_choice': 'Custom',
    'label_draft_name': '',
    'label_draft_minutes': '',
    'pending_alarm_messages': [],
}
for k, v in DEFAULT_STATE.items():
    if k not in st.session_state:
        st.session_state[k] = v.copy() if isinstance(v, (dict, list)) else v


def read_json_file(path, default):
    if not os.path.exists(path):
        return default
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return default


def write_json_file(path, payload):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def load_saved_thresholds():
    loaded = read_json_file(THRESHOLDS_PATH, {})
    if isinstance(loaded, dict):
        st.session_state.thresholds = loaded


def save_thresholds():
    write_json_file(THRESHOLDS_PATH, st.session_state.thresholds)


def load_label_metadata():
    prefixes = read_json_file(LABEL_PREFIXES_PATH, DEFAULT_PREFIXES)
    if isinstance(prefixes, list) and prefixes:
        unique_prefixes = []
        for item in ['None'] + prefixes:
            if item not in unique_prefixes:
                unique_prefixes.append(item)
        st.session_state.label_prefixes = unique_prefixes

    presets = read_json_file(LABEL_PRESETS_PATH, DEFAULT_LABEL_PRESETS)
    if isinstance(presets, list):
        cleaned = []
        for preset in presets:
            if not isinstance(preset, dict):
                continue
            name = str(preset.get('name', '')).strip()
            minutes = preset.get('minutes')
            if not name:
                continue
            if minutes in ['', None]:
                minutes = None
            else:
                try:
                    minutes = float(minutes)
                except Exception:
                    minutes = None
            cleaned.append({'name': name, 'minutes': minutes})
        if cleaned:
            st.session_state.label_presets = cleaned


def save_label_prefixes():
    prefixes = [p for p in st.session_state.label_prefixes if p and p != 'None']
    write_json_file(LABEL_PREFIXES_PATH, prefixes)


def save_label_presets():
    write_json_file(LABEL_PRESETS_PATH, st.session_state.label_presets)


def load_saved_notes():
    if os.path.exists(NOTES_PATH):
        try:
            notes_df = pd.read_csv(NOTES_PATH)
            if 'timestamp' in notes_df.columns:
                notes_df['timestamp'] = pd.to_datetime(notes_df['timestamp'], errors='coerce')
            else:
                notes_df['timestamp'] = pd.NaT
            if 'note' not in notes_df.columns:
                notes_df['note'] = ''
            st.session_state.notes = notes_df[['timestamp', 'note']].copy()
        except Exception:
            st.session_state.notes = pd.DataFrame(columns=['timestamp', 'note'])


def save_notes():
    notes = st.session_state.notes.copy()
    if not notes.empty:
        notes['timestamp'] = pd.to_datetime(notes['timestamp'], errors='coerce')
    notes.to_csv(NOTES_PATH, index=False)


def load_saved_labels():
    if os.path.exists(LABELS_PATH):
        try:
            labels_df = pd.read_csv(LABELS_PATH)
            for col in ['start_time', 'end_time', 'planned_end_time']:
                if col in labels_df.columns:
                    labels_df[col] = pd.to_datetime(labels_df[col], errors='coerce')
                else:
                    labels_df[col] = pd.NaT
            for col, default in [('prefix', ''), ('name', ''), ('is_active', False), ('alarm_triggered', False)]:
                if col not in labels_df.columns:
                    labels_df[col] = default
            st.session_state.labels = labels_df[['prefix', 'name', 'start_time', 'end_time', 'planned_end_time', 'is_active', 'alarm_triggered']].copy()
        except Exception:
            st.session_state.labels = DEFAULT_STATE['labels'].copy()


def save_labels():
    labels = st.session_state.labels.copy()
    for col in ['start_time', 'end_time', 'planned_end_time']:
        if col in labels.columns:
            labels[col] = pd.to_datetime(labels[col], errors='coerce')
    labels.to_csv(LABELS_PATH, index=False)


def add_note(note_text: str):
    note_text = (note_text or '').strip()
    if not note_text:
        return False
    new_note = pd.DataFrame({
        'timestamp': [datetime.now().replace(microsecond=0)],
        'note': [note_text],
    })
    st.session_state.notes = pd.concat([st.session_state.notes, new_note], ignore_index=True)
    save_notes()
    return True

def safe_text(value):
    if value is None:
        return ''
    try:
        if pd.isna(value):
            return ''
    except Exception:
        pass
    return str(value).strip()


def label_display_name(prefix, name):
    prefix = safe_text(prefix)
    name = safe_text(name)
    if prefix and prefix.lower() != 'none':
        return f'{prefix}: {name}' if name else prefix
    return name


def apply_label_preset():
    chosen = st.session_state.get('label_preset_choice', 'Custom')
    if chosen == 'Custom':
        return
    for preset in st.session_state.label_presets:
        if preset['minutes'] is None:
            duration_text = 'untimed'
        else:
            duration_text = f"{preset['minutes']} min"
        preset_label = f"{preset['name']} ({duration_text})"
        if preset_label == chosen:
            st.session_state.label_draft_name = preset['name']
            st.session_state.label_draft_minutes = '' if preset['minutes'] is None else str(preset['minutes'])
            return


def add_custom_prefix(prefix_name: str):
    prefix_name = (prefix_name or '').strip()
    if not prefix_name or prefix_name in st.session_state.label_prefixes:
        return False
    st.session_state.label_prefixes.append(prefix_name)
    save_label_prefixes()
    return True


def add_label_preset(name: str, minutes_text: str):
    name = (name or '').strip()
    minutes_text = (minutes_text or '').strip()
    if not name:
        return False
    minutes = None
    if minutes_text:
        try:
            minutes = float(minutes_text)
        except Exception:
            return False
    preset = {'name': name, 'minutes': minutes}
    for existing in st.session_state.label_presets:
        if existing == preset:
            return False
    st.session_state.label_presets.append(preset)
    save_label_presets()
    return True


def start_label(prefix: str, name: str, minutes_text: str):
    name = (name or '').strip()
    if not name:
        return False, 'Label name is required'
    prefix = '' if not prefix or prefix == 'None' else prefix.strip()
    start_time = datetime.now().replace(microsecond=0)
    planned_end_time = pd.NaT
    if (minutes_text or '').strip():
        try:
            minutes = float(minutes_text)
            planned_end_time = start_time + timedelta(minutes=minutes)
        except Exception:
            return False, 'Time must be a valid number of minutes'

    new_label = pd.DataFrame({
        'prefix': [prefix],
        'name': [name],
        'start_time': [start_time],
        'end_time': [pd.NaT],
        'planned_end_time': [planned_end_time],
        'is_active': [True],
        'alarm_triggered': [False],
    })
    st.session_state.labels = pd.concat([st.session_state.labels, new_label], ignore_index=True)
    save_labels()
    return True, 'Label started'


def end_label(index: int, early=False):
    if index not in st.session_state.labels.index:
        return
    end_time = datetime.now().replace(microsecond=0) if early else st.session_state.labels.at[index, 'planned_end_time']
    if pd.isna(end_time):
        end_time = datetime.now().replace(microsecond=0)
    st.session_state.labels.at[index, 'end_time'] = end_time
    st.session_state.labels.at[index, 'is_active'] = False
    save_labels()


def process_label_alarms():
    if st.session_state.labels.empty:
        return
    now = datetime.now()
    pending = []
    changed = False
    labels = st.session_state.labels.copy()
    for idx, row in labels.iterrows():
        planned_end = pd.to_datetime(row.get('planned_end_time'), errors='coerce')
        if row.get('is_active') is True and pd.notna(planned_end) and planned_end <= now:
            st.session_state.labels.at[idx, 'end_time'] = planned_end
            st.session_state.labels.at[idx, 'is_active'] = False
            changed = True
            if not bool(row.get('alarm_triggered', False)):
                st.session_state.labels.at[idx, 'alarm_triggered'] = True
                pending.append(f"Label ended: {label_display_name(row.get('prefix', ''), row.get('name', ''))}")
                changed = True
    if pending:
        existing = list(st.session_state.pending_alarm_messages)
        st.session_state.pending_alarm_messages = existing + pending
    if changed:
        save_labels()


def coerce_timestamp_column(df: pd.DataFrame):
    timestamp_cols = [col for col in df.columns if 'merge_timestamp' in col.lower()]
    if not timestamp_cols:
        return None, None
    timestamp_col = timestamp_cols[0]
    out = df.copy()
    out[timestamp_col] = pd.to_datetime(out[timestamp_col], errors='coerce', format='mixed')
    out = out.dropna(subset=[timestamp_col]).sort_values(timestamp_col)
    return out, timestamp_col


def extract_sensor_groups(columns, timestamp_col):
    sensor_groups = {}
    sensor_param_map = {}
    for col in columns:
        if col == timestamp_col or col in ['user_notes', '_connection_gap']:
            continue
        if '_' in col:
            sensor_name, parameter = col.split('_', 1)
        else:
            sensor_name, parameter = 'General', col
        sensor_name = sensor_name.replace('-', ' ').replace('_', ' ').title()
        sensor_groups.setdefault(sensor_name, [])
        if parameter not in sensor_groups[sensor_name]:
            sensor_groups[sensor_name].append(parameter)
        sensor_param_map[f'{sensor_name} - {parameter}'] = col
    return sensor_groups, sensor_param_map


def prepare_data(df: pd.DataFrame):
    df_plot, timestamp_col = coerce_timestamp_column(df)
    if df_plot is None:
        return None, None, None, None, None

    exclude_keywords = ['date', 'time', 'timestamp']
    plot_columns = []
    for col in df_plot.columns:
        if col in [timestamp_col, 'user_notes', '_connection_gap']:
            continue
        if any(k in col.lower() for k in exclude_keywords):
            continue
        plot_columns.append(col)

    sensor_groups, sensor_param_map = extract_sensor_groups(plot_columns, timestamp_col)

    st.session_state.timestamp_col = timestamp_col
    st.session_state.plot_columns = plot_columns
    st.session_state.sensor_groups = sensor_groups
    st.session_state.sensor_param_map = sensor_param_map
    st.session_state.available_sensors = list(sensor_groups.keys())

    if not st.session_state.selected_main_plots and plot_columns:
        st.session_state.selected_main_plots = plot_columns[: min(MAX_MAIN_PLOTS, len(plot_columns))]

    return df_plot, timestamp_col, plot_columns, sensor_groups, sensor_param_map


def infer_numeric_columns(df, cols):
    return [c for c in cols if c in df.columns and pd.api.types.is_numeric_dtype(df[c])]


def read_last_local_row(path=LOCAL_PATH):
    if not os.path.exists(path):
        return None, 'Local file not found'
    try:
        with open(path, 'rb') as f:
            f.seek(0, os.SEEK_END)
            end = f.tell()
            if end == 0:
                return None, 'Local file empty'
            pos = end - 1
            while pos > 0:
                f.seek(pos)
                if f.read(1) == b'\n':
                    break
                pos -= 1
            if pos == 0:
                f.seek(0)
            line = f.readline().decode('utf-8').strip()
        if not line:
            return None, 'Could not read local tail'
        header = pd.read_csv(path, nrows=0).columns.tolist()
        row = pd.read_csv(pd.io.common.StringIO('\n'.join([','.join(header), line])))
        return row, None
    except Exception as e:
        return None, f'Local tail failed: {e}'


def is_data_csv(path):
    file_name = os.path.basename(path).lower()
    return file_name == 'merged_data_latest.csv' or (
        file_name.startswith('merged_data_') and file_name.endswith('.csv')
    )


def list_available_local_csvs():
    candidates = []
    base_dir = os.path.dirname(LOCAL_PATH) or '.'
    if os.path.isdir(base_dir):
        for file_name in os.listdir(base_dir):
            full_path = os.path.join(base_dir, file_name)
            if os.path.isfile(full_path) and is_data_csv(full_path):
                candidates.append(full_path)
    return sorted(
        candidates,
        key=lambda p: (os.path.basename(p).lower() != 'merged_data_latest.csv', os.path.basename(p).lower())
    )


def get_latest_local_data_path():
    candidates = list_available_local_csvs()
    if not candidates:
        return LOCAL_PATH

    merged_timestamped = [
        p for p in candidates
        if os.path.basename(p).lower().startswith('merged_data_')
        and os.path.basename(p).lower() != 'merged_data_latest.csv'
    ]
    if merged_timestamped:
        return max(merged_timestamped, key=os.path.getmtime)
    return max(candidates, key=os.path.getmtime)


def get_remote_latest_filename(ssh_client):
    with ssh_client.open_sftp() as sftp:
        files = sftp.listdir(REMOTE_DIR)
    merged_files = [f for f in files if f.startswith('merged_data_') and f.endswith('.csv')]
    if not merged_files:
        return None
    latest_file = None
    latest_ts = None
    for file in merged_files:
        try:
            ts = datetime.strptime(file[12:-4], '%Y%m%d_%H%M%S')
            if latest_ts is None or ts > latest_ts:
                latest_ts = ts
                latest_file = file
        except ValueError:
            continue
    return latest_file


def read_remote_last_row():
    if paramiko is None:
        return None, 'paramiko unavailable', None
    ssh_client = None
    try:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname=HOST, username=USERNAME, password=PASSWORD, timeout=10)
        latest_file = get_remote_latest_filename(ssh_client)
        if not latest_file:
            return None, 'No remote merged file found', None
        remote_path = os.path.join(REMOTE_DIR, latest_file)
        cmd = f"head -n 1 '{remote_path}' && tail -n 1 '{remote_path}'"
        _, stdout, stderr = ssh_client.exec_command(cmd)
        out = stdout.read().decode('utf-8').splitlines()
        err = stderr.read().decode('utf-8').strip()
        if err:
            return None, err, latest_file
        if len(out) < 2:
            return None, 'Remote tail returned insufficient lines', latest_file
        header, line = out[0], out[-1]
        row = pd.read_csv(pd.io.common.StringIO('\n'.join([header, line])))
        return row, None, latest_file
    except Exception as e:
        return None, f'Remote tail failed: {e}', None
    finally:
        if ssh_client:
            ssh_client.close()


def read_data_file(path):
    df = pd.read_csv(path)
    return df, os.path.basename(path)


def set_view_to_latest():
    st.session_state.view_mode = 'latest'
    st.session_state.current_data = st.session_state.live_data.copy() if st.session_state.live_data is not None else None
    st.session_state.current_filename = st.session_state.live_filename


def set_view_to_uploaded(df, filename):
    st.session_state.view_mode = 'uploaded'
    st.session_state.uploaded_data = df.copy()
    st.session_state.uploaded_file_name = filename
    st.session_state.current_data = df.copy()
    st.session_state.current_filename = filename


def append_connection_gap(reason: str):
    df = st.session_state.live_data
    ts_col = st.session_state.timestamp_col or 'merge_timestamp'
    gap_time = datetime.now().replace(microsecond=0)
    if df is None or len(df) == 0:
        gap_row = pd.DataFrame([{ts_col: gap_time, '_connection_gap': True}])
        st.session_state.live_data = gap_row
        if st.session_state.view_mode == 'latest':
            st.session_state.current_data = gap_row.copy()
        return

    row = {col: np.nan for col in df.columns}
    row[ts_col] = gap_time
    row['_connection_gap'] = True
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    for col in numeric_cols:
        row[col] = OFFLINE_SENTINEL
    if 'user_notes' in df.columns:
        row['user_notes'] = f'Connection gap: {reason}'

    updated = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    st.session_state.live_data = updated
    if st.session_state.view_mode == 'latest':
        st.session_state.current_data = updated.copy()


def load_initial_data(selected_path=None):
    path = selected_path or (get_latest_local_data_path() if USE_LOCAL_DATA else LOCAL_PATH)
    try:
        df, label = read_data_file(path)
        if selected_path is None:
            st.session_state.live_data = df.copy()
            st.session_state.live_filename = label
            st.session_state.current_data = df.copy()
            st.session_state.current_filename = label
            st.session_state.view_mode = 'latest'
        else:
            st.session_state.current_data = df.copy()
            st.session_state.current_filename = label
            st.session_state.view_mode = 'uploaded'
        st.session_state.data_loaded = True
        st.session_state.last_update_time = datetime.now()
        st.session_state.comm_status = 'online'
        st.session_state.comm_message = f'Loaded {label}'
        st.session_state.last_comm_time = datetime.now()
        return df, label
    except Exception as e:
        st.session_state.comm_status = 'offline'
        st.session_state.comm_message = str(e)
        return None, None


def refresh_live_data_from_source():
    try:
        if USE_LOCAL_DATA:
            latest_path = get_latest_local_data_path()
            df, label = read_data_file(latest_path)
        else:
            df, label = load_initial_data()
            return df, label

        st.session_state.live_data = df.copy()
        st.session_state.live_filename = label
        st.session_state.data_loaded = True
        st.session_state.last_update_time = datetime.now()
        st.session_state.comm_status = 'online'
        st.session_state.comm_message = f'Reloaded {label}'
        st.session_state.last_comm_time = datetime.now()
        return df, label
    except Exception as e:
        st.session_state.comm_status = 'offline'
        st.session_state.comm_message = f'Reload failed: {e}'
        return None, None


def update_live_data_incremental():
    if st.session_state.live_data is None:
        return load_initial_data()

    if USE_LOCAL_DATA:
        latest_path = get_latest_local_data_path()
        last_row_df, err = read_last_local_row(latest_path)
        latest_file = os.path.basename(latest_path)
    else:
        last_row_df, err, latest_file = read_remote_last_row()

    if err or last_row_df is None or last_row_df.empty:
        st.session_state.comm_status = 'offline'
        st.session_state.comm_message = err or 'Unknown communication failure'
        append_connection_gap(st.session_state.comm_message)
        return st.session_state.live_data, st.session_state.live_filename

    try:
        current = st.session_state.live_data.copy()
        new_row = last_row_df.copy()
        if '_connection_gap' not in current.columns:
            current['_connection_gap'] = False
        if '_connection_gap' not in new_row.columns:
            new_row['_connection_gap'] = False
        if 'user_notes' not in current.columns:
            current['user_notes'] = ''
        if 'user_notes' not in new_row.columns:
            new_row['user_notes'] = ''

        df_coerced, ts_col = coerce_timestamp_column(pd.concat([current.tail(1), new_row], ignore_index=True))
        if df_coerced is None or df_coerced.empty:
            st.session_state.comm_status = 'offline'
            st.session_state.comm_message = 'Could not parse tail timestamp'
            append_connection_gap(st.session_state.comm_message)
            return current, st.session_state.live_filename

        new_ts = df_coerced[ts_col].iloc[-1]
        existing, ex_ts_col = coerce_timestamp_column(current)
        if existing is not None and not existing.empty:
            last_existing_ts = existing[ex_ts_col].iloc[-1]
            if new_ts <= last_existing_ts:
                st.session_state.comm_status = 'online'
                st.session_state.comm_message = 'No new row yet'
                st.session_state.last_comm_time = datetime.now()
                return current, st.session_state.live_filename

        updated = pd.concat([current, new_row], ignore_index=True)
        st.session_state.live_data = updated
        st.session_state.live_filename = latest_file
        if st.session_state.view_mode == 'latest':
            st.session_state.current_data = updated.copy()
            st.session_state.current_filename = latest_file
        st.session_state.last_update_time = datetime.now()
        st.session_state.comm_status = 'online'
        st.session_state.comm_message = 'Tail append ok'
        st.session_state.last_comm_time = datetime.now()
        return updated, latest_file
    except Exception as e:
        st.session_state.comm_status = 'offline'
        st.session_state.comm_message = f'Append failed: {e}'
        append_connection_gap(st.session_state.comm_message)
        return st.session_state.live_data, st.session_state.live_filename


def get_sensor_status(df, timestamp_col, sensor_groups, timeout=5):
    if df is None or df.empty:
        return {}

    latest_valid_time = df.loc[df['_connection_gap'] != True, timestamp_col].max() if '_connection_gap' in df.columns else df[timestamp_col].max()
    if pd.isna(latest_valid_time):
        latest_valid_time = df[timestamp_col].max()

    now = datetime.now()
    sensor_status = {}
    latest_row = df.iloc[-1]
    for sensor in sensor_groups.keys():
        sensor_key = sensor.lower().replace(' ', '')
        sensor_cols = [col for col in df.columns if sensor_key in col.lower().replace('_', '')]
        if not sensor_cols:
            sensor_status[sensor] = 'dead'
            continue
        has_data = any(pd.notna(latest_row.get(col)) for col in sensor_cols)
        time_diff = (now - latest_valid_time).total_seconds() if pd.notna(latest_valid_time) else 1e9
        sensor_status[sensor] = 'alive' if has_data and time_diff <= timeout else 'dead'
    return sensor_status


def windowed_df(df, timestamp_col, minutes):
    if df is None or df.empty:
        return df, None, None
    end_time = df[timestamp_col].max()
    start_time = end_time - timedelta(minutes=float(minutes))
    return df[df[timestamp_col] >= start_time].copy(), start_time, end_time


def get_gap_intervals(df, timestamp_col):
    if df is None or df.empty or '_connection_gap' not in df.columns:
        return []
    gap_times = df.loc[df['_connection_gap'] == True, timestamp_col].sort_values().tolist()
    return [(ts - timedelta(seconds=0.5), ts + timedelta(seconds=0.5)) for ts in gap_times]


def merge_notes_with_data(data):
    if st.session_state.notes.empty or st.session_state.timestamp_col is None:
        out = data.copy()
        if 'user_notes' not in out.columns:
            out['user_notes'] = ''
        return out

    out = data.copy()
    if 'user_notes' not in out.columns:
        out['user_notes'] = ''
    ts_col = st.session_state.timestamp_col
    out[ts_col] = pd.to_datetime(out[ts_col], errors='coerce', format='mixed')

    for _, note_row in st.session_state.notes.iterrows():
        note_time = pd.to_datetime(note_row['timestamp'], errors='coerce')
        note_text = str(note_row['note'])
        if pd.isna(note_time):
            continue
        time_diff = abs(out[ts_col] - note_time)
        if time_diff.notna().any():
            min_idx = time_diff.idxmin()
            if pd.notna(time_diff.loc[min_idx]) and time_diff.loc[min_idx] <= timedelta(seconds=1):
                existing = out.at[min_idx, 'user_notes']
                out.at[min_idx, 'user_notes'] = note_text if not existing else f'{existing}; {note_text}'
    return out


def ensure_threshold_entry(column_name):
    thresholds = st.session_state.thresholds
    if column_name not in thresholds:
        thresholds[column_name] = {'min': None, 'max': None}


def active_threshold_warnings(df):
    warnings = []
    if df is None or df.empty:
        return warnings
    last_row = df.iloc[-1]
    for col, lims in st.session_state.thresholds.items():
        if col not in df.columns:
            continue
        if not pd.notna(last_row[col]):
            continue
        try:
            value = float(last_row[col])
        except Exception:
            continue
        lo = lims.get('min')
        hi = lims.get('max')
        if lo is not None and value < lo:
            warnings.append(f'{col} below min: {value:.2f} < {lo}')
        if hi is not None and value > hi:
            warnings.append(f'{col} above max: {value:.2f} > {hi}')
    return warnings


def get_completed_label_ranges():
    labels = st.session_state.labels.copy()
    if labels.empty:
        return labels
    labels['start_time'] = pd.to_datetime(labels['start_time'], errors='coerce')
    labels['end_time'] = pd.to_datetime(labels['end_time'], errors='coerce')
    return labels.dropna(subset=['start_time'])


def build_main_figure(df, timestamp_col, selected_columns, y_limits, threshold_ranges, notes_df, x_start=None, x_end=None):
    fig = go.Figure()
    for col in selected_columns:
        if col not in df.columns:
            continue
        yvals = df[col].copy().replace(OFFLINE_SENTINEL, np.nan)
        fig.add_trace(go.Scatter(
            x=df[timestamp_col],
            y=yvals,
            mode='lines+markers',
            name=col,
        ))

    for start, end in get_gap_intervals(df, timestamp_col):
        fig.add_vrect(x0=start, x1=end, fillcolor='red', opacity=0.18, line_width=0)

    labels_df = get_completed_label_ranges()
    if not labels_df.empty:
        for _, row in labels_df.iterrows():
            start = row['start_time']
            end = row['end_time'] if pd.notna(row['end_time']) else x_end
            if pd.isna(start):
                continue
            if pd.isna(end):
                end = start
            fig.add_vrect(x0=start, x1=end, opacity=0.1, line_width=0, annotation_text=label_display_name(row.get('prefix', ''), row.get('name', '')), annotation_position='top left')

    notes_df = notes_df.copy()
    if not notes_df.empty:
        notes_df['timestamp'] = pd.to_datetime(notes_df['timestamp'], errors='coerce')
        notes_df = notes_df.dropna(subset=['timestamp'])
        if not notes_df.empty:
            for _, row in notes_df.iterrows():
                fig.add_vline(x=row['timestamp'], line_width=1, line_dash='dot', opacity=0.35)

    axis_min = None
    axis_max = None
    for col in selected_columns:
        lims = y_limits.get(col, {})
        lo = lims.get('min')
        hi = lims.get('max')
        if lo is not None:
            axis_min = lo if axis_min is None else min(axis_min, lo)
        if hi is not None:
            axis_max = hi if axis_max is None else max(axis_max, hi)

    if axis_min is not None or axis_max is not None:
        fig.update_yaxes(range=[axis_min, axis_max])

    for _, lims in threshold_ranges.items():
        lo = lims.get('min')
        hi = lims.get('max')
        if lo is not None:
            fig.add_hline(y=lo, line_dash='dot', opacity=0.25)
        if hi is not None:
            fig.add_hline(y=hi, line_dash='dot', opacity=0.25)

    fig.update_layout(
        height=480,
        margin=dict(l=20, r=20, t=10, b=20),
        hovermode='x unified',
        showlegend=True,
        uirevision='main',
    )
    if x_start is not None and x_end is not None and not df.empty:
        actual_min = df[timestamp_col].min()
        actual_max = df[timestamp_col].max()
        if pd.notna(actual_min) and pd.notna(actual_max):
            if actual_min <= x_start:
                fig.update_xaxes(range=[x_start, x_end])
            else:
                fig.update_xaxes(range=[actual_min, actual_max])
    fig.update_layout(transition_duration=0)
    return fig


load_saved_notes()
load_saved_labels()
load_saved_thresholds()
load_label_metadata()
process_label_alarms()

st.sidebar.checkbox('Auto-refresh', key='auto_refresh')
st.sidebar.number_input('Refresh interval (s)', min_value=0.5, max_value=10.0, step=0.5, key='refresh_interval')
st.sidebar.number_input('Alive timeout (s)', min_value=1, max_value=60, step=1, key='alive_timeout')
st.sidebar.number_input('Window size (minutes)', min_value=1, max_value=240, step=1, key='window_minutes')
st.sidebar.checkbox('Use local data', key='use_local_sidebar', value=USE_LOCAL_DATA, disabled=True)

uploaded_file = st.sidebar.file_uploader('Open data file', type=['csv'])
if uploaded_file is not None:
    try:
        uploaded_df = pd.read_csv(uploaded_file)
        set_view_to_uploaded(uploaded_df, uploaded_file.name)
        st.session_state.data_loaded = True
        st.sidebar.success(f'Viewing {uploaded_file.name}')
    except Exception as e:
        st.sidebar.error(f'Could not open uploaded file: {e}')

local_csvs = list_available_local_csvs()
if local_csvs:
    selected_local_csv = st.sidebar.selectbox('Open local CSV', options=[''] + local_csvs, format_func=lambda x: 'Choose local file...' if x == '' else os.path.basename(x))
    if selected_local_csv:
        if st.sidebar.button('View selected local file'):
            try:
                local_df, label = read_data_file(selected_local_csv)
                set_view_to_uploaded(local_df, label)
                st.session_state.data_loaded = True
                st.rerun()
            except Exception as e:
                st.sidebar.error(f'Could not open local file: {e}')

if st.sidebar.button('View latest', type='primary'):
    refresh_live_data_from_source()
    set_view_to_latest()
    st.rerun()

if st.session_state.current_data is not None:
    st.sidebar.metric('Rows', len(st.session_state.current_data))
    st.sidebar.metric('Columns', len(st.session_state.current_data.columns))
    st.sidebar.metric('Notes', len(st.session_state.notes))
    st.sidebar.metric('View', st.session_state.current_filename or '-')

if st.session_state.plot_columns:
    st.sidebar.selectbox('Warning variable', options=st.session_state.plot_columns, key='threshold_var')
    ensure_threshold_entry(st.session_state.threshold_var)
    current_thr = st.session_state.thresholds[st.session_state.threshold_var]
    thr_min = st.sidebar.text_input('Warn min', value='' if current_thr['min'] is None else str(current_thr['min']))
    thr_max = st.sidebar.text_input('Warn max', value='' if current_thr['max'] is None else str(current_thr['max']))
    st.session_state.thresholds[st.session_state.threshold_var]['min'] = float(thr_min) if thr_min.strip() else None
    st.session_state.thresholds[st.session_state.threshold_var]['max'] = float(thr_max) if thr_max.strip() else None
    save_thresholds()

if not st.session_state.data_loaded:
    refresh_live_data_from_source()
    set_view_to_latest()

status_color = '🟢' if st.session_state.comm_status == 'online' else '🔴'
comm_text = f"{status_color} {st.session_state.comm_status.upper()}"
if st.session_state.comm_message:
    comm_text += f" — {st.session_state.comm_message}"
st.markdown(comm_text)
st.caption(f"Viewing: {st.session_state.current_filename or '-'} | Live source: {st.session_state.live_filename or 'not loaded'}")

for alarm_msg in st.session_state.pending_alarm_messages:
    st.error(alarm_msg)
if st.session_state.pending_alarm_messages:
    st.components.v1.html(
        """
        <script>
        try {
            const ctx = new (window.AudioContext || window.webkitAudioContext)();
            const o = ctx.createOscillator();
            const g = ctx.createGain();
            o.type = 'sine';
            o.frequency.value = 880;
            o.connect(g); g.connect(ctx.destination);
            g.gain.setValueAtTime(0.001, ctx.currentTime);
            g.gain.exponentialRampToValueAtTime(0.2, ctx.currentTime + 0.02);
            g.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.4);
            o.start(); o.stop(ctx.currentTime + 0.42);
        } catch (e) {}
        </script>
        """,
        height=0,
    )
    st.session_state.pending_alarm_messages = []

main_tab, notes_tab, data_tab = st.tabs(['Main', 'Notes', 'Data'])

if st.session_state.data_loaded and st.session_state.current_data is not None:
    merged = merge_notes_with_data(st.session_state.current_data)
    df_plot, timestamp_col, plot_columns, sensor_groups, sensor_param_map = prepare_data(merged)
else:
    df_plot = timestamp_col = plot_columns = sensor_groups = sensor_param_map = None

if df_plot is not None and not df_plot.empty:
    df_window, x_start, x_end = windowed_df(df_plot, timestamp_col, st.session_state.window_minutes)
    sensor_status = get_sensor_status(df_window, timestamp_col, sensor_groups, st.session_state.alive_timeout)
    warnings = active_threshold_warnings(df_window)
else:
    df_window = None
    x_start = x_end = None
    sensor_status = {}
    warnings = []

with main_tab:
    if warnings:
        for warning in warnings:
            st.warning(warning)

    st.subheader('Labels')

    label_col1, label_col2, label_col3 = st.columns(3)

    with label_col1:
        st.selectbox('Prefix', options=st.session_state.label_prefixes, key='label_prefix_choice')
        new_prefix = st.text_input('New prefix', key='new_prefix_input')
        if st.button('Save prefix', use_container_width=True):
            if add_custom_prefix(new_prefix):
                st.success('Prefix saved')
                st.rerun()
            else:
                st.info('Prefix not added')

    with label_col2:
        preset_options = ['Custom']
        for p in st.session_state.label_presets:
            duration_text = 'untimed' if p['minutes'] is None else f"{p['minutes']} min"
            preset_options.append(f"{p['name']} ({duration_text})")
        st.selectbox('Preset', options=preset_options, key='label_preset_choice', on_change=apply_label_preset)
        st.text_input('Label name', key='label_draft_name')
        if st.button('Save preset', use_container_width=True):
            if add_label_preset(st.session_state.label_draft_name, st.session_state.label_draft_minutes):
                st.success('Preset saved')
                st.rerun()
            else:
                st.info('Preset not added')

    with label_col3:
        st.text_input('Time (minutes)', key='label_draft_minutes', placeholder='optional')
        st.write('')
        if st.button('Start label', use_container_width=True):
            ok, msg = start_label(st.session_state.label_prefix_choice, st.session_state.label_draft_name, st.session_state.label_draft_minutes)
            (st.success if ok else st.error)(msg)
            if ok:
                st.rerun()

    active_labels = st.session_state.labels[st.session_state.labels['is_active'] == True].copy()
    if not active_labels.empty:
        st.markdown('**Active labels**')
        for idx, row in active_labels.iterrows():
            end_text = 'untimed' if pd.isna(row['planned_end_time']) else f"ends {pd.to_datetime(row['planned_end_time']).strftime('%Y-%m-%d %H:%M:%S')}"
            c1, c2 = st.columns([4, 1])
            with c1:
                st.info(f"{label_display_name(row['prefix'], row['name'])} — started {pd.to_datetime(row['start_time']).strftime('%Y-%m-%d %H:%M:%S')} — {end_text}")
            with c2:
                if st.button('End now', key=f'end_label_{idx}', use_container_width=True):
                    end_label(idx, early=True)
                    st.rerun()

    st.subheader('Notes')
    note_col1, note_col2 = st.columns([5, 1])
    with note_col1:
        note_value = st.text_input('Note', key='main_note_input', label_visibility='collapsed', placeholder='Add note')
    with note_col2:
        if st.button('Save note', key='save_main_note', use_container_width=True):
            if add_note(note_value):
                st.rerun()

    sensors = list(sensor_status.items())
    for row_start in range(0, len(sensors), 4):
        row_items = sensors[row_start: row_start + 4]
        cols = st.columns(4)
        for i, (sensor, state) in enumerate(row_items):
            with cols[i]:
                emoji = '🟢' if state == 'alive' else '🔴'
                st.markdown(f'{emoji} {sensor}')
                btn_emoji = '⏹️' if state == 'alive' else '▶️'
                st.button(btn_emoji, key=f'toggle_{sensor}', help=f'Toggle {sensor}')

    if df_window is not None:
        available_plot_options = [f'{sensor} - {param}' for sensor, params in sensor_groups.items() for param in params]
        reverse_map = {k: v for k, v in sensor_param_map.items()}
        default_displays = []
        for col in st.session_state.selected_main_plots:
            for disp, actual in reverse_map.items():
                if actual == col:
                    default_displays.append(disp)
                    break
        selected_display = st.multiselect(
            'Variables',
            options=available_plot_options,
            default=default_displays[:MAX_MAIN_PLOTS],
            max_selections=MAX_MAIN_PLOTS,
            key='main_plot_selection'
        )
        selected_columns = [sensor_param_map[d] for d in selected_display if d in sensor_param_map]
        st.session_state.selected_main_plots = selected_columns

        numeric_selected = infer_numeric_columns(df_window, selected_columns)
        if numeric_selected:
            metric_cols = st.columns(min(len(numeric_selected), MAX_MAIN_PLOTS))
            for i, col in enumerate(numeric_selected[:MAX_MAIN_PLOTS]):
                series = df_window[col].replace(OFFLINE_SENTINEL, np.nan).dropna()
                if series.empty:
                    continue
                with metric_cols[i]:
                    st.metric(col, f'{series.iloc[-1]:.2f}', help=f'avg {series.mean():.2f} | max {series.max():.2f}')
                    st.caption(f'avg {series.mean():.2f} | max {series.max():.2f}')

        plot_limits = st.session_state.plot_limits
        for col in selected_columns:
            plot_limits.setdefault(col, {'min': None, 'max': None})

        fig = build_main_figure(df_window, timestamp_col, selected_columns, plot_limits, st.session_state.thresholds, st.session_state.notes, x_start=x_start, x_end=x_end)
        st.plotly_chart(fig, width='stretch', key='main_chart')

        if selected_columns:
            rows = []
            for col in selected_columns:
                rows.append({
                    'variable': col,
                    'display_min': plot_limits[col]['min'],
                    'display_max': plot_limits[col]['max'],
                })
            limits_df = pd.DataFrame(rows)
            with st.form('plot_limits_form', clear_on_submit=False):
                edited_limits = st.data_editor(
                    limits_df,
                    width='stretch',
                    hide_index=True,
                    disabled=['variable'],
                    key='limits_editor'
                )
                limits_submitted = st.form_submit_button('Apply display min/max')
            if limits_submitted:
                for _, row in edited_limits.iterrows():
                    st.session_state.plot_limits[row['variable']] = {
                        'min': None if pd.isna(row['display_min']) else float(row['display_min']),
                        'max': None if pd.isna(row['display_max']) else float(row['display_max']),
                    }
                st.rerun()

with notes_tab:
    notes_df = st.session_state.notes.copy()
    if notes_df.empty:
        st.info('No notes yet')
    else:
        notes_df['delete'] = False
        edited = st.data_editor(
            notes_df,
            width='stretch',
            hide_index=True,
            num_rows='dynamic',
            key='notes_editor'
        )
        col_a, col_b = st.columns(2)
        with col_a:
            if st.button('Save note edits'):
                edited = edited.copy()
                if 'delete' in edited.columns:
                    edited = edited[edited['delete'] != True].copy()
                    edited = edited.drop(columns=['delete'])
                edited['timestamp'] = pd.to_datetime(edited['timestamp'], errors='coerce')
                edited = edited.dropna(subset=['timestamp'])
                st.session_state.notes = edited[['timestamp', 'note']].reset_index(drop=True)
                save_notes()
                st.rerun()
        with col_b:
            if st.button('Delete marked notes'):
                edited = edited.copy()
                if 'delete' in edited.columns:
                    edited = edited[edited['delete'] != True].copy()
                    edited = edited.drop(columns=['delete'])
                st.session_state.notes = edited[['timestamp', 'note']].reset_index(drop=True)
                save_notes()
                st.rerun()

    st.markdown('---')
    st.subheader('Labels history')
    labels_df = st.session_state.labels.copy()
    if labels_df.empty:
        st.info('No labels yet')
    else:
        labels_df['label'] = labels_df.apply(lambda row: label_display_name(row.get('prefix', ''), row.get('name', '')), axis=1)
        show_cols = ['label', 'start_time', 'end_time', 'planned_end_time', 'is_active']
        st.dataframe(labels_df[show_cols].sort_values('start_time', ascending=False), width='stretch', hide_index=True)

with data_tab:
    if df_window is None:
        st.info('Load data to view table')
    else:
        st.dataframe(df_window.tail(200), width='stretch')

if st.session_state.auto_refresh:
    time.sleep(float(st.session_state.refresh_interval))
    refresh_live_data_from_source()
    if st.session_state.view_mode == 'latest':
        set_view_to_latest()
    st.rerun()
