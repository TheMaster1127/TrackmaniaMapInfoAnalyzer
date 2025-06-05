import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import requests
import sqlite3
import json
from datetime import datetime
import os
import re
import time
import math

DATABASE_NAME = 'trackmania_stats.db'
MAP_URLS_FILE = 'maps_api_urls.txt'

# --- Database Setup ---
def init_db():
    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS maps (
        map_uid TEXT PRIMARY KEY, api_url TEXT UNIQUE, map_display_name TEXT,
        last_playercount INTEGER, last_fetched_at TEXT, wr_player_id TEXT,
        wr_time_ms INTEGER, wr_game_timestamp TEXT, wr_script_recorded_at TEXT,
        is_new_wr_since_last_fetch INTEGER DEFAULT 0, fetch_order INTEGER,
        FOREIGN KEY (wr_player_id) REFERENCES players (player_id)
    )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS players (
        player_id TEXT PRIMARY KEY, last_known_name TEXT, country_name TEXT,
        country_flag TEXT, first_seen_by_script_at TEXT
    )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS records (
        map_uid TEXT, player_id TEXT, time_ms INTEGER, score INTEGER,
        position INTEGER, game_timestamp TEXT, script_recorded_at TEXT, script_updated_at TEXT,
        is_pb_since_last_fetch INTEGER DEFAULT 0, is_new_player_on_map_since_last_fetch INTEGER DEFAULT 0,
        PRIMARY KEY (map_uid, player_id),
        FOREIGN KEY (map_uid) REFERENCES maps (map_uid),
        FOREIGN KEY (player_id) REFERENCES players (player_id)
    )
    ''')
    conn.commit()
    conn.close()


def extract_map_uid_from_url(api_url):
    match = re.search(r'/map/([^?]+)', api_url)
    return match.group(1) if match else None

def get_actual_country_info(zone_data):
    current_zone = zone_data
    country_name = current_zone.get('name', 'Unknown')
    country_flag = current_zone.get('flag', 'WOR')
    path = []
    temp_zone = zone_data
    while temp_zone:
        path.insert(0, temp_zone)
        temp_zone = temp_zone.get('parent')
    if len(path) > 1:
        for i in range(len(path) - 1, -1, -1):
            zone = path[i]
            parent_zone = zone.get('parent')
            if parent_zone:
                grandparent_zone = parent_zone.get('parent')
                if (grandparent_zone and grandparent_zone.get('name') == "World") or \
                   (parent_zone.get('name') == "World" and zone.get('name') != "World"):
                    country_name = zone.get('name', country_name)
                    country_flag = zone.get('flag', country_flag)
                    break
            elif zone.get('name') != "World": # Case where the zone itself is a country direct child of nothing (or World removed)
                country_name = zone.get('name', country_name)
                country_flag = zone.get('flag', country_flag)
                break
    elif path and path[0].get('name') != "World": # Single zone, not "World"
        country_name = path[0].get('name', country_name)
        country_flag = path[0].get('flag', country_flag)
    return country_name, country_flag

def format_time_ms(ms, show_millis=True, show_hours_minutes_optional=True):
    if ms is None: return "N/A"
    total_seconds_float = ms / 1000.0
    milliseconds_part = 0
    if show_millis:
        milliseconds_part = int(round((total_seconds_float - math.floor(total_seconds_float)) * 1000))
    seconds_int = int(math.floor(total_seconds_float))
    hours = seconds_int // 3600
    minutes = (seconds_int % 3600) // 60
    seconds = seconds_int % 60
    time_str = ""
    if hours > 0:
        time_str += f"{hours:d}:"
        time_str += f"{minutes:02d}:{seconds:02d}"
    elif minutes > 0:
        if not show_hours_minutes_optional and hours == 0:
             time_str += f"00:{minutes:02d}:{seconds:02d}"
        else:
            time_str += f"{minutes:d}:{seconds:02d}"
    else:
        if not show_hours_minutes_optional and minutes == 0: # Ensure 00:00:SS format if minutes are 0 and optionality is off
             time_str += f"00:00:{seconds:02d}"
        else: # Otherwise, just SS
             time_str += f"{seconds:02d}"
    if show_millis: time_str += f".{milliseconds_part:03d}"
    return time_str

def calculate_points_for_rank(rank):
    if not rank or rank <= 0: return 0
    tier = math.ceil(math.log10(rank))
    points = 0
    if tier < 1: tier = 1 # Should not happen if rank > 0
    if tier < 2: # Rank 1-9
        points = 40000 / rank
    else: # Rank 10+
        base_points = 4000 / (2**(tier - 1))
        rank_multiplier = (10**(tier - 1)) / rank + 0.9
        points = base_points * rank_multiplier
    return round(points, 2)

# --- API and Data Processing ---
def fetch_and_process_data(log_callback):
    if not os.path.exists(MAP_URLS_FILE):
        log_callback(f"Error: {MAP_URLS_FILE} not found.")
        messagebox.showerror("Error", f"{MAP_URLS_FILE} not found...")
        return {'error': True}

    map_url_entries = []
    with open(MAP_URLS_FILE, 'r') as f:
        for line_num, line in enumerate(f):
            line = line.strip()
            if not line or line.startswith('#'): continue
            parts = line.split('|', 1)
            if len(parts) == 2:
                map_url_entries.append({'url_template': parts[0].strip(), 'name': parts[1].strip(), 'fetch_order': line_num})
            else:
                log_callback(f"Warning: Skipping malformed line {line_num+1} in {MAP_URLS_FILE}: '{line}'")
    if not map_url_entries:
        log_callback(f"No valid map entries found in {MAP_URLS_FILE}.")
        return {'error': True}

    conn = sqlite3.connect(DATABASE_NAME)
    cursor = conn.cursor()
    cursor.execute("UPDATE records SET is_pb_since_last_fetch = 0, is_new_player_on_map_since_last_fetch = 0")
    cursor.execute("UPDATE maps SET is_new_wr_since_last_fetch = 0")
    conn.commit()

    current_fetch_time = datetime.now().isoformat()
    session_data = {
        'newly_added_players': set(), 'new_pbs': [], 'new_players_on_map': [],
        'new_wrs': [], 'player_name_changes': []
    }
    headers = {'User-Agent': 'TrackmaniaMapInfoAnalyzer/1.0 (+https://github.com/TheMaster1127/TrackmaniaMapInfoAnalyzer)'} 

    RECORDS_PER_PAGE = 100

    for entry_data in map_url_entries:
        base_api_url_template = entry_data['url_template']
        map_display_name = entry_data['name']
        fetch_order = entry_data['fetch_order']

        base_api_url = base_api_url_template
        base_api_url = re.sub(r'[?&]offset=\d+', '', base_api_url)
        base_api_url = re.sub(r'[?&]length=\d+', '', base_api_url)
        if base_api_url.endswith('?'):
            base_api_url = base_api_url[:-1]
        if base_api_url.endswith('&'):
            base_api_url = base_api_url[:-1]


        map_uid = extract_map_uid_from_url(base_api_url_template)
        if not map_uid:
            log_callback(f"Could not extract Map UID from URL template: {base_api_url_template} (for map '{map_display_name}')")
            continue

        log_callback(f"Fetching data for map: '{map_display_name}' (UID: {map_uid})...")

        all_tops_for_map = []
        current_offset = 0
        map_total_playercount_api = 0
        first_fetch_done = False

        while True:
            paginated_api_url = base_api_url
            param_char = '&' if '?' in paginated_api_url else '?'
            paginated_api_url += f"{param_char}offset={current_offset}&length={RECORDS_PER_PAGE}"
            # If the first param added was offset (so paginated_api_url had no '?' initially),
            # and now we add length, it needs to be an '&'
            if param_char == '?' and 'length' in paginated_api_url : # Check if length got added with '?' and now needs '&'
                 paginated_api_url = paginated_api_url.replace(f"?length={RECORDS_PER_PAGE}",f"&length={RECORDS_PER_PAGE}",1)


            log_callback(f"  Fetching page: {paginated_api_url}")
            try:
                response = requests.get(paginated_api_url, headers=headers, timeout=30)
                response.raise_for_status()
                api_response_data = response.json()
            except requests.exceptions.RequestException as e:
                log_callback(f"  Error fetching page for '{map_display_name}': {e}")
                break
            except json.JSONDecodeError as e:
                log_callback(f"  Error decoding JSON for page of '{map_display_name}': {e}")
                break

            current_page_tops = api_response_data.get('tops')

            if not first_fetch_done:
                map_total_playercount_api = api_response_data.get('playercount', 0)
                cursor.execute('''
                INSERT OR IGNORE INTO maps (map_uid, api_url, map_display_name, fetch_order, last_fetched_at, last_playercount)
                VALUES (?, ?, ?, ?, ?, ?)
                ''', (map_uid, base_api_url_template, map_display_name, fetch_order, current_fetch_time, map_total_playercount_api ))
                cursor.execute('''
                UPDATE maps SET map_display_name = ?, last_playercount = ?, last_fetched_at = ?,
                               api_url = ?, fetch_order = ?
                WHERE map_uid = ?
                ''', (map_display_name, map_total_playercount_api, current_fetch_time,
                      base_api_url_template, fetch_order, map_uid))
                first_fetch_done = True

            if current_page_tops:
                all_tops_for_map.extend(current_page_tops)
                if len(current_page_tops) < RECORDS_PER_PAGE:
                    log_callback(f"  Got {len(current_page_tops)} records, less than page size. Assuming end of leaderboard for '{map_display_name}'.")
                    break
                current_offset += RECORDS_PER_PAGE
                if current_offset >= map_total_playercount_api and map_total_playercount_api > 0 :
                    log_callback(f"  Fetched {current_offset} records, matching/exceeding reported playercount {map_total_playercount_api}. Assuming end.")
                    break
                if current_offset >= 10000 and map_total_playercount_api == 0: # Safety break if playercount is 0 but API keeps sending data
                    log_callback(f"  Warning: Playercount is 0 but still fetching. Safety break after 10k records for '{map_display_name}'.")
                    break # Should not happen with correct API behavior
            else:
                log_callback(f"  No more 'tops' data in response for '{map_display_name}'. End of leaderboard.")
                break
            time.sleep(1.5) # Rate limit

        actual_fetched_count = len(all_tops_for_map)
        if actual_fetched_count != map_total_playercount_api :
            log_callback(f"  Note: API reported {map_total_playercount_api} players, but {actual_fetched_count} records were processed for '{map_display_name}'. Updating playercount.")
            # Only update if actual_fetched_count is sensible (e.g., not 0 when API said 1000 but failed)
            # For now, we'll trust the actual fetched count more if it's different.
            cursor.execute("UPDATE maps SET last_playercount = ? WHERE map_uid = ?", (actual_fetched_count, map_uid))

        api_wr_player_id, api_wr_time_ms, api_wr_game_timestamp, api_wr_player_name = None, None, None, "N/A"
        if all_tops_for_map:
            wr_entry_data = all_tops_for_map[0]
            wr_player_data_json = wr_entry_data.get('player', {})
            api_wr_player_id = wr_player_data_json.get('id')
            api_wr_time_ms = wr_entry_data.get('time')
            api_wr_game_timestamp = wr_entry_data.get('timestamp')
            api_wr_player_name = wr_player_data_json.get('name', 'UnknownWRPlayer')
            if api_wr_player_id:
                wr_country_name, wr_country_flag = get_actual_country_info(wr_player_data_json.get('zone', {}))
                cursor.execute("SELECT last_known_name FROM players WHERE player_id = ?", (api_wr_player_id,))
                db_player_row_wr = cursor.fetchone()
                if not db_player_row_wr:
                    cursor.execute("INSERT INTO players VALUES (?, ?, ?, ?, ?)",
                                   (api_wr_player_id, api_wr_player_name, wr_country_name, wr_country_flag, current_fetch_time))
                    session_data['newly_added_players'].add(api_wr_player_name)
                elif db_player_row_wr[0] != api_wr_player_name:
                    if not any(change[0] == api_wr_player_id and change[2] == api_wr_player_name for change in session_data['player_name_changes']):
                        session_data['player_name_changes'].append((api_wr_player_id, db_player_row_wr[0], api_wr_player_name))
                    cursor.execute("UPDATE players SET last_known_name=?, country_name=?, country_flag=? WHERE player_id=?",
                                   (api_wr_player_name, wr_country_name, wr_country_flag, api_wr_player_id))
                else: # Name is same, update country info if needed
                    cursor.execute("UPDATE players SET country_name=?, country_flag=? WHERE player_id=?",
                                   (wr_country_name, wr_country_flag, api_wr_player_id))

        cursor.execute("SELECT m.wr_player_id, m.wr_time_ms, p.last_known_name FROM maps m LEFT JOIN players p ON m.wr_player_id = p.player_id WHERE m.map_uid = ?", (map_uid,))
        db_wr_row_map = cursor.fetchone()
        db_wr_player_id, db_wr_time_ms_map, db_wr_player_name_map = (db_wr_row_map[0], db_wr_row_map[1], db_wr_row_map[2] if db_wr_row_map and db_wr_row_map[2] else "N/A") if db_wr_row_map else (None, None, "N/A")

        wr_changed_this_fetch = False # Initialize this flag for each map
        if api_wr_player_id and api_wr_time_ms is not None:
            if db_wr_player_id != api_wr_player_id or db_wr_time_ms_map != api_wr_time_ms:
                session_data['new_wrs'].append((map_display_name, api_wr_player_name, api_wr_time_ms, db_wr_player_name_map, db_wr_time_ms_map))
                cursor.execute("UPDATE maps SET wr_player_id=?, wr_time_ms=?, wr_game_timestamp=?, wr_script_recorded_at=?, is_new_wr_since_last_fetch=1 WHERE map_uid=?",
                               (api_wr_player_id, api_wr_time_ms, api_wr_game_timestamp, current_fetch_time, map_uid))
                wr_changed_this_fetch = True
        elif db_wr_player_id is not None: # DB had WR, API now shows no WR
            session_data['new_wrs'].append((map_display_name, "None (Empty Leaderboard or WR Deleted)", None, db_wr_player_name_map, db_wr_time_ms_map))
            cursor.execute("UPDATE maps SET wr_player_id=NULL, wr_time_ms=NULL, wr_game_timestamp=NULL, wr_script_recorded_at=?, is_new_wr_since_last_fetch=1 WHERE map_uid=?",
                               (current_fetch_time, map_uid))
            wr_changed_this_fetch = True
        
        # If wr_changed_this_fetch is False, is_new_wr_since_last_fetch remains 0 (due to initial reset)

        for rank, record_entry in enumerate(all_tops_for_map, 1):
            player_data_json = record_entry.get('player', {})
            player_id = player_data_json.get('id')
            current_player_name_api = player_data_json.get('name', 'Unknown')
            if not player_id: continue # Should not happen with valid API data
            country_name, country_flag = get_actual_country_info(player_data_json.get('zone', {}))
            cursor.execute("SELECT last_known_name FROM players WHERE player_id = ?", (player_id,))
            db_player_row_rec = cursor.fetchone()
            if not db_player_row_rec:
                cursor.execute("INSERT INTO players VALUES (?, ?, ?, ?, ?)",
                               (player_id, current_player_name_api, country_name, country_flag, current_fetch_time))
                session_data['newly_added_players'].add(current_player_name_api)
            else:
                db_last_known_name = db_player_row_rec[0]
                if db_last_known_name != current_player_name_api:
                    if not any(change[0] == player_id and change[2] == current_player_name_api for change in session_data['player_name_changes']): # Avoid duplicate name change logs per session
                        session_data['player_name_changes'].append((player_id, db_last_known_name, current_player_name_api))
                # Always update name and country info, as it might change (name change, or better zone data)
                cursor.execute("UPDATE players SET last_known_name=?, country_name=?, country_flag=? WHERE player_id=?",
                               (current_player_name_api, country_name, country_flag, player_id))

            time_ms_val = record_entry.get('time')
            score_val = record_entry.get('score') # Typically 0 for time attack, useful for other modes
            game_timestamp = record_entry.get('timestamp')
            cursor.execute("SELECT time_ms, position FROM records WHERE map_uid=? AND player_id=?", (map_uid, player_id))
            existing_record_tuple = cursor.fetchone()
            if existing_record_tuple is None: # New player on this map
                cursor.execute("INSERT INTO records VALUES (?,?,?,?,?,?,?,?,0,1)", # is_pb=0, is_new_player=1
                               (map_uid,player_id,time_ms_val,score_val,rank,game_timestamp,current_fetch_time,current_fetch_time))
                session_data['new_players_on_map'].append((current_player_name_api, map_display_name, time_ms_val))
            else: # Existing player on this map
                existing_time_ms, existing_pos = existing_record_tuple
                is_pb = False
                if time_ms_val is not None and (existing_time_ms is None or time_ms_val < existing_time_ms): is_pb = True
                # Also consider PB if time is same but rank improved (e.g. someone above got deleted)
                elif time_ms_val is not None and existing_time_ms is not None and time_ms_val == existing_time_ms and rank < existing_pos : is_pb = True

                if is_pb:
                    cursor.execute("UPDATE records SET time_ms=?,score=?,position=?,game_timestamp=?,script_updated_at=?,is_pb_since_last_fetch=1 WHERE map_uid=? AND player_id=?",
                                   (time_ms_val,score_val,rank,game_timestamp,current_fetch_time,map_uid,player_id))
                    session_data['new_pbs'].append((current_player_name_api, map_display_name, time_ms_val, existing_time_ms))
                else: # Not a PB, but rank might have changed or just update timestamp
                    if rank != existing_pos: # Rank changed without time improving (e.g. others improved/got deleted)
                        cursor.execute("UPDATE records SET position=?,script_updated_at=? WHERE map_uid=? AND player_id=?",(rank,current_fetch_time,map_uid,player_id))
                    else: # Nothing changed except our fetch time
                        cursor.execute("UPDATE records SET script_updated_at=? WHERE map_uid=? AND player_id=?",(current_fetch_time,map_uid,player_id))
        conn.commit()
        log_callback(f"Finished processing map: '{map_display_name}'. Fetched {actual_fetched_count} records.")
        time.sleep(1.5)

    conn.close()
    log_callback("Data fetching and processing complete.")

    summary_message = "Fetch Complete!\n"
    if session_data['newly_added_players']:
        summary_message += f"\nNewly discovered players: {len(session_data['newly_added_players'])}\n  "
        summary_message += ", ".join(list(session_data['newly_added_players'])[:3]) + ("..." if len(session_data['newly_added_players']) > 3 else "") + "\n"
    if session_data['new_wrs']:
        summary_message += f"\nWR Changes: {len(session_data['new_wrs'])}\n"
        for m,nn,nt,on,ot in session_data['new_wrs'][:2]: summary_message += f"  '{m[:15]}..': {nn}({format_time_ms(nt)}) (Old:{on}({format_time_ms(ot)}))\n"
        if len(session_data['new_wrs']) > 2: summary_message += "  ...\n"
    if session_data['new_pbs']:
        summary_message += f"\nNew PBs: {len(session_data['new_pbs'])}\n"
        for p,m,nt,ot in session_data['new_pbs'][:2]: summary_message += f"  {p} on '{m[:15]}..': {format_time_ms(nt)} (was {format_time_ms(ot)})\n"
        if len(session_data['new_pbs']) > 2: summary_message += "  ...\n"
    if session_data['new_players_on_map']:
        summary_message += f"\nNew on Map: {len(session_data['new_players_on_map'])}\n"
        for p,m,t in session_data['new_players_on_map'][:2]: summary_message += f"  {p} on '{m[:15]}..': {format_time_ms(t)}\n"
        if len(session_data['new_players_on_map']) > 2: summary_message += "  ...\n"
    if session_data['player_name_changes']:
        summary_message += f"\nName Changes: {len(session_data['player_name_changes'])}\n"
        for pid,on,nn in session_data['player_name_changes'][:2]: summary_message += f"  '{on}' is now '{nn}'\n"
        if len(session_data['player_name_changes']) > 2: summary_message += "  ...\n"

    no_changes_detected = True
    for key in session_data:
        if key != 'error' and isinstance(session_data[key], (list, set)) and session_data[key]:
            no_changes_detected = False
            break
    if no_changes_detected:
        summary_message += "\nNo major changes detected in this fetch."
    messagebox.showinfo("Fetch Complete", summary_message)
    return session_data

# --- GUI Class TrackmaniaAnalyzerApp ---
class TrackmaniaAnalyzerApp:
    def __init__(self, root_window):
        self.root = root_window
        self.root.title("Trackmania Map Analyzer")
        try: self.root.tk_setPalette(background='#2E2E2E', foreground='white', activeBackground='#4E4E4E', activeForeground='white')
        except tk.TclError: print("Dark mode palette not fully supported.")
        self.root.geometry("1500x800") # Slightly increased size for new tab

        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Treeview", background="#2E2E2E", foreground="white", fieldbackground="#2E2E2E", rowheight=25, font=('Calibri', 9))
        style.map('Treeview', background=[('selected', '#0078D7')])
        style.configure("Treeview.Heading", background="#1E1E1E", foreground="white", relief="flat", font=('Calibri', 10, 'bold'))
        style.map("Treeview.Heading", background=[('active', '#3E3E3E')])
        style.configure("TNotebook.Tab", padding=[10, 5], font=('Calibri', 10, 'bold'))
        style.map("TNotebook.Tab", background=[("selected", "#0078D7"), ("active", "#4E4E4E")], foreground=[("selected", "white"), ("active", "white")])
        style.configure("TNotebook", background="#1E1E1E", borderwidth=0)
        style.configure("TLabelframe", background="#2E2E2E", foreground="white", bordercolor="#555555")
        style.configure("TLabelframe.Label", background="#2E2E2E", foreground="white", font=('Calibri', 10, 'bold'))


        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(expand=True, fill=tk.BOTH)

        top_control_frame = ttk.Frame(main_frame)
        top_control_frame.pack(fill=tk.X, pady=5)

        self.fetch_button = ttk.Button(top_control_frame, text="Fetch/Refresh All Data", command=self.run_fetch_and_refresh_gui)
        self.fetch_button.pack(side=tk.LEFT, padx=5)

        self.log_text = scrolledtext.ScrolledText(main_frame, height=8, wrap=tk.WORD, bg="#1E1E1E", fg="lightgrey", font=("Consolas", 9))
        self.log_text.pack(fill=tk.X, pady=5)
        self.log_to_gui("Application started. Initialize DB if needed.")
        self.log_to_gui(f"Reading map list from: {os.path.abspath(MAP_URLS_FILE)}")

        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(expand=True, fill=tk.BOTH, pady=5)
        self.tabs = {}

        # For Player Profile Tab
        self.player_profile_search_var = tk.StringVar()
        self.overall_lb_sorted_players_with_rank = [] # To store player data with global rank

        # StringVars for selected player details
        self.selected_player_name_val = tk.StringVar(value="N/A")
        self.selected_player_country_val = tk.StringVar(value="N/A")
        self.selected_player_score_val = tk.StringVar(value="N/A")
        self.selected_player_global_rank_val = tk.StringVar(value="N/A")
        self.selected_player_maps_played_val = tk.StringVar(value="N/A")
        self.selected_player_first_seen_val = tk.StringVar(value="N/A")


        self.create_tabs()

        for tv_name, tv_widget in self.tabs.items():
            if hasattr(tv_widget, 'tag_configure'): # Check if it's a Treeview
                tv_widget.tag_configure('new_wr_highlight', background='#006400', foreground='white') # Dark Green
                tv_widget.tag_configure('pb_highlight', background='#0050A2', foreground='white') # Darker Blue
                tv_widget.tag_configure('new_player_highlight', background='#B8860B', foreground='white') # Dark Goldenrod
                tv_widget.tag_configure('evenrow', background='#2E2E2E')
                tv_widget.tag_configure('oddrow', background='#383838') # Slightly lighter than main bg
        # Apply to Player Profile Treeviews too
        if hasattr(self, 'player_search_results_tv'): # Check if these exist
            for tag_name, bg, fg in [('evenrow', '#2E2E2E', 'white'), ('oddrow', '#383838', 'white')]:
                 self.player_search_results_tv.tag_configure(tag_name, background=bg, foreground=fg)
                 self.selected_player_records_tv.tag_configure(tag_name, background=bg, foreground=fg)


        self.session_changes_for_gui = {} # Store session changes to highlight in GUI
        self.refresh_all_tabs()

    def log_to_gui(self, message):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.log_text.insert(tk.END, f"[{now}] {message}\n")
        self.log_text.see(tk.END)
        print(f"LOG: {message}") # Also print to console for debugging

    def run_fetch_and_refresh_gui(self):
        self.log_to_gui("Starting data fetch...")
        self.fetch_button.config(state=tk.DISABLED)
        session_data_from_fetch = fetch_and_process_data(self.log_to_gui)
        self.session_changes_for_gui = session_data_from_fetch # Store for highlighting PBs etc.
        if not session_data_from_fetch.get('error'):
            self.log_to_gui("Data fetch successful. Refreshing GUI.")
            self.refresh_all_tabs()
        else:
            self.log_to_gui("Data fetch encountered issues or was aborted.")
        self.fetch_button.config(state=tk.NORMAL)

    def create_treeview_in_tab(self, tab_frame, columns_config_list_with_hash, height=None):
        tv_frame = ttk.Frame(tab_frame) # Create a frame to hold treeview and scrollbars
        tv_frame.pack(expand=True, fill=tk.BOTH)

        # Columns_config_list_with_hash already includes '#' as first item
        actual_columns_config = [("#", "#", 40)] + columns_config_list_with_hash # Prepend # for display index
        column_identifiers = [config_tuple[0] for config_tuple in actual_columns_config]

        tv_params = {'columns': column_identifiers, 'show': 'headings'}
        if height: # For treeviews with a fixed number of rows visible
            tv_params['height'] = height
        tv = ttk.Treeview(tv_frame, **tv_params)


        for col_id, col_text, col_width in actual_columns_config:
            anchor_val = tk.CENTER if col_id == "#" else tk.W
            tv.heading(col_id, text=col_text, command=lambda _col=col_id, _tv=tv: self.treeview_sort_column(_tv, _col, False))
            tv.column(col_id, width=col_width, anchor=anchor_val, minwidth=max(30, col_width//2))

        vsb = ttk.Scrollbar(tv_frame, orient="vertical", command=tv.yview)
        hsb = ttk.Scrollbar(tv_frame, orient="horizontal", command=tv.xview)
        tv.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        tv.pack(side=tk.LEFT, expand=True, fill=tk.BOTH) # Treeview fills the rest of tv_frame

        return tv

    def treeview_sort_column(self, tv, col, reverse):
        try:
            l = []
            for k in tv.get_children(''):
                val_str = str(tv.set(k, col)) # Get value as string first
                try:
                    # Attempt to convert to a numerical value for sorting times, scores, ranks
                    if ':' in val_str and '.' in val_str : # Likely time formatted as M:S.ms or H:M:S.ms
                        parts = val_str.replace('.',':').split(':') # Split by both . and :
                        num_val = 0
                        if len(parts) == 4: # H:M:S:MS
                            num_val = int(parts[0])*3600000 + int(parts[1])*60000 + int(parts[2])*1000 + int(parts[3])
                        elif len(parts) == 3: # M:S:MS or S:MS if minutes < 10
                            # Heuristic: if first part is small, it's minutes or seconds
                            if val_str.count(':') == 2 and val_str.index(':') < 3: # M:S.MS like 1:23.456 or 00:12:34.567
                                num_val = int(parts[0])*60000 + int(parts[1])*1000 + int(parts[2])
                            else: # S.MS like 23.456 (parts[0]=23, parts[1]=456 from split logic) or maybe 00:00:23.456
                                num_val = int(parts[0])*1000 + int(parts[1]) # Assuming S.MS if not a full H:M:S or M:S.ms
                        elif len(parts) == 2: # S.MS
                            num_val = int(parts[0])*1000 + int(parts[1])
                        else: # Fallback if parsing is tricky
                             num_val = float(val_str.replace(":", "").replace(".","")) # Fallback
                    elif val_str == "N/A":
                        num_val = float('-inf') if reverse else float('inf') # Sort N/A to bottom/top
                    else:
                        num_val = float(val_str) # Try direct float conversion for ranks, scores, etc.
                    l.append((num_val, k))
                except ValueError: # If not easily converted to number, sort as string
                    l.append((val_str.lower(), k)) # Fallback to string sort

            l.sort(key=lambda t: t[0], reverse=reverse)

            # Reorder items in the treeview and re-apply even/odd row tags
            for index, (val, k) in enumerate(l):
                tv.move(k, '', index)
                # Manage tags for striping, keeping other tags if any
                tags = list(tv.item(k, 'tags'))
                tags = [t for t in tags if t not in ('evenrow', 'oddrow')] # Remove old striping
                tags.append('evenrow' if index % 2 == 0 else 'oddrow') # Add new striping
                tv.item(k, tags=tuple(tags))

            # Update the heading command to sort in the opposite direction next time
            tv.heading(col, command=lambda _col=col, _tv=tv: self.treeview_sort_column(_tv, _col, not reverse))
        except Exception as e:
            self.log_to_gui(f"Error during sort for column {col}: {e}")


    def create_tabs(self):
        tab_configs = {
            "Overview": [("metric", "Metric", 300), ("value", "Value", 500)],
            "Maps": [("map_name", "Map Name", 220), ("map_uid", "Map UID", 160), ("players", "Players", 60),
                     ("best_time", "Any Best", 100), ("best_player", "Any Best Player", 150),
                     ("wr_time", "WR Time", 100), ("wr_holder", "WR Holder", 150),
                     ("last_fetched", "Last Fetched", 130)],
            "Overall Leaderboard": [("player_name", "Player", 180), ("overall_score", "Score (Formula)", 120),
                                    ("maps_contrib", "Maps", 80), ("country", "Country", 120)],
            "Players": [ ("player_name", "Player Name", 160), ("overall_score", "Score (Formula)", 110),
                        ("country", "Country", 110), ("maps_played", "Maps Played", 90),
                        ("first_seen", "First Seen", 140), ("player_id", "Player ID", 200)],
            "Country Top Players": [("country", "Country", 150), 
                                    ("global_rank", "Top Player's Global Rk", 70),
                                    ("player_name", "Top Player", 180), 
                                    ("overall_score", "Score", 120),
                                    ("num_players", "Num Players", 100)],
            "Playtime Stats": [("map_name", "Map Name", 250), ("total_playtime", "Total Playtime on Map", 200),
                               ("player_count", "Unique Players", 120)],
            "Recent PBs": [("player", "Player", 160), ("map_name", "Map Name", 180), ("new_time", "New Time", 100),
                           ("old_time", "Old Time", 100), ("improvement", "Improvement", 100), ("game_ts", "Record At", 140)],
            "New Players on Map": [("player", "Player", 160), ("map_name", "Map Name", 180), ("time", "Time", 100),
                                   ("game_ts", "Record At", 140)]
        }
        for name, cols_config in tab_configs.items():
            frame = ttk.Frame(self.notebook, padding=5)
            self.notebook.add(frame, text=name)
            tv = self.create_treeview_in_tab(frame, cols_config)
            self.tabs[name] = tv

        self.create_player_profile_tab()


    def create_player_profile_tab(self):
        player_profile_frame = ttk.Frame(self.notebook, padding=10)
        self.notebook.add(player_profile_frame, text="Player Profile")

        # --- Top Search Frame ---
        search_area_frame = ttk.Frame(player_profile_frame, padding=(0,0,0,10)) # Add bottom padding
        search_area_frame.pack(fill=tk.X, side=tk.TOP) # Ensure it's at the top
        ttk.Label(search_area_frame, text="Search Player:", font=('Calibri', 10)).pack(side=tk.LEFT, padx=(0,5))
        player_search_entry = ttk.Entry(search_area_frame, textvariable=self.player_profile_search_var, width=40)
        player_search_entry.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        player_search_entry.bind("<Return>", self.perform_player_profile_search) # Bind Enter key
        search_button = ttk.Button(search_area_frame, text="Search", command=self.perform_player_profile_search)
        search_button.pack(side=tk.LEFT, padx=5)

        # --- Main Content PanedWindow (Search Results | Player Details) ---
        main_paned_window = ttk.PanedWindow(player_profile_frame, orient=tk.HORIZONTAL)
        main_paned_window.pack(expand=True, fill=tk.BOTH)

        # --- Left Pane: Search Results Treeview ---
        search_results_frame = ttk.Frame(main_paned_window, padding=5) # Add padding around frame
        search_results_cols = [
            ("name", "Player Name", 180), ("country", "Country", 100),
            ("score", "Score", 80), ("global_rank", "Global Rk", 70), ("maps", "Maps", 50)
        ]
        self.player_search_results_tv = self.create_treeview_in_tab(search_results_frame, search_results_cols, height=15) # Added height
        self.player_search_results_tv.bind("<<TreeviewSelect>>", self.display_player_profile_details_from_search_selection)
        main_paned_window.add(search_results_frame, weight=1) # Give it a weight for resizing


        # --- Right Pane: Selected Player Details ---
        player_details_outer_frame = ttk.Frame(main_paned_window, padding=5) # Add padding

        # Player Summary Info
        summary_frame = ttk.LabelFrame(player_details_outer_frame, text="Player Summary", padding=10)
        summary_frame.pack(fill=tk.X, pady=(0,10)) # Add bottom margin
        
        info_labels_config = [
            ("Name:", self.selected_player_name_val), ("Country:", self.selected_player_country_val),
            ("Overall Score:", self.selected_player_score_val), ("Global Rank:", self.selected_player_global_rank_val),
            ("Maps Played:", self.selected_player_maps_played_val), ("First Seen:", self.selected_player_first_seen_val)
        ]
        for i, (text, var) in enumerate(info_labels_config):
            ttk.Label(summary_frame, text=text, font=('Calibri', 10, 'bold')).grid(row=i, column=0, sticky=tk.W, padx=5, pady=2)
            ttk.Label(summary_frame, textvariable=var, font=('Calibri', 10)).grid(row=i, column=1, sticky=tk.W, padx=5, pady=2)


        # Player Records Treeview
        records_frame_outer = ttk.LabelFrame(player_details_outer_frame, text="Player Map Records", padding=10)
        records_frame_outer.pack(expand=True, fill=tk.BOTH)

        player_records_cols = [
            ("map_name", "Map Name", 200), ("rank", "Rank", 60), ("time", "Time", 100),
            ("score", "Time in ms", 80), ("date", "Date", 120)
        ]
        # Need to pass a sub-frame to create_treeview_in_tab for it to pack correctly
        self.selected_player_records_tv = self.create_treeview_in_tab(records_frame_outer, player_records_cols)

        main_paned_window.add(player_details_outer_frame, weight=2) # Give it more weight
        
        # Initialize sash position after widgets are drawn
        self.root.update_idletasks() 
        initial_sash_pos = player_profile_frame.winfo_width() // 3
        if initial_sash_pos > 0: # Only set if reasonable
            main_paned_window.sashpos(0, initial_sash_pos)


    def _clear_player_profile_fields(self):
        self.player_profile_search_var.set("")
        if hasattr(self, 'player_search_results_tv'):
            for i in self.player_search_results_tv.get_children(): self.player_search_results_tv.delete(i)
        
        self.selected_player_name_val.set("N/A")
        self.selected_player_country_val.set("N/A")
        self.selected_player_score_val.set("N/A")
        self.selected_player_global_rank_val.set("N/A")
        self.selected_player_maps_played_val.set("N/A")
        self.selected_player_first_seen_val.set("N/A")

        if hasattr(self, 'selected_player_records_tv'):
            for i in self.selected_player_records_tv.get_children(): self.selected_player_records_tv.delete(i)


    def perform_player_profile_search(self, event=None): # event=None for button click
        query = self.player_profile_search_var.get().lower().strip()
        
        # Clear previous results and details
        for i in self.player_search_results_tv.get_children(): self.player_search_results_tv.delete(i)
        self.selected_player_name_val.set("N/A") # Reset displayed details
        self.selected_player_country_val.set("N/A")
        self.selected_player_score_val.set("N/A")
        self.selected_player_global_rank_val.set("N/A")
        self.selected_player_maps_played_val.set("N/A")
        self.selected_player_first_seen_val.set("N/A")
        for i in self.selected_player_records_tv.get_children(): self.selected_player_records_tv.delete(i)

        if not query:
            self.log_to_gui("Player search query is empty.")
            return

        self.log_to_gui(f"Searching for player: '{query}'")
        
        matches_found = 0
        # self.overall_lb_sorted_players_with_rank is already sorted by score
        for idx, p_data in enumerate(self.overall_lb_sorted_players_with_rank):
            if query in p_data['name'].lower():
                tags = ('evenrow',) if matches_found % 2 == 0 else ('oddrow',)
                self.player_search_results_tv.insert("", "end", iid=p_data['id'], values=( # Using player_id as iid
                    matches_found + 1, # Display index for search results
                    p_data['name'], 
                    p_data['country'] if p_data['country'] else "Unknown",
                    p_data['score'], 
                    p_data['global_rank'], # Global rank from precomputed list
                    p_data['maps']
                ), tags=tags)
                matches_found +=1
        self.log_to_gui(f"Found {matches_found} players matching '{query}'.")


    def display_player_profile_details_from_search_selection(self, event=None):
        selected_items = self.player_search_results_tv.selection()
        if not selected_items:
            return
        player_id = selected_items[0] # IID is player_id
        self.update_player_profile_display(player_id)


    def update_player_profile_display(self, player_id):
        # Find the full player data using player_id from the precomputed list
        player_data = next((p for p in self.overall_lb_sorted_players_with_rank if p['id'] == player_id), None)

        if not player_data:
            self.log_to_gui(f"Could not find data for player ID: {player_id}")
            return

        self.selected_player_name_val.set(player_data['name'])
        self.selected_player_country_val.set(player_data['country'] if player_data['country'] else "Unknown")
        self.selected_player_score_val.set(str(player_data['score']))
        self.selected_player_global_rank_val.set(str(player_data['global_rank']))
        self.selected_player_maps_played_val.set(str(player_data['maps']))
        first_seen_formatted = datetime.fromisoformat(player_data['first_seen']).strftime('%Y-%m-%d %H:%M') if player_data['first_seen'] else 'N/A'
        self.selected_player_first_seen_val.set(first_seen_formatted)

        # Clear old records
        for i in self.selected_player_records_tv.get_children(): self.selected_player_records_tv.delete(i)

        # Fetch and display map records for this player
        conn = sqlite3.connect(DATABASE_NAME)
        conn.row_factory = sqlite3.Row # Access columns by name
        cursor = conn.cursor()
        cursor.execute('''
            SELECT m.map_display_name, m.map_uid, r.position, r.time_ms, r.score, r.game_timestamp
            FROM records r
            JOIN maps m ON r.map_uid = m.map_uid
            WHERE r.player_id = ?
            ORDER BY m.map_display_name COLLATE NOCASE ASC 
        ''', (player_id,))
        
        for row_idx, record_row in enumerate(cursor.fetchall()):
            tags = ('evenrow',) if row_idx % 2 == 0 else ('oddrow',)
            self.selected_player_records_tv.insert("", "end", values=(
                row_idx + 1, # Display index
                record_row['map_display_name'] if record_row['map_display_name'] else record_row['map_uid'],
                record_row['position'] if record_row['position'] is not None else "N/A",
                format_time_ms(record_row['time_ms']),
                record_row['score'] if record_row['score'] is not None else "N/A", # Score here is API score, not calculated points
                datetime.fromisoformat(record_row['game_timestamp']).strftime('%y-%m-%d %H:%M') if record_row['game_timestamp'] else 'N/A'
            ), tags=tags)
        
        conn.close()
        self.log_to_gui(f"Displayed profile for {player_data['name']} (ID: {player_id}).")


    def refresh_all_tabs(self):
        self.log_to_gui("Refreshing all GUI tabs...")
        if hasattr(self, '_clear_player_profile_fields'): 
            self._clear_player_profile_fields() # Clear player profile search and details

        conn = sqlite3.connect(DATABASE_NAME)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # --- Precompute Player Scores and Ranks (used by multiple tabs) ---
        all_players_data_with_scores = []
        cursor.execute("SELECT player_id, last_known_name, country_name, first_seen_by_script_at FROM players")
        player_rows = cursor.fetchall()

        # Fetch all records once to avoid N+1 queries for scores
        all_records_map = {} # player_id -> list of positions
        cursor.execute("SELECT player_id, position FROM records WHERE position IS NOT NULL")
        for rec in cursor.fetchall():
            if rec['player_id'] not in all_records_map:
                all_records_map[rec['player_id']] = []
            all_records_map[rec['player_id']].append(rec['position'])

        for p_row in player_rows:
            player_id = p_row['player_id']
            overall_score = 0
            maps_played_count = 0
            if player_id in all_records_map:
                for position in all_records_map[player_id]:
                    overall_score += calculate_points_for_rank(position)
                maps_played_count = len(all_records_map[player_id])

            all_players_data_with_scores.append({
                'id': player_id, 'name': p_row['last_known_name'], 'country': p_row['country_name'],
                'score': round(overall_score, 2), 'maps': maps_played_count,
                'first_seen': p_row['first_seen_by_script_at']
            })

        # Sort all players by score for global ranking, then store with rank
        self.overall_lb_sorted_players_with_rank = []
        temp_sorted_for_rank = sorted(all_players_data_with_scores, key=lambda x: x['score'], reverse=True)
        for rank_idx, p_data in enumerate(temp_sorted_for_rank):
            player_info_with_rank = p_data.copy() 
            player_info_with_rank['global_rank'] = rank_idx + 1
            self.overall_lb_sorted_players_with_rank.append(player_info_with_rank)


        # --- Overview Tab ---
        tv_overview = self.tabs["Overview"]
        for i in tv_overview.get_children(): tv_overview.delete(i)
        cursor.execute("SELECT COUNT(*) as count FROM maps")
        total_maps = cursor.fetchone()['count']
        cursor.execute("SELECT COUNT(*) as count FROM players")
        total_players = cursor.fetchone()['count']
        cursor.execute("SELECT COUNT(*) as count FROM records")
        total_records = cursor.fetchone()['count']
        cursor.execute("SELECT SUM(r.time_ms) as total_time FROM records r WHERE r.time_ms IS NOT NULL") # Sum of all player times
        total_playtime_ms_row = cursor.fetchone()
        total_playtime_ms = total_playtime_ms_row['total_time'] if total_playtime_ms_row and total_playtime_ms_row['total_time'] else 0
        overview_data_list = [
            ("Total Maps Tracked", total_maps), ("Total Unique Players Seen", total_players),
            ("Total Records Stored", total_records),
            ("Grand Total Playtime (Sum of all record times)", format_time_ms(total_playtime_ms, show_millis=False) if total_playtime_ms else "N/A")
        ]
        for row_idx, (metric, value) in enumerate(overview_data_list):
            tags = ('evenrow',) if row_idx % 2 == 0 else ('oddrow',)
            tv_overview.insert("", "end", values=(row_idx + 1, metric, value), tags=tags)


        # --- Maps Tab ---
        tv_maps = self.tabs["Maps"]
        for i in tv_maps.get_children(): tv_maps.delete(i)
        cursor.execute('''
            SELECT m.map_display_name, m.map_uid, m.last_playercount,
                   r.time_ms as any_best_time, p_rec.last_known_name as any_best_player,
                   m.wr_time_ms, p_wr.last_known_name as wr_holder_name,
                   m.last_fetched_at, m.is_new_wr_since_last_fetch, m.fetch_order
            FROM maps m
            LEFT JOIN (
                SELECT map_uid, MIN(time_ms) as min_time 
                FROM records 
                WHERE position = 1 GROUP BY map_uid
            ) as min_r ON m.map_uid = min_r.map_uid
            LEFT JOIN records r ON m.map_uid = r.map_uid AND r.time_ms = min_r.min_time AND r.position = 1
            LEFT JOIN players p_rec ON r.player_id = p_rec.player_id
            LEFT JOIN players p_wr ON m.wr_player_id = p_wr.player_id
            ORDER BY m.fetch_order ASC, m.map_display_name COLLATE NOCASE ASC
        ''')
        for row_idx, row_data in enumerate(cursor.fetchall()):
            tags = ('new_wr_highlight',) if row_data['is_new_wr_since_last_fetch'] == 1 else ()
            # Add striping tag after conditional highlight tag
            tags += ('evenrow',) if row_idx % 2 == 0 else ('oddrow',)
            
            tv_maps.insert("", "end", values=(
                row_idx + 1, row_data['map_display_name'] if row_data['map_display_name'] else row_data['map_uid'],
                row_data['map_uid'], row_data['last_playercount'] if row_data['last_playercount'] is not None else '0',
                format_time_ms(row_data['any_best_time']), row_data['any_best_player'] if row_data['any_best_player'] is not None else 'N/A',
                format_time_ms(row_data['wr_time_ms']), row_data['wr_holder_name'] if row_data['wr_holder_name'] is not None else 'N/A',
                datetime.fromisoformat(row_data['last_fetched_at']).strftime('%y-%m-%d %H:%M') if row_data['last_fetched_at'] else 'N/A'
            ), tags=tags)


        # --- Overall Leaderboard Tab ---
        tv_overall_lb = self.tabs["Overall Leaderboard"]
        for i in tv_overall_lb.get_children(): tv_overall_lb.delete(i)
        # Use self.overall_lb_sorted_players_with_rank which is already sorted and has rank
        for p_data in self.overall_lb_sorted_players_with_rank:
            # global_rank starts at 1, so use (p_data['global_rank'] - 1) for 0-based indexing for striping
            tags = ('evenrow',) if (p_data['global_rank'] -1) % 2 == 0 else ('oddrow',) 
            tv_overall_lb.insert("", "end", values=(
                p_data['global_rank'], # Display rank as first column
                p_data['name'], p_data['score'], p_data['maps'], p_data['country'] if p_data['country'] else "Unknown"
            ), tags=tags)

        # --- Players Tab --- (Shows all players, ranked)
        tv_players_tab = self.tabs["Players"]
        for i in tv_players_tab.get_children(): tv_players_tab.delete(i)
        for p_data in self.overall_lb_sorted_players_with_rank: # Already sorted by score
            tags = ('evenrow',) if (p_data['global_rank'] -1) % 2 == 0 else ('oddrow',)
            tv_players_tab.insert("", "end", values=(
                p_data['global_rank'], # Display rank as first column
                p_data['name'], p_data['score'],
                p_data['country'] if p_data['country'] else "Unknown",
                p_data['maps'],
                datetime.fromisoformat(p_data['first_seen']).strftime('%y-%m-%d %H:%M') if p_data['first_seen'] else 'N/A',
                p_data['id']
            ), tags=tags)

        # --- Country Top Players Tab ---
        tv_country_top = self.tabs["Country Top Players"]
        for i in tv_country_top.get_children(): tv_country_top.delete(i)
        
        country_player_counts = {}
        for p_data_count in self.overall_lb_sorted_players_with_rank: # This list has all players
            country_for_count = p_data_count['country'] if p_data_count['country'] else "Unknown"
            country_player_counts[country_for_count] = country_player_counts.get(country_for_count, 0) + 1

        country_top_player_list_detailed = []
        temp_country_best_players = {} # country -> player_data (including global_rank and num_players_in_country)
        
        for p_data_top in self.overall_lb_sorted_players_with_rank: # Iterates by global rank
            country = p_data_top['country'] if p_data_top['country'] else "Unknown"
            if country == "Unknown": continue # Skip "Unknown" country for this list

            if country not in temp_country_best_players:
                player_info_for_country_top = p_data_top.copy()
                player_info_for_country_top['num_players_in_country'] = country_player_counts.get(country, 0)
                temp_country_best_players[country] = player_info_for_country_top
        
        country_top_player_list_detailed = list(temp_country_best_players.values())
        # Sort by Global Rank
        country_top_player_list_detailed.sort(key=lambda x: x['global_rank'])


        for display_row_idx, item_data in enumerate(country_top_player_list_detailed):
            tags = ('evenrow',) if display_row_idx % 2 == 0 else ('oddrow',)
            tv_country_top.insert("", "end", values=(
                display_row_idx + 1, # Display index
                item_data['country'], 
                item_data['global_rank'], # Global rank of this country's top player
                item_data['name'], 
                item_data['score'],
                item_data['num_players_in_country'] # New column data
            ), tags=tags)


        # --- Playtime Stats Tab ---
        tv_playtime = self.tabs["Playtime Stats"]
        for i in tv_playtime.get_children(): tv_playtime.delete(i)
        cursor.execute('''
            SELECT
                m.map_display_name,
                m.map_uid,
                SUM(CASE WHEN r.time_ms IS NOT NULL THEN r.time_ms ELSE 0 END) as total_map_playtime,
                COUNT(DISTINCT r.player_id) as map_player_count
            FROM maps m
            LEFT JOIN records r ON m.map_uid = r.map_uid
            GROUP BY m.map_uid, m.map_display_name
            ORDER BY total_map_playtime DESC, m.map_display_name COLLATE NOCASE ASC
        ''')
        for row_idx, row_data in enumerate(cursor.fetchall()):
            tags = ('evenrow',) if row_idx % 2 == 0 else ('oddrow',)
            playtime_val = row_data['total_map_playtime']
            player_count_val = row_data['map_player_count']
            tv_playtime.insert("", "end", values=(
                row_idx + 1,
                row_data['map_display_name'] if row_data['map_display_name'] else row_data['map_uid'],
                format_time_ms(playtime_val, show_millis=False) if playtime_val > 0 else "0s" if playtime_val == 0 else "N/A", # Handle 0 explicitly
                player_count_val if player_count_val > 0 else "0"
            ), tags=tags)


        # --- Recent PBs Tab ---
        tv_pbs = self.tabs["Recent PBs"]
        for i in tv_pbs.get_children(): tv_pbs.delete(i)
        # Fetch PBs marked in DB
        cursor.execute('''
            SELECT p.last_known_name, m.map_display_name, r.map_uid, r.player_id, r.time_ms, r.game_timestamp
            FROM records r JOIN players p ON r.player_id = p.player_id JOIN maps m ON r.map_uid = m.map_uid
            WHERE r.is_pb_since_last_fetch = 1 ORDER BY r.script_updated_at DESC
        ''')
        pb_rows_from_db = cursor.fetchall()
        new_pbs_session_list = self.session_changes_for_gui.get('new_pbs', []) if self.session_changes_for_gui else []

        for row_idx, row_data in enumerate(pb_rows_from_db):
            player_name_db = row_data['last_known_name']
            map_name_val = row_data['map_display_name'] if row_data['map_display_name'] else row_data['map_uid']
            new_time_ms = row_data['time_ms']
            
            old_time_display = "N/A"
            improvement_display = "N/A"
            
            # Try to find the matching PB in session data to get old time
            # Session data: (player_name, map_name, new_time_ms, old_time_ms)
            matched_pb_session = next((pb for pb in new_pbs_session_list 
                                       if pb[0] == player_name_db and 
                                          pb[1] == map_name_val and 
                                          pb[2] == new_time_ms), None)
            if matched_pb_session:
                old_time_ms_session = matched_pb_session[3]
                old_time_display = format_time_ms(old_time_ms_session) if old_time_ms_session is not None else "N/A (First Time)"
                if old_time_ms_session is not None and new_time_ms is not None:
                    improvement_ms = old_time_ms_session - new_time_ms
                    improvement_display = format_time_ms(improvement_ms)
            
            tags = ['pb_highlight'] # Start with highlight tag
            tags.append('evenrow' if row_idx % 2 == 0 else 'oddrow') # Add striping tag
            
            tv_pbs.insert("", "end", values=(
                row_idx + 1, player_name_db, map_name_val, format_time_ms(new_time_ms),
                old_time_display, improvement_display,
                datetime.fromisoformat(row_data['game_timestamp']).strftime('%y-%m-%d %H:%M') if row_data['game_timestamp'] else 'N/A'
            ), tags=tuple(tags)) # Convert list to tuple for tags


        # --- New Players on Map Tab ---
        tv_new_players = self.tabs["New Players on Map"]
        for i in tv_new_players.get_children(): tv_new_players.delete(i)
        cursor.execute('''
            SELECT p.last_known_name, m.map_display_name, r.map_uid, r.time_ms, r.game_timestamp
            FROM records r JOIN players p ON r.player_id = p.player_id JOIN maps m ON r.map_uid = m.map_uid
            WHERE r.is_new_player_on_map_since_last_fetch = 1 ORDER BY r.script_recorded_at DESC
        ''')
        for row_idx, row_data in enumerate(cursor.fetchall()):
            tags = ['new_player_highlight'] # Start with highlight tag
            tags.append('evenrow' if row_idx % 2 == 0 else 'oddrow') # Add striping tag

            tv_new_players.insert("", "end", values=(
                row_idx + 1, row_data['last_known_name'],
                row_data['map_display_name'] if row_data['map_display_name'] else row_data['map_uid'],
                format_time_ms(row_data['time_ms']),
                datetime.fromisoformat(row_data['game_timestamp']).strftime('%y-%m-%d %H:%M') if row_data['game_timestamp'] else 'N/A'
            ), tags=tuple(tags))

        conn.close()
        self.log_to_gui("GUI tabs refreshed.")


if __name__ == '__main__':
    init_db()
    app_root = tk.Tk()
    app = TrackmaniaAnalyzerApp(app_root)
    app_root.mainloop()