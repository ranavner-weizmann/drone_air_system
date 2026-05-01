# APL logger overview

This repository is a small sensor collection stack with four main layers:

1. **Sensor processes** read from serial or UDP devices and write one CSV per sensor.
2. **Special-case sensors** such as the spectrometer have their own standalone runner.
3. **Aggregation processes** read the latest sensor CSVs and produce merged outputs or reduced “vitals” outputs.
4. **The dashboard** (`revised.py`) loads merged CSV data and provides live plotting, notes, labels, thresholds, and quick review tools.

---

## Main files

### Core runners
- `runall.py` — master controller. Starts enabled sensor processes, waits for startup delays, then starts the merger. Also restarts crashed processes.
- `sensor_runner.py` — generic entrypoint for serial/UDP sensors defined in `sensor_config.json`. Run with:
  ```bash
  python sensor_runner.py <sensor_name>
  ```
- `generic_sensor.py` — shared base class for most sensors. Handles device discovery, serial setup, reconnects, CSV creation, logging, and the main read/write loop.
- `sensor_implementations.py` — concrete sensor classes and the `create_sensor(...)` factory.

### Standalone sensor / exporters
- `spectro.py` — standalone spectrometer collector. Writes both a summary CSV and a full-spectrum CSV.
- `vitals.py` — extracts a reduced set of “critical” columns from selected sensors and writes a historical vitals CSV plus a single-row live vitals CSV.

### Data aggregation
- `real_time_merger.py` — watches sensor CSV outputs and writes a merged CSV combining the most recent row from each active sensor.

### Configuration
- `sensor_config.json` — central configuration for enabled sensors, device identifiers, CSV column names, startup delays, scripts, and base path.

### Dashboard
- `revised.py` — Streamlit dashboard for viewing merged data. Supports:
  - live auto-refresh
  - viewing latest or uploaded CSV files
  - sensor status indicators
  - threshold warnings
  - notes and timed labels
  - data review / plotting

---

## Typical runtime flow

If the system is set up correctly, the normal startup path is:

```bash
python runall.py
```

What happens:
1. `runall.py` loads `sensor_config.json`
2. it changes into the configured base directory
3. it starts each enabled sensor using the configured script
4. it waits for the configured startup delay for each sensor
5. it starts `real_time_merger.py`
6. it keeps watching all child processes and restarts them if they die

### Individual sensor debugging
For a single generic sensor:
```bash
python sensor_runner.py imet
python sensor_runner.py partector2pro
python sensor_runner.py ldd
```

For the spectrometer:
```bash
python spectro.py
```

For the merger only:
```bash
python real_time_merger.py --interval 1.0
```

For the vitals exporter only:
```bash
python vitals.py --interval 1.0
```

For the dashboard:
```bash
streamlit run revised.py
```

---

## Output layout

The code expects a structure like:

```text
output/
  <sensor_name>/
    <sensor_name>_data_<timestamp>.csv
    <sensor_name>_log_<timestamp>.log
  spectro/
    spectro_summary_<timestamp>.csv
    spectro_full_<timestamp>.csv
    spectro_log_<timestamp>.log
  process_logs/
    <sensor>.log
    merger.log
  merged_data_<timestamp>.csv
```

### Important note about merged files
`real_time_merger.py` writes timestamped files such as:
```text
output/merged_data_YYYYMMDD_HHMMSS.csv
```

The dashboard should therefore be pointed at the folder where those files actually live, or a separate process should copy/symlink the latest merged file to a stable path if you want a fixed filename.

---

## Dashboard (`revised.py`)

The dashboard is intended as the operator view for merged CSV output.

### What it does
- loads the latest merged CSV or a selected local/uploaded CSV
- auto-refreshes on a timer
- plots selected variables over a recent time window
- shows alive/dead status for sensor groups
- lets you add notes tied to nearby timestamps
- lets you create labels / timed annotations
- stores thresholds, notes, label presets, and prefixes in local JSON/CSV files

### Local files the dashboard creates
In the same directory as `revised.py`:
- `merged_data_latest.csv` or another selected CSV (read target)
- `user_notes.csv`
- `user_labels.csv`
- `warning_thresholds.json`
- `label_prefixes.json`
- `label_presets.json`

### Running the dashboard
```bash
streamlit run revised.py
```

### Dashboard debugging checklist
If the dashboard opens but shows no live data:
1. Confirm that the merger is actually writing a merged CSV.
2. Confirm which directory the dashboard is scanning for merged files.
3. Confirm that the merged CSV contains a `merge_timestamp` column.
4. Confirm that timestamps parse correctly.
5. Confirm that the dashboard machine has permission to read the target files.
6. If using remote mode later, verify SSH credentials, remote path, and hostname.

If plots are blank:
1. Check whether the selected columns are numeric.
2. Check whether the selected data window is too short.
3. Check whether recent rows are connection-gap rows only.
4. Check whether the selected columns were filtered out because their names contain `date`, `time`, or `timestamp`.

If sensors appear dead:
1. Confirm the merger is still receiving fresh rows.
2. Confirm the most recent merged row contains non-empty values for that sensor’s prefixed columns.
3. Adjust the dashboard `Alive timeout (s)` if the sensor is naturally slow.

---

## General debugging workflow

When something is wrong, debug from the bottom up.

### 1. Confirm config is the one you think you are using
Check:
- `sensor_config.json` path
- enabled flags
- `script` values
- serial identifiers
- `column_names`
- configured base `path`

Because `runall.py` changes the working directory to the configured `path`, many file and script path issues come from running with the wrong config or an outdated base directory.

### 2. Start one process at a time
Do **not** start with the whole stack unless you already know the system is healthy.

Recommended order:
1. one sensor
2. confirm that sensor CSV updates
3. merger
4. confirm merged CSV updates
5. vitals exporter (if used)
6. dashboard

### 3. Check process logs
Useful log locations:
- `sensor_controller.log`
- `merger.log`
- `vitals_exporter.log`
- `output/process_logs/<sensor>.log`
- `output/<sensor>/<sensor>_log_<timestamp>.log`
- `output/spectro/spectro_log_<timestamp>.log`

### 4. Verify CSV headers against parsed row lengths
A very common failure mode is:
- parser returns a different number of columns than `column_names`
- merger then falls back to generic `col0`, `col1`, ... fields or leaves data blank
- downstream vitals / dashboard logic then silently stops matching expected names

When debugging a sensor, compare:
- configured `column_names`
- actual parsed row length
- actual CSV header written to disk

### 5. Verify device discovery
For serial sensors, check whether `pyudev` finds the expected device by:
- vendor ID
- model ID
- optional `serial_short`

If multiple devices share the same IDs, add a `serial_short` to disambiguate.

### 6. Verify merger inputs
For each enabled sensor, confirm that the expected file pattern actually matches a real file.

Examples:
- generic sensor default pattern: `output/<sensor>/<sensor>_data_*.csv`
- spectrometer override should point to the summary file if you want summary values in merged output

### 7. Verify dashboard source path
The dashboard must point to the same merged file location that the merger writes.
If not, the dashboard may look healthy but never update.

---

## FIFO / live command control

### LDD commands
`LDDSensor` supports runtime commands through a FIFO.

Create the FIFO once:
```bash
mkfifo output/ldd/cmd.fifo
```

Then in another terminal:
```bash
cat > output/ldd/cmd.fifo
```

Type commands and press Enter.

Valid commands:
- `PING`
- `GET`
- `RESET`
- `SETC <amps>`
- `SETT <degC>`

### Pump power control
`PumpSensor` supports runtime control through a FIFO.

Create the FIFO once:
```bash
mkfifo output/pump/power.fifo
```

Send a new power setpoint:
```bash
echo 55 > output/pump/power.fifo
```

### Cavity commands
`CavitySensor` is designed to use a single command FIFO:
```bash
mkfifo output/cavity/cmd.fifo
```

Examples:
```bash
echo "SETC 1.23" > output/cavity/cmd.fifo
echo "SETT 35.0" > output/cavity/cmd.fifo
echo "SETPWR 40" > output/cavity/cmd.fifo
echo "RESET" > output/cavity/cmd.fifo
```

---

## Known code issues worth fixing

These are the most obvious source-level issues visible from the current code:

1. **`revised.py` expects data in a location/name that does not match the merger output by default.**
   - The merger writes timestamped files under `output/`.
   - The dashboard defaults to `merged_data_latest.csv` in the same directory as `revised.py`.
   - Either update the dashboard paths or create a “latest” copy/symlink.

2. **`revised.py` remote path appears to point at a different project tree than `sensor_config.json`.**
   - That suggests a stale deployment path or copied dashboard config.

3. **`CavitySensor` builds `Path(..., exist_ok=True)`, which is invalid.**
   - `Path()` does not accept `exist_ok`.
   - This should be fixed before enabling the cavity sensor.

4. **Top-level logging verbosity uses `4`, but `GenericSensor` only defines mappings for `0..3`.**
   - It falls back to `INFO`, so verbosity `4` does not actually mean “more verbose”.

5. **The pump output column name `target_speed` does not match the value being written.**
   - The code writes power percentage, not an RPM speed target.

6. **`sensor_runner.py` prints a hard-coded “Available sensors” list that is incomplete/outdated.**
   - The actual allowed names come from `sensor_config.json`.

7. **The old README mentions `spectro_hdf5.py` and `read_hdf5.py`, but the current uploaded source uses `spectro.py`.**
   - The documentation should describe the files that actually exist in the active codebase.

8. **Partector 2 Pro issues reading bins, !X0006 causes failure.**
---

## Practical first checks after any change

After editing code or config:

```bash
python sensor_runner.py partector2pro
python real_time_merger.py --interval 1.0
streamlit run revised.py
```

Then verify:
- a fresh sensor CSV appears
- a fresh merged CSV appears
- the dashboard points at that merged CSV
- timestamps continue increasing
- selected dashboard variables plot without gaps

---

## Recommended cleanup items

- align merger output path and dashboard input path
- fix `CavitySensor` FIFO path construction
- rename pump `target_speed` to something like `target_power_pct`
- make dashboard host/path/credentials configurable instead of hard-coded
- make `sensor_runner.py` usage text dynamic from config
- make vitals exporter output paths internally consistent
- keep README synchronized with current filenames
