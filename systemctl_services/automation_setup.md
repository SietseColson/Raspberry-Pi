# Colson Automation Service Setup

This guide explains how to install and enable the automation services on the Raspberry Pi.

## Files in this folder
- `automation.service` — runs `automation.py` continuously
- `smart-coop-control.service` — runs `ventilation_rate_calc/smart_coop_control.py` continuously

## Installation steps

1. Copy the service files to systemd:

```bash
sudo cp /home/projectwork/colson_bundle/systemctl_services/automation.service /etc/systemd/system/
sudo cp /home/projectwork/colson_bundle/systemctl_services/smart-coop-control.service /etc/systemd/system/
```

2. Reload systemd so it sees the new units:

```bash
sudo systemctl daemon-reload
```

3. Enable the services to start automatically on boot:

```bash
sudo systemctl enable automation.service
sudo systemctl enable smart-coop-control.service
```

4. Start them now:

```bash
sudo systemctl start automation.service
sudo systemctl start smart-coop-control.service
```

5. Check their status:

```bash
sudo systemctl status automation.service
sudo systemctl status smart-coop-control.service
```

6. Tail their logs for debugging:

```bash
sudo journalctl -u automation.service -f
sudo journalctl -u smart-coop-control.service -f
```

## Notes

- The services assume the code is located at `/home/projectwork/colson_bundle`.
- The Python interpreter used is `/home/projectwork/colson_bundle/.venv/bin/python`.
- If your virtual environment path differs, update `ExecStart` accordingly.
- `automation.py` now uses the H-bridge enable pin `GPIO25` for fan speed control.
- **Important:** If you have a `sensor-station.service` file, update the `ExecStart` line to change `sensor_station_colson.py` to `sensor_station.py` (remove the `_colson` suffix). Then, reload systemd with `sudo systemctl daemon-reload` and restart the service with `sudo systemctl restart sensor-station.service`.

## Expected behavior

- `automation.service` should manage the physical hardware (door, feeder, fan, predator LED).
- `smart-coop-control.service` should periodically calculate ventilation demand, map the result to fan `%`, and write that percentage to the `device_control` table.

## If something fails

- Confirm the `colson_bundle` directory exists and contains `automation.py` and `ventilation_rate_calc/smart_coop_control.py`.
- Ensure the Raspberry Pi user is `projectwork` or adjust the service `User=` line.
- Check for any import or runtime errors in the journal output.
