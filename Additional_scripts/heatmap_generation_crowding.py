import cv2
import csv
import numpy as np
import os
from datetime import datetime
from ultralytics import solutions

# --- Configuration ---
CHICKEN_MODEL_PATH  = "/home/projectwork/MONITORING/CV/chick_model.pt"
FRAME_SAVE_DIR      = "/home/projectwork/monitoring test file"
OUTPUT_DIR          = "/home/projectwork/monitoring test file"
CSV_LOG_PATH        = os.path.join(FRAME_SAVE_DIR, "counts.csv")
GRID_ROWS           = 3
GRID_COLS           = 3
LOW_THRESHOLD       = 0.33
MEDIUM_THRESHOLD    = 0.66

# --- Coop & welfare configuration ---
COOP_AREA_M2        = 10.0      # Total floor area of the coop in m²
AVG_WEIGHT_KG       = 2.0       # Average weight per chicken in kg
MAX_DENSITY_KG_M2   = 33.0      # EU legal maximum (33 kg/m²)
WELFARE_DENSITY     = 30.0      # Better Chicken Commitment recommendation (30 kg/m²)

# --- Pixel-based usage configuration ---
OCCUPANCY_THRESHOLD = 0.05      # Pixels below this fraction of max are considered unoccupied

def classify_occupancy(value, max_val):
    ratio = value / max_val if max_val > 0 else 0
    if ratio <= LOW_THRESHOLD:
        return "LOW"
    elif ratio <= MEDIUM_THRESHOLD:
        return "MEDIUM"
    else:
        return "HIGH"
        
        
def crowding():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # --- Read average chicken count from CSV ---
    avg_chicken_count = None
    if os.path.exists(CSV_LOG_PATH):
        with open(CSV_LOG_PATH, "r") as f:
            reader = csv.DictReader(f)
            counts = [int(row["chicken_count"]) for row in reader]
        if counts:
            avg_chicken_count = sum(counts) / len(counts)
            print(f"Average chicken count from logs: {avg_chicken_count:.1f}")
    else:
        print("Warning: counts.csv not found. Overcrowding assessment will be skipped.")
        return None

    # --- Collect saved frames ---
    frame_files = sorted([
        f for f in os.listdir(FRAME_SAVE_DIR)
        if f.endswith(".jpg")
    ])

    if not frame_files:
        print("No frames found in save directory. Exiting.")
        exit()
        return None

    print(f"Found {len(frame_files)} frames to process.")

    # --- Initialize heatmap ---
    heatmap = solutions.Heatmap(
        show=False,
        model=CHICKEN_MODEL_PATH,
        colormap=cv2.COLORMAP_PARULA,
    )

    last_frame = None

    for i, filename in enumerate(frame_files):
        frame_path = os.path.join(FRAME_SAVE_DIR, filename)
        frame = cv2.imread(frame_path)

        if frame is None:
            print(f"Could not read frame {filename}, skipping.")
            continue

        print(f"Processing frame {i+1}/{len(frame_files)}: {filename}")
        heatmap(frame)
        last_frame = frame

    if last_frame is None:
        print("No frames could be processed. Exiting.")
        return None

    # --- Heatmap post-processing ---
    raw_heatmap     = heatmap.heatmap
    normalized      = cv2.normalize(raw_heatmap, None, 0, 255, cv2.NORM_MINMAX)
    normalized      = np.uint8(normalized)
    colored_heatmap = cv2.applyColorMap(normalized, cv2.COLORMAP_PARULA)
    final_frame     = cv2.addWeighted(last_frame, 0.5, colored_heatmap, 0.5, 0)

    date_str     = datetime.now().strftime("%Y%m%d")
    heatmap_path = os.path.join(OUTPUT_DIR, f"heatmap_{date_str}.png")
    report_path  = os.path.join(OUTPUT_DIR, f"report_{date_str}.txt")

    cv2.imwrite(heatmap_path, final_frame)
    print(f"Heatmap saved to {heatmap_path}")

    # --- Zone analysis ---
    frame_h, frame_w = raw_heatmap.shape[:2]
    zone_h = frame_h // GRID_ROWS
    zone_w = frame_w // GRID_COLS

    zone_averages = np.zeros((GRID_ROWS, GRID_COLS))
    for row in range(GRID_ROWS):
        for col in range(GRID_COLS):
            y1, y2 = row * zone_h, (row + 1) * zone_h
            x1, x2 = col * zone_w, (col + 1) * zone_w
            zone_averages[row, col] = raw_heatmap[y1:y2, x1:x2].mean()

    max_occupancy = zone_averages.max()

    col_labels = ["Left", "Center", "Right"] if GRID_COLS == 3 else [f"Col {i+1}" for i in range(GRID_COLS)]
    row_labels = ["Top", "Middle", "Bottom"] if GRID_ROWS == 3 else [f"Row {i+1}" for i in range(GRID_ROWS)]

    # --- Pixel-based effective area calculation ---
    heatmap_01      = raw_heatmap / raw_heatmap.max() if raw_heatmap.max() > 0 else raw_heatmap
    total_pixels    = heatmap_01.size
    occupied_pixels = np.sum(heatmap_01 > OCCUPANCY_THRESHOLD)
    usage_ratio     = occupied_pixels / total_pixels
    effective_area  = usage_ratio * COOP_AREA_M2

    # --- Overcrowding assessment ---
    if avg_chicken_count is not None:
        total_weight      = avg_chicken_count * AVG_WEIGHT_KG
        effective_density = total_weight / effective_area if effective_area > 0 else 0
        total_density     = total_weight / COOP_AREA_M2

        if effective_density <= WELFARE_DENSITY:
            verdict = "NOT OVERCROWDED - within Better Chicken Commitment standards (< 30 kg/m²)"
        elif effective_density <= MAX_DENSITY_KG_M2:
            verdict = "ACCEPTABLE - within EU legal limit but above welfare recommendation"
        else:
            verdict = "OVERCROWDED - exceeds EU legal maximum of 33 kg/m²"


    # --- Build report ---
    report_lines = []
    report_lines.append("=" * 55)
    report_lines.append("           ZONE OCCUPANCY ANALYSIS REPORT")
    report_lines.append("=" * 55)
    report_lines.append(f"Generated on   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append(f"Frames used    : {len(frame_files)}")
    report_lines.append(f"Grid size      : {GRID_ROWS} rows x {GRID_COLS} cols")
    report_lines.append("-" * 55)

    for row in range(GRID_ROWS):
        for col in range(GRID_COLS):
            zone_name = f"{row_labels[row]}-{col_labels[col]}"
            avg       = zone_averages[row, col]
            occupancy = classify_occupancy(avg, max_occupancy)
            report_lines.append(f"{zone_name:<20} {occupancy:<10} (avg: {avg:.2f})")

    report_lines.append("-" * 55)
    report_lines.append("Most occupied zone  : " + f"{row_labels[np.unravel_index(zone_averages.argmax(), zone_averages.shape)[0]]}-{col_labels[np.unravel_index(zone_averages.argmax(), zone_averages.shape)[1]]}")
    report_lines.append("Least occupied zone : " + f"{row_labels[np.unravel_index(zone_averages.argmin(), zone_averages.shape)[0]]}-{col_labels[np.unravel_index(zone_averages.argmin(), zone_averages.shape)[1]]}")
    report_lines.append("=" * 55)
    report_lines.append("           EFFECTIVE SPACE USAGE")
    report_lines.append("=" * 55)
    report_lines.append(f"Total coop area         : {COOP_AREA_M2:.1f} m²")
    report_lines.append(f"Occupancy threshold     : {OCCUPANCY_THRESHOLD*100:.0f}% of max heatmap value")
    report_lines.append(f"Occupied pixels         : {occupied_pixels}/{total_pixels} ({usage_ratio*100:.1f}%)")
    report_lines.append(f"Effective used area     : {effective_area:.2f} m²")
    report_lines.append(f"Unused area             : {COOP_AREA_M2 - effective_area:.2f} m²")
    report_lines.append("=" * 55)
    report_lines.append("           OVERCROWDING ASSESSMENT")
    report_lines.append("=" * 55)

    if avg_chicken_count is not None:
        report_lines.append(f"Avg. chickens in frame  : {avg_chicken_count:.1f}")
        report_lines.append(f"Avg. weight per chicken : {AVG_WEIGHT_KG:.1f} kg")
        report_lines.append(f"Total estimated weight  : {total_weight:.1f} kg")
        report_lines.append(f"Density (total area)    : {total_density:.2f} kg/m²")
        report_lines.append(f"Density (effective area): {effective_density:.2f} kg/m²")
        report_lines.append("-" * 55)
        report_lines.append(f"Verdict: {verdict}")
    else:
        report_lines.append("No counts.csv found - assessment skipped.")

    report_lines.append("=" * 55)

    report_text = "\n".join(report_lines)
    print("\n" + report_text)

    with open(report_path, "w") as f:
        f.write(report_text)
    print(f"\nReport saved to {report_path}")

    # --- Clear processed frames and CSV ---
    print("\nCleaning up processed frames...")
    for filename in frame_files:
        os.remove(os.path.join(FRAME_SAVE_DIR, filename))
    os.remove(CSV_LOG_PATH)
    print(f"Removed {len(frame_files)} frames and counts.csv from {FRAME_SAVE_DIR}")
    
    return verdict
