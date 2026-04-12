"""
cv_monitor.py — Computer vision monitor for chicken and egg counting.

Captures a frame every SCAN_INTERVAL_SECONDS, runs YOLO detection,
and writes to the database ONLY when the counts change compared to
the previous DB row. This means the latest DB row always reflects
the current situation, and unchanged periods are implicit.

Usage:
    python cv_monitor.py                 # normal run
    python cv_monitor.py --self-test     # dry-run without camera or DB
    python cv_monitor.py --camera 1      # use camera index 1
"""

import argparse
import sys
import time
from datetime import datetime

# =============================================================================
# CONFIGURATION
# =============================================================================

# Path to YOLO model files (relative to this script's directory)
CHICKEN_MODEL_PATH   = "CV/chick_model.pt"
EGG_MODEL_PATH       = "CV/egg_detection_model_1.pt"

# How often to capture and analyse a frame (seconds)
SCAN_INTERVAL_SECONDS = 60

# Only count a detection run as valid when at least this many chickens
# are visible — avoids writing zeros caused by a blocked camera view.
MIN_CHICKENS_VISIBLE = 1

# Active monitoring hours (24h). Outside these hours the service sleeps.
MONITORING_HOUR_START = 7
MONITORING_HOUR_END   = 19

# Camera index (0 = first/default camera on the Pi)
DEFAULT_CAMERA_INDEX  = 0

# =============================================================================
# CORE PIPELINE: DETECTION
# =============================================================================

def load_models():
    """Load and return (chicken_model, egg_model). Imported lazily so
    --self-test does not require ultralytics to be installed."""
    from ultralytics import YOLO
    import os
    base = os.path.dirname(os.path.abspath(__file__))
    chicken_model = YOLO(os.path.join(base, CHICKEN_MODEL_PATH))
    egg_model     = YOLO(os.path.join(base, EGG_MODEL_PATH))
    print("[CV] Models loaded.")
    return chicken_model, egg_model


def open_camera(index: int):
    """Try to open a camera. Returns a cv2.VideoCapture or raises RuntimeError."""
    import cv2
    candidates = [index] + [i for i in range(4) if i != index]
    for idx in candidates:
        cap = cv2.VideoCapture(idx, cv2.CAP_V4L2)
        cap.set(cv2.CAP_PROP_FRAME_WIDTH,  1280)
        cap.set(cv2.CAP_PROP_FRAME_HEIGHT,  720)
        time.sleep(0.3)
        if cap.isOpened():
            print(f"[CV] Camera opened on index {idx}.")
            return cap
        cap.release()
    raise RuntimeError("Could not open any camera device.")


def detect_counts(cap, chicken_model, egg_model):
    """
    Grab one frame and run both YOLO models.
    Returns (chickens: int, eggs: int) or raises RuntimeError on read failure.
    """
    import cv2
    ret, frame = cap.read()
    if not ret:
        raise RuntimeError("Failed to read frame from camera.")

    chickens = len(chicken_model(frame, verbose=False)[0].boxes)
    eggs     = len(egg_model(frame,     verbose=False)[0].boxes)
    return chickens, eggs


# =============================================================================
# CORE PIPELINE: CHANGE DETECTION + DB WRITE
# =============================================================================

def counts_changed(new_chickens: int, new_eggs: int, prev) -> bool:
    """
    Returns True if counts differ from the previous DB row (or if there
    is no previous row yet).
    prev is either None or (chickens, eggs).
    """
    if prev is None:
        return True
    return (new_chickens, new_eggs) != prev


# =============================================================================
# SERVICE RUNTIME
# =============================================================================

def main(camera_index: int = DEFAULT_CAMERA_INDEX) -> None:
    import db_utils as db

    db.setup_database()

    chicken_model, egg_model = load_models()
    cap = open_camera(camera_index)

    # Seed previous counts from DB so a restart doesn't cause a spurious write
    prev_counts = db.get_latest_cv_count()
    if prev_counts:
        print(f"[CV] Seeded from DB: chickens={prev_counts[0]}, eggs={prev_counts[1]}")
    else:
        print("[CV] No previous DB row found — first write will always go through.")

    print(f"[CV] Scanning every {SCAN_INTERVAL_SECONDS}s. "
          f"Active hours: {MONITORING_HOUR_START:02d}:00–{MONITORING_HOUR_END:02d}:00.")

    try:
        while True:
            now = datetime.now()

            # Sleep outside active hours
            if not (MONITORING_HOUR_START <= now.hour < MONITORING_HOUR_END):
                print(f"[CV] Outside active hours ({now.strftime('%H:%M')}). Sleeping 60s.")
                time.sleep(60)
                continue

            # Capture + detect
            try:
                chickens, eggs = detect_counts(cap, chicken_model, egg_model)
            except RuntimeError as exc:
                print(f"[CV] Detection error: {exc}. Retrying in 10s.")
                time.sleep(10)
                continue

            ts = now.strftime("%Y-%m-%d %H:%M:%S")
            print(f"[CV] [{ts}] chickens={chickens}, eggs={eggs}", end="")

            # Only write when something changed
            if chickens < MIN_CHICKENS_VISIBLE:
                print(f"  → skipped (chickens < {MIN_CHICKENS_VISIBLE}, likely blocked view)")
            elif counts_changed(chickens, eggs, prev_counts):
                row_id = db.insert_cv_count(chickens, eggs)
                prev_counts = (chickens, eggs)
                print(f"  → CHANGED — wrote row {row_id}")
            else:
                print("  → unchanged, no write")

            time.sleep(SCAN_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\n[CV] Stopped by user.")
    finally:
        import cv2
        cap.release()
        cv2.destroyAllWindows()
        print("[CV] Camera released.")


# =============================================================================
# DEV / SMOKE-TEST ENTRYPOINT
# =============================================================================

def run_self_test() -> None:
    """Dry-run without camera or DB. Just validates logic."""
    print("=== CV monitor self-test ===")

    # Simulate two identical readings then a change
    scenarios = [
        (12, 3,  None,         True,  "first ever reading → should write"),
        (12, 3,  (12, 3),      False, "same as before → should NOT write"),
        (11, 3,  (12, 3),      True,  "chicken count changed → should write"),
        (11, 4,  (11, 3),      True,  "egg count changed → should write"),
        (11, 4,  (11, 4),      False, "both same → should NOT write"),
    ]

    all_ok = True
    for chickens, eggs, prev, expected, label in scenarios:
        result = counts_changed(chickens, eggs, prev)
        status = "OK" if result == expected else "FAIL"
        if status == "FAIL":
            all_ok = False
        print(f"  [{status}] {label}")

    if all_ok:
        print("\nSelf-test PASSED.")
        sys.exit(0)
    else:
        print("\nSelf-test FAILED.")
        sys.exit(1)


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CV chicken/egg monitor")
    parser.add_argument("--self-test",  action="store_true",
                        help="Run logic self-test without camera or DB")
    parser.add_argument("--camera",     type=int, default=DEFAULT_CAMERA_INDEX,
                        help=f"Camera index (default {DEFAULT_CAMERA_INDEX})")
    args = parser.parse_args()

    if args.self_test:
        run_self_test()
    else:
        main(camera_index=args.camera)
