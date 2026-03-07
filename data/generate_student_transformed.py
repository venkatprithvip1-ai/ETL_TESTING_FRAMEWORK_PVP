"""
generate_student_transformed.py
================================
Generates students_transformed.csv — the TARGET dataset.

Simulates what an ETL pipeline would do after reading students_raw.csv:

  Transformations applied:
    1. student_name   → UPPER CASE
    2. student_class  → TRIM whitespace
    3. student_score  → ROUND to 2 decimal places
    4. student_gender → NULL replaced with "UNKNOWN"
    5. student_age    → invalid ages (outside 5-18) replaced with NULL

  Intentional ETL bugs injected (~3%):
    - Some names NOT uppercased   → TransformationChecker catches this
    - Some rows silently dropped  → MissingRowsChecker catches this
    - Some extra phantom rows     → ExtraRowsChecker catches this
    - Some scores changed         → AggregateMatchChecker catches this
    - Some JOIN explosion rows    → DuplicateExplosionChecker catches this

Run this AFTER generate_student_data.py:
    python data/generate_student_transformed.py
"""

import csv
import os
import random

random.seed(77)

SRC_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "student.csv")
TGT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "students_transformed.csv")


def transform_and_inject_bugs() -> None:
    if not os.path.exists(SRC_PATH):
        print(f"  ERROR: {SRC_PATH} not found.")
        print("  Run generate_student_data.py first.")
        return

    print(f"\n  Reading source : {SRC_PATH}")

    with open(SRC_PATH, "r", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))

    total_src = len(reader)
    print(f"  Source rows    : {total_src:,}")

    transformed = []
    dropped     = 0
    exploded    = 0
    bug_name    = 0
    bug_score   = 0

    for row in reader:

        # ── Intentional bug: drop ~1% of rows silently ───────────────────────
        if random.random() < 0.01:
            dropped += 1
            continue    # skip this row — MissingRowsChecker will catch it

        # ── Intentional bug: duplicate ~0.5% of rows ─────────────────────────
        duplicate_this = random.random() < 0.005

        # ── Transformation 1: student_name → UPPER CASE ──────────────────────
        name = row["student_name"] or ""
        if random.random() < 0.97:    # 97% correctly uppercased
            name = name.upper()
        else:
            bug_name += 1             # 3% NOT uppercased — bug!

        # ── Transformation 2: student_class → TRIM whitespace ────────────────
        cls = (row["student_class"] or "").strip()

        # ── Transformation 3: student_score → ROUND to 2 decimal places ──────
        try:
            score = round(float(row["student_score"]), 2)
            if random.random() < 0.01:    # 1% wrong score — bug!
                score = round(score + random.uniform(5, 20), 2)
                bug_score += 1
            score = str(score)
        except (ValueError, TypeError):
            score = row["student_score"]

        # ── Transformation 4: student_gender NULL → "UNKNOWN" ────────────────
        gender = row["student_gender"]
        if not gender or gender.strip() == "":
            gender = "UNKNOWN"

        # ── Transformation 5: invalid age → NULL ─────────────────────────────
        try:
            age = int(row["student_age"])
            age = str(age) if 5 <= age <= 18 else ""
        except (ValueError, TypeError):
            age = ""

        new_row = {
            "student_id"     : row["student_id"],
            "student_name"   : name,
            "student_class"  : cls,
            "student_gender" : gender,
            "student_age"    : age,
            "student_height" : row["student_height"],
            "student_score"  : score,
        }

        transformed.append(new_row)

        # Add duplicate row (JOIN explosion simulation)
        if duplicate_this:
            exploded += 1
            transformed.append(new_row.copy())

    # ── Add ~10 phantom rows (extra rows not in source) ───────────────────────
    phantom_ids = [str(total_src + i + 1) for i in range(10)]
    for pid in phantom_ids:
        transformed.append({
            "student_id"     : pid,
            "student_name"   : "PHANTOM STUDENT",
            "student_class"  : "Class 13A",
            "student_gender" : "M",
            "student_age"    : "20",
            "student_height" : "175.0",
            "student_score"  : "50.00",
        })

    # ── Write target CSV ──────────────────────────────────────────────────────
    with open(TGT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "student_id", "student_name", "student_class",
            "student_gender", "student_age", "student_height", "student_score"
        ])
        writer.writeheader()
        writer.writerows(transformed)

    print(f"  Target rows    : {len(transformed):,}")
    print(f"\n  Intentional ETL bugs injected:")
    print(f"    Rows dropped silently : {dropped:,}  ← MissingRowsChecker will catch")
    print(f"    Rows duplicated       : {exploded:,}  ← DuplicateExplosionChecker will catch")
    print(f"    Phantom rows added    : 10          ← ExtraRowsChecker will catch")
    print(f"    Names not uppercased  : {bug_name:,}  ← TransformationChecker will catch")
    print(f"    Scores changed        : {bug_score:,}  ← AggregateMatchChecker will catch")
    print(f"\n  ✓ Saved : {TGT_PATH}")
    print(f"\n  Next step:")
    print(f"  python main_v6.py --reconcile --dataset students")


if __name__ == "__main__":
    print("=" * 55)
    print("  STUDENT TRANSFORMATION GENERATOR")
    print("=" * 55)
    transform_and_inject_bugs()
    print("=" * 55)
