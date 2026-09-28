"""
Synthetic data generator for zaki-pipeline-iac (Project 2).

Generates randomized manufacturing/client drawing METADATA records
(no actual drawing files) as a CSV, suitable for landing in the S3
raw zone and being processed by the Glue job.

Deliberately includes a small percentage of "dirty" records (missing
required fields, duplicate drawing_number+revision pairs) so the
pipeline's data-quality check step has real problems to catch.

Run with: python generate_drawings_metadata.py
Output: drawings_metadata_raw.csv in the same folder.
"""

import csv
import random
from datetime import date, timedelta

# --- Fixed reference lists -------------------------------------------------
# Using fixed lists (rather than a random-name library) keeps this script
# dependency-free and keeps the data reproducible/explainable.

ENGINEERS = ["A. Patel", "J. Smith", "M. Chen", "R. Okafor", "L. Novak"]

CLIENTS = [
    "Acme Manufacturing",
    "Brightline Industries",
    "Coastal Fabrication Ltd",
    "Delta Precision Works",
    "Everline Components",
]

DRAWING_TYPES = ["Assembly", "Detail", "BOM", "Schematic"]
MATERIALS = ["Steel", "Aluminium", "Stainless Steel", "Plastic", "Titanium"]
SHEET_SIZES = ["A4", "A3", "A2", "A1", "A0"]

# Status values a drawing revision can be in. "Superseded" is only ever
# applied to an earlier revision once a later one exists (handled below,
# not picked randomly from this list for that reason).
STATUSES = ["Draft", "Pending Approval", "Approved", "Rejected"]

# --- Config ------------------------------------------------------------
NUM_DRAWINGS = 150          # distinct drawing numbers
MAX_REVISIONS_PER_DRAWING = 3
DIRTY_RECORD_RATE = 0.05    # ~5% of rows get an intentional data-quality problem

FIELDNAMES = [
    "drawing_number",
    "revision",
    "client_name",
    "project_code",
    "drawing_title",
    "drawing_type",
    "material",
    "status",
    "issued_date",
    "drawn_by",
    "checked_by",
    "approved_by",
    "approved_date",
    "sheet_size",
    "superseded_by",
]


def random_date(start: date, end: date) -> date:
    delta_days = (end - start).days
    return start + timedelta(days=random.randint(0, delta_days))


def make_project_code(client_index: int) -> str:
    year = random.randint(2023, 2026)
    seq = random.randint(1, 999)
    return f"PRJ-{year}-{client_index:02d}{seq:03d}"


def build_drawing_revisions(drawing_number: str) -> list[dict]:
    """Build a realistic revision history for one drawing number:
    revision A, then possibly B, C, ... each superseding the last."""

    client_index = random.randint(0, len(CLIENTS) - 1)
    client_name = CLIENTS[client_index]
    project_code = make_project_code(client_index)
    drawing_title = f"{random.choice(['Bracket', 'Housing', 'Panel', 'Mount', 'Frame'])} " \
                     f"{random.choice(['Assembly', 'Detail', 'Weldment'])}"
    drawing_type = random.choice(DRAWING_TYPES)
    material = random.choice(MATERIALS)
    sheet_size = random.choice(SHEET_SIZES)

    num_revisions = random.randint(1, MAX_REVISIONS_PER_DRAWING)
    revision_letters = ["A", "B", "C"][:num_revisions]

    rows = []
    issue_date = random_date(date(2024, 1, 1), date(2026, 6, 1))

    for i, rev in enumerate(revision_letters):
        is_latest = (i == num_revisions - 1)
        drawn_by = random.choice(ENGINEERS)
        checked_by = random.choice([e for e in ENGINEERS if e != drawn_by])

        if is_latest:
            # Only the latest revision can still be in progress; earlier
            # revisions are always resolved as Approved/Rejected before
            # being superseded.
            status = random.choice(STATUSES)
        else:
            status = random.choice(["Approved", "Rejected"])

        approved_by = ""
        approved_date = ""
        if status == "Approved":
            approved_by = random.choice(ENGINEERS)
            approved_date = str(issue_date + timedelta(days=random.randint(1, 14)))

        # supersede reference: earlier revisions point forward to whichever
        # revision replaced them.
        superseded_by = f"{drawing_number}:{revision_letters[i + 1]}" if not is_latest else ""
        if not is_latest:
            status = "Superseded" if status == "Approved" else status

        rows.append({
            "drawing_number": drawing_number,
            "revision": rev,
            "client_name": client_name,
            "project_code": project_code,
            "drawing_title": drawing_title,
            "drawing_type": drawing_type,
            "material": material,
            "status": status,
            "issued_date": str(issue_date),
            "drawn_by": drawn_by,
            "checked_by": checked_by,
            "approved_by": approved_by,
            "approved_date": approved_date,
            "sheet_size": sheet_size,
            "superseded_by": superseded_by,
        })

        issue_date = issue_date + timedelta(days=random.randint(5, 60))

    return rows


def dirty_a_record(row: dict) -> dict:
    """Randomly corrupt one record to simulate a real-world bad record,
    so the pipeline's data-quality check has something genuine to catch."""
    problem = random.choice(["missing_drawing_number", "missing_client", "duplicate_marker"])
    row = dict(row)  # copy
    if problem == "missing_drawing_number":
        row["drawing_number"] = ""
    elif problem == "missing_client":
        row["client_name"] = ""
    elif problem == "duplicate_marker":
        # Leave as-is; duplicates are created separately below by
        # appending an exact copy of a clean row.
        pass
    return row


def main():
    all_rows: list[dict] = []

    for n in range(1, NUM_DRAWINGS + 1):
        drawing_number = f"DWG-{10000 + n}"
        all_rows.extend(build_drawing_revisions(drawing_number))

    # Inject a handful of dirty / duplicate records among the clean ones.
    num_dirty = int(len(all_rows) * DIRTY_RECORD_RATE)
    dirty_indexes = random.sample(range(len(all_rows)), k=num_dirty)

    for idx in dirty_indexes:
        all_rows[idx] = dirty_a_record(all_rows[idx])

    # Add a few exact-duplicate (drawing_number, revision) rows on purpose.
    for _ in range(max(1, num_dirty // 3)):
        source = random.choice([r for r in all_rows if r["drawing_number"]])
        all_rows.append(dict(source))

    random.shuffle(all_rows)

    with open("drawings_metadata_raw.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"Wrote {len(all_rows)} rows ({num_dirty} intentionally dirty) "
          f"to drawings_metadata_raw.csv")


if __name__ == "__main__":
    main()
