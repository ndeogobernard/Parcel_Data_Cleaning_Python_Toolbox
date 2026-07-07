# =============================================================================
# CleanParcels.pyt  —  Sequential (arcpy-only) version
# Ohio Statewide Travel Model — Land Inventory Pipeline
# Group 2 — Data Preparation
#
# PURPOSE
#   Cleans Parcels_Raw and produces Parcels_Cleaned. Integrates geometry
#   cleaning (multipart explosion, geometric deduplication, overlap-based
#   deduplication) with attribute cleaning (LUC parsing, geodesic area,
#   quality flagging) in one tool with user-controlled phase checkboxes.
#
# PROCESSING SEQUENCE
#   Phase 1  — Copy Parcels_Raw -> Parcels_Cleaned (always runs)
#   Phase 2  — Explode multipart polygons to singlepart (checkbox)
#   Phase 3  — FindIdentical geometric deduplication (checkbox)
#   Phase 4  — IoU overlap-based deduplication (checkbox)
#   Phase 5  — Attribute cleaning + geodesic Land_Acres (always runs)
#
# PARAMETERS
#   0  Project Geodatabase         (DEWorkspace)
#   1  Log Folder                  (DEFolder)
#   2  Scratch GDB                 (DEWorkspace)
#   3  XY Tolerance                (GPString,  default "10 Feet")
#   4  IoU Threshold               (GPDouble,  default 0.85)
#   5  Run Phase 2 — Multiparts    (GPBoolean, default True)
#   6  Run Phase 3 — FindIdentical (GPBoolean, default True)
#   7  Run Phase 4 — Overlap       (GPBoolean, default True)
#   8  Force Fresh Copy            (GPBoolean, default False)
#   9  Run Phase 5 — Attr Clean   (GPBoolean, default True)
#
# OUTPUTS — Parcels_Cleaned in GDB raw_inputs dataset
#
# LOGS (written to Log Folder / RunID /)
#   CleanParcels_Log_{RunID}.txt          — summary log, all phases
#   CleanParcels_GeomDetail_{RunID}.txt   — granular per-county/LUC detail
#   Multipart_Detail_{RunID}.csv          — one row per exploded feature
#   FindIdentical_Groups_{RunID}.csv      — one row per FEAT_SEQ group
#   Overlap_Groups_{RunID}.csv            — one row per overlap group
#   Overlap_Detail_{RunID}.csv            — one row per overlap-deleted record
#
# SCRATCH GDB OUTPUTS (persist for audit/recovery)
#   Deleted_Multipart_{RunID}
#   Deleted_FindIdentical_{RunID}
#   Deleted_Overlap_{RunID}
#
# KEY DESIGN DECISIONS
#   - Land_Acres: geodesic via shape.getArea('GEODESIC','ACRES'),
#     US Survey Acres. Replaces prior planar Shape_Area/43560.
#   - Geometry duplicates: hard-deleted from Parcels_Cleaned.
#     Full original records saved to scratch GDB for recovery.
#   - Keeper selection: prefer valid StateLUC; among valid-LUC records
#     (or if none), prefer largest geodesic area; tie-break lowest OID.
#   - Phase 1 copy skipped if Parcels_Cleaned exists and Force Fresh
#     Copy is unchecked — allows rerunning individual phases.
#   - FindIdentical runs statewide (all counties at once).
#   - Overlap detection county-by-county with graph-based connected
#     components for groups of 3+ mutually overlapping records.
#   - Memory workspace used for Phase 4 intermediates when county
#     count <= MEMORY_THRESHOLD; disk scratch GDB for larger counties.
# =============================================================================

import arcpy
import os
import csv
import datetime
import multiprocessing as mp
import sys as _sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import ctypes as _ctypes
from collections import defaultdict

# Worker functions live in a companion .py file so multiprocessing can
# import and pickle them on Windows (.pyt files are not importable).
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in _sys.path:
    _sys.path.insert(0, _SCRIPT_DIR)
from CleanParcels_workers import (
    _ogr_has_valid_luc, _ogr_read_county, _ogr_mem_layer,
    _p4_worker, _p4c_worker,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
INPUT_FC          = "Parcels_Raw"
OUTPUT_FC         = "Parcels_Cleaned"
OUTPUT_DATASET    = "raw_inputs"
FIND_IDENTICAL_TBL = "FindIdentical_Temp"
MEMORY_THRESHOLD  = 50000
BATCH_SIZE        = 1000

FLAG_NULL_LUC      = "NULL_LUC"
FLAG_BLANK_LUC     = "BLANK_LUC"
FLAG_PARSE_ERROR   = "PARSE_ERROR"
FLAG_ZERO_AREA     = "ZERO_AREA"
FLAG_NEGATIVE_AREA = "NEGATIVE_AREA"
FLAG_NULL_ID       = "NULL_PARCEL_ID"


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def generate_run_id():
    return datetime.datetime.now().strftime("LI_%Y%m%d_%H%M%S")


def setup_logs(log_folder, run_id):
    run_dir = os.path.join(log_folder, run_id)
    os.makedirs(run_dir, exist_ok=True)
    return {
        "main":   os.path.join(run_dir, f"CleanParcels_Log_{run_id}.txt"),
        "geom":   os.path.join(run_dir, f"CleanParcels_GeomDetail_{run_id}.txt"),
        "mp_csv": os.path.join(run_dir, f"Multipart_Detail_{run_id}.csv"),
        "fi_csv": os.path.join(run_dir, f"FindIdentical_Groups_{run_id}.csv"),
        "ov_grp": os.path.join(run_dir, f"Overlap_Groups_{run_id}.csv"),
        "ov_det": os.path.join(run_dir, f"Overlap_Detail_{run_id}.csv"),
    }


def wl(log_path, msg, also_print=True):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{ts}] {msg}\n")
    if also_print:
        arcpy.AddMessage(msg)


def wl2(logs, msg, also_print=True):
    wl(logs["main"], msg, also_print)
    wl(logs["geom"], msg, also_print=False)


def section(logs, title, log_key="main"):
    wl(logs[log_key], "")
    wl(logs[log_key], "=" * 68)
    wl(logs[log_key], title)
    wl(logs[log_key], "=" * 68)


def write_csv(path, fieldnames, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


# ---------------------------------------------------------------------------
# Attribute helpers
# ---------------------------------------------------------------------------
def parse_luc_code(stateluc_value):
    """
    Parse integer LUC code from StateLUC string.
    Handles multiple county formats:
      "510: Res-Single Family"  ->  510  (colon separator — standard)
      "510- Res-Single Family"  ->  510  (dash separator — Warren County)
      "999"                     ->  999  (bare integer)
    """
    if stateluc_value is None:
        return -1, False
    s = str(stateluc_value).strip()
    if not s:
        return -1, False
    # Try colon first (standard), then space-dash, then dash (Warren County)
    # Always split on first occurrence — descriptions may contain dashes
    for sep in [":", " -", "-"]:
        parts = s.split(sep, 1)
        if len(parts) >= 1:
            candidate = parts[0].strip()
            try:
                code = int(candidate)
                if code > 0:
                    return code, True
            except (ValueError, TypeError):
                continue
    try:
        code = int(s)
        return code, (code > 0)
    except ValueError:
        return -1, False


def append_flag(existing, new_flag):
    if not existing:
        return new_flag
    flags = existing.split(",")
    if new_flag not in flags:
        flags.append(new_flag)
    return ",".join(flags)


def has_valid_luc(stateluc_value):
    _, valid = parse_luc_code(stateluc_value)
    return valid


# ---------------------------------------------------------------------------
# GDB / selection helpers
# ---------------------------------------------------------------------------
def delete_fc_anywhere(gdb_path, fc_name, log_path):
    arcpy.env.workspace = gdb_path
    datasets = [""]
    listed = arcpy.ListDatasets("*", "Feature")
    if listed:
        datasets += listed
    for ds in datasets:
        p = (os.path.join(gdb_path, ds, fc_name)
             if ds else os.path.join(gdb_path, fc_name))
        if arcpy.Exists(p):
            if not arcpy.TestSchemaLock(p):
                raise RuntimeError(
                    f"Cannot delete {fc_name} at:\n  {p}\n"
                    f"It is locked — open in ArcGIS Pro.\n"
                    f"Remove from Contents pane, close attribute tables, "
                    f"then run again.")
            wl(log_path, f"  Deleting existing {fc_name}: {p}")
            arcpy.management.Delete(p)




def ensure_scratch_gdb(scratch_gdb, log_path):
    """
    Create scratch GDB if it does not exist.
    Handles both the GDB itself and its parent folder.
    """
    if arcpy.Exists(scratch_gdb):
        return
    parent = os.path.dirname(scratch_gdb)
    gdb_name = os.path.basename(scratch_gdb)
    if not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)
        wl(log_path, f"  Created scratch folder: {parent}")
    arcpy.management.CreateFileGDB(parent, gdb_name)
    wl(log_path, f"  Created scratch GDB: {scratch_gdb}")

def get_counties(fc_path):
    seen = set()
    with arcpy.da.SearchCursor(fc_path, ["County"]) as cur:
        for row in cur:
            if row[0]:
                seen.add(str(row[0]).strip())
    return sorted(seen)


def batch_delete(fc_path, oid_field, oid_list, log_path, label=""):
    deleted = 0
    lyr = "bd_lyr"
    if arcpy.Exists(lyr):
        arcpy.management.Delete(lyr)
    arcpy.management.MakeFeatureLayer(fc_path, lyr)
    for i in range(0, len(oid_list), BATCH_SIZE):
        batch = oid_list[i:i + BATCH_SIZE]
        where = f"{oid_field} IN ({','.join(str(o) for o in batch)})"
        arcpy.management.SelectLayerByAttribute(lyr, "NEW_SELECTION", where)
        arcpy.management.DeleteRows(lyr)
        deleted += len(batch)
        if deleted % 10000 == 0:
            wl(log_path, f"    {label} {deleted:,} deleted so far...")
    arcpy.management.SelectLayerByAttribute(lyr, "CLEAR_SELECTION")
    arcpy.management.Delete(lyr)
    return deleted


def save_to_scratch(fc_path, oid_field, oid_list,
                    scratch_gdb, out_name, log_path):
    if not oid_list:
        return
    lyr = "sts_lyr"
    if arcpy.Exists(lyr):
        arcpy.management.Delete(lyr)
    arcpy.management.MakeFeatureLayer(fc_path, lyr)
    arcpy.management.SelectLayerByAttribute(lyr, "CLEAR_SELECTION")
    for i in range(0, len(oid_list), BATCH_SIZE):
        batch = oid_list[i:i + BATCH_SIZE]
        where = f"{oid_field} IN ({','.join(str(o) for o in batch)})"
        arcpy.management.SelectLayerByAttribute(
            lyr, "ADD_TO_SELECTION", where)
    out_fc = os.path.join(scratch_gdb, out_name)
    if arcpy.Exists(out_fc):
        arcpy.management.Delete(out_fc)
    arcpy.conversion.FeatureClassToFeatureClass(lyr, scratch_gdb, out_name)
    arcpy.management.Delete(lyr)
    wl(log_path, f"  Saved {len(oid_list):,} deleted records -> {out_fc}")


def write_validation_log(gdb_path, run_id, stats, log_path):
    tbl = os.path.join(gdb_path, "Validation_Log")
    if not arcpy.Exists(tbl):
        arcpy.management.CreateTable(os.path.dirname(tbl), os.path.basename(tbl))
        for fn, ft, fl in [
            ("Run_ID","TEXT",30), ("Run_Time","TEXT",25),
            ("Tool_Name","TEXT",50), ("Check_ID","TEXT",20),
            ("Description","TEXT",255), ("Value_Numeric","DOUBLE",None),
            ("Status","TEXT",10), ("Notes","TEXT",500)]:
            kw = {"field_length": fl} if fl else {}
            arcpy.management.AddField(tbl, fn, ft, **kw)
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    flds = ["Run_ID","Run_Time","Tool_Name","Check_ID",
            "Description","Value_Numeric","Status","Notes"]
    with arcpy.da.InsertCursor(tbl, flds) as cur:
        for cid, desc, val, status, notes in stats:
            cur.insertRow([run_id, ts, "CleanParcels",
                           cid, desc, val, status, notes])


# ---------------------------------------------------------------------------
# Keeper selection
# ---------------------------------------------------------------------------
def select_keeper(group_records):
    """
    Priority: (1) valid StateLUC parse, (2) largest geodesic area,
    (3) lowest OID.
    Returns (keeper_oid, reason_str).
    """
    def sort_key(r):
        valid = 0 if has_valid_luc(r["stateluc"]) else 1
        area  = -(r["geodesic_acres"] if r["geodesic_acres"] > 0
                  else r["shape_area"] / 43560.0)
        return (valid, area, r["oid"])
    sorted_recs = sorted(group_records, key=sort_key)
    keeper = sorted_recs[0]
    reason = ("VALID_LUC_LARGEST_AREA"
              if has_valid_luc(keeper["stateluc"])
              else "LARGEST_AREA_NO_VALID_LUC")
    return keeper["oid"], reason



# ---------------------------------------------------------------------------
# PHASE 1b — RepairGeometry (always runs after Phase 1 copy)
# ---------------------------------------------------------------------------
def phase1b_repair_geometry(out_fc, logs):
    section(logs, "PHASE 1b — REPAIR GEOMETRY", "main")
    section(logs, "PHASE 1b — REPAIR GEOMETRY", "geom")

    # RepairGeometry must run before Phase 2 (MultipartToSinglepart) and
    # before Phase 4 (self-join Intersect). Both operations fail silently or
    # produce corrupt output on self-intersecting rings.
    #
    # OGC validation mode matches GEOS topology rules used by OGR workers in
    # OGR_AssignParcelsTAZ_Parallel. Fixes:
    #   - Self-intersecting rings (TopologyException: side location conflict)
    #   - Unassigned interior rings (TopologyException: unable to assign hole)
    #   - Degenerate rings (< 4 points)
    #   - Duplicate vertices
    #
    # DELETE_NULL: removes records with geometry that cannot be repaired.
    # These are unrecoverable and would produce null geometry downstream.

    count_before = int(arcpy.management.GetCount(out_fc)[0])
    wl(logs["main"], f"Records before repair : {count_before:,}")
    wl(logs["main"], "Running RepairGeometry (OGC, DELETE_NULL)...")

    result = arcpy.management.RepairGeometry(
        in_features=out_fc,
        delete_null="DELETE_NULL",
        validation_method="OGC"
    )

    count_after = int(arcpy.management.GetCount(out_fc)[0])
    deleted     = count_before - count_after
    wl(logs["main"], f"Records after repair  : {count_after:,}")

    if deleted:
        wl(logs["main"],
           f"  WARNING: {deleted:,} records with unrecoverable geometry deleted.")
    else:
        wl(logs["main"], "  No records deleted — all geometry repaired in place.")

    # Log repair messages from arcpy (reports each fix type and count)
    msgs = result.getMessages()
    if msgs:
        for line in msgs.splitlines():
            line = line.strip()
            if line:
                wl(logs["main"], f"  {line}", also_print=False)
                wl(logs["geom"], f"  {line}", also_print=False)

    wl(logs["main"],
       f"Phase 1b complete. {count_before:,} -> {count_after:,} "
       f"({deleted:,} unrecoverable deleted)")
    return count_before, count_after


# ---------------------------------------------------------------------------
# PHASE 2 — Multipart explosion
# ---------------------------------------------------------------------------
def phase2_explode_multiparts(out_fc, scratch_gdb, run_id, logs):
    section(logs, "PHASE 2 — MULTIPART EXPLOSION", "main")
    section(logs, "PHASE 2 — MULTIPART EXPLOSION", "geom")

    oid_field    = arcpy.Describe(out_fc).OIDFieldName
    counties     = get_counties(out_fc)
    count_before = int(arcpy.management.GetCount(out_fc)[0])

    wl(logs["main"], f"Counties    : {len(counties)}")
    wl(logs["main"], f"Records in  : {count_before:,}")
    wl(logs["geom"], f"Counties: {len(counties)}  Records in: {count_before:,}")

    scratch_mp   = "MultipartTemp_P2"
    mp_csv_rows  = []
    sw_multipart = 0
    sw_new_parts = 0
    all_mp_oids  = []
    county_stats = {}

    lyr = "p2_lyr"
    if arcpy.Exists(lyr):
        arcpy.management.Delete(lyr)
    arcpy.management.MakeFeatureLayer(out_fc, lyr)

    for county in counties:
        arcpy.management.SelectLayerByAttribute(
            lyr, "NEW_SELECTION", f"County = '{county}'")
        n_county = int(arcpy.management.GetCount(lyr)[0])

        mp_records = []
        with arcpy.da.SearchCursor(
            lyr, [oid_field, "SHAPE@", "LocalParcelID", "StateLUC"]
        ) as cur:
            for row in cur:
                if row[1] and row[1].partCount > 1:
                    mp_records.append({
                        "oid":        row[0],
                        "part_count": row[1].partCount,
                        "local_id":   str(row[2]) if row[2] else "",
                        "stateluc":   str(row[3]) if row[3] else "",
                    })

        n_mp = len(mp_records)
        sw_multipart += n_mp
        luc_bkdn = defaultdict(int)
        for r in mp_records:
            luc_bkdn[r["stateluc"]] += 1

        if n_mp == 0:
            county_stats[county] = {
                "n_county": n_county, "n_mp": 0,
                "n_new": 0, "luc_bkdn": {}
            }
            continue

        mp_oids  = [r["oid"] for r in mp_records]
        oid_str  = ",".join(str(o) for o in mp_oids)
        mp_where = f"{oid_field} IN ({oid_str})"
        arcpy.management.SelectLayerByAttribute(
            lyr, "NEW_SELECTION", mp_where)

        scratch_path = os.path.join(scratch_gdb, scratch_mp)
        if arcpy.Exists(scratch_path):
            arcpy.management.Delete(scratch_path)
        arcpy.management.MultipartToSinglepart(lyr, scratch_path)
        n_scratch = int(arcpy.management.GetCount(scratch_path)[0])
        n_new     = n_scratch - n_mp
        sw_new_parts += n_new

        for r in mp_records:
            mp_csv_rows.append({
                "County":          county,
                "Original_OID":    r["oid"],
                "LocalParcelID":   r["local_id"],
                "StateLUC":        r["stateluc"],
                "Original_Parts":  r["part_count"],
            })

        all_mp_oids.extend(mp_oids)

        arcpy.management.SelectLayerByAttribute(
            lyr, "NEW_SELECTION", mp_where)
        arcpy.management.DeleteRows(lyr)
        arcpy.management.Append(
            scratch_path, out_fc, schema_type="NO_TEST")
        arcpy.management.Delete(scratch_path)

        county_stats[county] = {
            "n_county": n_county, "n_mp": n_mp,
            "n_new": n_new, "luc_bkdn": dict(luc_bkdn)
        }
        wl(logs["geom"],
           f"  {county:<22}: {n_mp:>6} multipart  "
           f"+{n_new:>6} new parts  (county: {n_county:,})")

    arcpy.management.SelectLayerByAttribute(lyr, "CLEAR_SELECTION")
    arcpy.management.Delete(lyr)
    count_after = int(arcpy.management.GetCount(out_fc)[0])

    # Save all multipart originals in one pass — per-county calls would
    # overwrite the same FC on each iteration, leaving only the last county.
    save_to_scratch(
        out_fc, oid_field, all_mp_oids,
        scratch_gdb, f"Deleted_Multipart_{run_id}", logs["main"])

    # Geom log detail
    wl(logs["geom"], "")
    wl(logs["geom"], "--- Per-County LUC Breakdown (multipart features) ---")
    for county, s in sorted(county_stats.items()):
        if s["n_mp"] == 0:
            continue
        wl(logs["geom"], f"  {county}:")
        for luc, cnt in sorted(s["luc_bkdn"].items(), key=lambda x: -x[1]):
            wl(logs["geom"], f"    {luc:<52}: {cnt:,}")

    wl(logs["geom"], "")
    wl(logs["geom"],
       f"  {'County':<22} {'Total':>8} {'Multipart':>10} "
       f"{'NewParts':>10} {'Net':>8}")
    wl(logs["geom"], "  " + "-" * 62)
    for county in sorted(county_stats.keys()):
        s = county_stats[county]
        wl(logs["geom"],
           f"  {county:<22} {s['n_county']:>8,} {s['n_mp']:>10,} "
           f"{s['n_new']:>10,} {s['n_new']-s['n_mp']:>+8,}")

    wl(logs["geom"], "")
    wl(logs["geom"], "--- PHASE 2 STATEWIDE SUMMARY ---")
    wl(logs["geom"],
       f"  Records before   : {count_before:,}")
    wl(logs["geom"],
       f"  Multipart found  : {sw_multipart:,}")
    wl(logs["geom"],
       f"  New parts created: {sw_new_parts:,}")
    wl(logs["geom"],
       f"  Records after    : {count_after:,}")
    wl(logs["main"],
       f"Phase 2 complete. Multipart: {sw_multipart:,}  "
       f"New parts: {sw_new_parts:,}  "
       f"Records: {count_before:,} -> {count_after:,}")

    if mp_csv_rows:
        write_csv(logs["mp_csv"],
                  ["County","Original_OID","LocalParcelID",
                   "StateLUC","Original_Parts"],
                  mp_csv_rows)
        wl(logs["main"], f"  CSV: {logs['mp_csv']}")

    return count_before, count_after


# ---------------------------------------------------------------------------
# PHASE 3 — FindIdentical
# ---------------------------------------------------------------------------
def phase3_find_identical(out_fc, scratch_gdb, run_id, xy_tol, logs):
    section(logs, "PHASE 3 — FINDIDENTICAL DEDUPLICATION", "main")
    section(logs, "PHASE 3 — FINDIDENTICAL DEDUPLICATION", "geom")

    oid_field    = arcpy.Describe(out_fc).OIDFieldName
    count_before = int(arcpy.management.GetCount(out_fc)[0])

    wl(logs["main"], f"Records in   : {count_before:,}")
    wl(logs["main"], f"XY Tolerance : {xy_tol}")

    fi_table = os.path.join(scratch_gdb, FIND_IDENTICAL_TBL)
    if arcpy.Exists(fi_table):
        arcpy.management.Delete(fi_table)

    wl(logs["main"], "Running FindIdentical (statewide)...")
    arcpy.management.FindIdentical(
        in_dataset=out_fc,
        out_dataset=fi_table,
        fields=["Shape"],
        xy_tolerance=xy_tol,
        output_record_option="ONLY_DUPLICATES"
    )
    n_fi = int(arcpy.management.GetCount(fi_table)[0])
    wl(logs["main"], f"FindIdentical rows: {n_fi:,}")

    if n_fi == 0:
        wl(logs["main"], "No geometric duplicates found. Phase 3 done.")
        arcpy.management.Delete(fi_table)
        return count_before, count_before

    feat_seq_groups = defaultdict(list)
    with arcpy.da.SearchCursor(fi_table, ["IN_FID","FEAT_SEQ"]) as cur:
        for row in cur:
            feat_seq_groups[row[1]].append(row[0])
    arcpy.management.Delete(fi_table)

    all_group_oids = set()
    for fids in feat_seq_groups.values():
        all_group_oids.update(fids)

    oid_to_attrs = {}
    for i in range(0, len(list(all_group_oids)), BATCH_SIZE):
        batch = list(all_group_oids)[i:i+BATCH_SIZE]
        where = f"{oid_field} IN ({','.join(str(o) for o in batch)})"
        with arcpy.da.SearchCursor(
            out_fc,
            [oid_field,"County","LocalParcelID",
             "StateParcelID","StateLUC","Shape_Area","SHAPE@"],
            where
        ) as cur:
            for row in cur:
                shape = row[6]
                acres = shape.getArea("GEODESIC","ACRES") if shape else 0.0
                oid_to_attrs[row[0]] = {
                    "oid": row[0], "county": str(row[1]) if row[1] else "",
                    "local_id": str(row[2]) if row[2] else "",
                    "state_id": str(row[3]) if row[3] else "",
                    "stateluc": str(row[4]) if row[4] else "",
                    "shape_area": row[5] or 0.0,
                    "geodesic_acres": acres,
                }

    oids_to_delete = []
    fi_csv_rows    = []
    county_groups  = defaultdict(int)
    county_deleted = defaultdict(int)
    county_max_grp = defaultdict(int)
    county_luc_del = defaultdict(lambda: defaultdict(int))
    pattern_a = pattern_b = 0

    for feat_seq, fids in feat_seq_groups.items():
        recs = [oid_to_attrs[f] for f in fids if f in oid_to_attrs]
        if not recs:
            continue
        keeper_oid, reason = select_keeper(recs)
        keeper_rec  = next(r for r in recs if r["oid"] == keeper_oid)
        deleted_recs = [r for r in recs if r["oid"] != keeper_oid]
        county = recs[0]["county"]

        local_ids   = set(r["local_id"] for r in recs)
        is_pat_a    = len(local_ids) == 1 and list(local_ids)[0] != ""
        if is_pat_a:
            pattern_a += 1
        else:
            pattern_b += 1

        lucs = list(set(r["stateluc"] for r in recs))
        county_groups[county]  += 1
        county_deleted[county] += len(deleted_recs)
        county_max_grp[county]  = max(county_max_grp[county], len(recs))

        for r in deleted_recs:
            oids_to_delete.append(r["oid"])
            county_luc_del[county][r["stateluc"]] += 1

        fi_csv_rows.append({
            "FEAT_SEQ":          feat_seq,
            "County":            county,
            "Group_Size":        len(recs),
            "Keeper_OID":        keeper_oid,
            "Keeper_LocalID":    keeper_rec["local_id"],
            "Keeper_StateLUC":   keeper_rec["stateluc"],
            "Keeper_Acres":      round(keeper_rec["geodesic_acres"], 6),
            "Keeper_Reason":     reason,
            "Deleted_OIDs":      "|".join(str(r["oid"]) for r in deleted_recs),
            "Deleted_LocalIDs":  "|".join(r["local_id"] for r in deleted_recs),
            "Deleted_StateLUCs": "|".join(r["stateluc"] for r in deleted_recs),
            "Pattern":           "A_SAME_ID" if is_pat_a else "B_DIFF_ID",
            "Mixed_LUC":         len(lucs) > 1,
            "All_LUC_Codes":     "|".join(sorted(lucs)),
        })

    save_to_scratch(
        out_fc, oid_field, oids_to_delete,
        scratch_gdb, f"Deleted_FindIdentical_{run_id}", logs["main"])

    wl(logs["main"], f"Deleting {len(oids_to_delete):,} records...")
    batch_delete(out_fc, oid_field, oids_to_delete,
                 logs["main"], "FindIdentical")
    count_after = int(arcpy.management.GetCount(out_fc)[0])

    # Geom log detail
    wl(logs["geom"], "")
    wl(logs["geom"], "--- Per-County FindIdentical Summary ---")
    wl(logs["geom"],
       f"  {'County':<22} {'Groups':>8} {'Deleted':>10} "
       f"{'MaxGrp':>8} {'DelPct':>8}")
    wl(logs["geom"], "  " + "-" * 60)

    with arcpy.da.SearchCursor(out_fc, ["County"]) as cur:
        cc = defaultdict(int)
        for row in cur:
            cc[str(row[0]) if row[0] else ""] += 1

    for county in sorted(county_deleted.keys()):
        nd  = county_deleted[county]
        nt  = cc.get(county, 0) + nd
        pct = 100 * nd / max(nt, 1)
        wl(logs["geom"],
           f"  {county:<22} {county_groups[county]:>8,} "
           f"{nd:>10,} {county_max_grp[county]:>8,} {pct:>7.2f}%")

    wl(logs["geom"], "")
    wl(logs["geom"], "--- Per-County LUC Breakdown (deleted) ---")
    for county in sorted(county_luc_del.keys()):
        wl(logs["geom"], f"  {county}:")
        for luc, cnt in sorted(
                county_luc_del[county].items(), key=lambda x: -x[1]):
            wl(logs["geom"], f"    {luc:<52}: {cnt:,}")

    wl(logs["geom"], "")
    wl(logs["geom"], "--- PHASE 3 STATEWIDE SUMMARY ---")
    wl(logs["geom"],
       f"  Dup groups found         : {len(feat_seq_groups):,}")
    wl(logs["geom"],
       f"  Pattern A (same ID)      : {pattern_a:,}")
    wl(logs["geom"],
       f"  Pattern B (diff ID)      : {pattern_b:,}")
    wl(logs["geom"],
       f"  Records deleted          : {len(oids_to_delete):,}")
    wl(logs["geom"],
       f"  Records remaining        : {count_after:,}")
    wl(logs["main"],
       f"Phase 3 complete. Groups: {len(feat_seq_groups):,}  "
       f"Deleted: {len(oids_to_delete):,}  "
       f"Records: {count_before:,} -> {count_after:,}")

    if fi_csv_rows:
        write_csv(logs["fi_csv"],
                  ["FEAT_SEQ","County","Group_Size","Keeper_OID",
                   "Keeper_LocalID","Keeper_StateLUC","Keeper_Acres",
                   "Keeper_Reason","Deleted_OIDs","Deleted_LocalIDs",
                   "Deleted_StateLUCs","Pattern","Mixed_LUC",
                   "All_LUC_Codes"],
                  fi_csv_rows)
        wl(logs["main"], f"  CSV: {logs['fi_csv']}")

    return count_before, count_after


# ---------------------------------------------------------------------------
# PHASE 4 — IoU overlap detection
# ---------------------------------------------------------------------------
def _iou(area_a, area_b, intersection_area):
    union = area_a + area_b - intersection_area
    return (intersection_area / union) if union > 0 else 0.0



def phase4_overlap_detection(out_fc, scratch_gdb, run_id,
                              iou_threshold, logs, n_workers=1):
    """
    Phase 4: Within-county IoU overlap deduplication.
    Parallelized — each county processed by an OGR worker.
    All audit logging preserved: Overlap_Groups CSV, Overlap_Detail CSV,
    Deleted_Overlap scratch FC.
    """
    section(logs, "PHASE 4 — OVERLAP DETECTION (IoU) [PARALLEL]", "main")
    section(logs, "PHASE 4 — OVERLAP DETECTION (IoU) [PARALLEL]", "geom")

    oid_field    = arcpy.Describe(out_fc).OIDFieldName
    count_before = int(arcpy.management.GetCount(out_fc)[0])
    counties     = get_counties(out_fc)
    gdb_path     = os.path.dirname(os.path.dirname(out_fc))
    layer_name   = os.path.basename(out_fc)

    wl(logs["main"], f"Records in      : {count_before:,}")
    wl(logs["main"], f"IoU threshold   : {iou_threshold}")
    wl(logs["main"], f"Counties        : {len(counties)}")
    wl(logs["main"], f"Workers         : {n_workers}")

    args = [(gdb_path, layer_name, county, iou_threshold)
            for county in counties]

    all_to_delete  = []
    ov_grp_rows    = []
    ov_det_rows    = []
    county_grps    = defaultdict(int)
    county_del     = defaultdict(int)
    county_luc_del = defaultdict(lambda: defaultdict(int))
    group_id_ctr   = 0
    errors         = []

    wl(logs["main"], "Dispatching parallel workers...")
    import os as _os
    _os.environ["ARCGIS_WORKER"] = "1"   # workers check this — skip arcpy init
    _ctx = mp.get_context("spawn")
    results_p4 = []
    with _ctx.Pool(processes=n_workers) as pool:
        for result in pool.imap_unordered(_p4_worker, args):
            results_p4.append(result)
    _os.environ.pop("ARCGIS_WORKER", None)

    for done, (county, fids_del, comp_data, err) in enumerate(results_p4, 1):
        if True:  # keep indentation

            if err:
                errors.append(f"{county}: {err}")
                wl(logs["main"], f"  WARNING worker error {county}: {err[:120]}")

            county_grps[county] += len(comp_data)
            county_del[county]  += len(fids_del)
            all_to_delete.extend(fids_del)

            for cd in comp_data:
                group_id_ctr += 1
                gid      = group_id_ctr
                keeper   = cd['keeper']
                del_recs = cd['del_recs']
                reason   = cd['reason']
                recs     = [keeper] + del_recs
                local_ids = set(r['local_id'] for r in recs)
                lucs      = set(r['stateluc']  for r in recs)
                for r in del_recs:
                    county_luc_del[county][r['stateluc']] += 1
                ov_grp_rows.append({
                    "Group_ID":          gid,
                    "County":            county,
                    "Group_Size":        len(recs),
                    "Keeper_OID":        keeper['fid'],
                    "Keeper_LocalID":    keeper['local_id'],
                    "Keeper_StateLUC":   keeper['stateluc'],
                    "Keeper_Acres":      round(keeper['geo_acres'], 6),
                    "Keeper_Reason":     reason,
                    "Deleted_OIDs":      "|".join(str(r['fid'])    for r in del_recs),
                    "Deleted_LocalIDs":  "|".join(r['local_id']    for r in del_recs),
                    "Deleted_StateLUCs": "|".join(r['stateluc']    for r in del_recs),
                    "Max_IoU":           round(cd['max_iou'], 6),
                    "Min_IoU":           round(cd['min_iou'], 6),
                    "Same_LocalID":      len(local_ids) == 1,
                    "Mixed_LUC":         len(lucs) > 1,
                    "All_LUC_Codes":     "|".join(sorted(lucs)),
                })
                for r in del_recs:
                    pair_key = (min(r['fid'], keeper['fid']),
                                max(r['fid'], keeper['fid']))
                    fid_pair_iou = cd['fid_pairs'].get(pair_key, (0.0, 0.0))
                    ov_det_rows.append({
                        "Group_ID":         gid,
                        "County":           county,
                        "Deleted_OID":      r['fid'],
                        "Deleted_LocalID":  r['local_id'],
                        "Deleted_StateLUC": r['stateluc'],
                        "Deleted_Acres":    round(r['geo_acres'], 6),
                        "Keeper_OID":       keeper['fid'],
                        "IoU_vs_Keeper":    round(fid_pair_iou[1] if isinstance(fid_pair_iou, tuple) else 0.0, 6),
                        "Deletion_Reason":  "OVERLAP_DUPLICATE",
                    })

            if done % 10 == 0 or done == len(counties):
                wl(logs["main"],
                   f"  [{done:>3}/{len(counties)}] counties done — "
                   f"{len(all_to_delete):,} to delete so far")

    if all_to_delete:
        save_to_scratch(out_fc, oid_field, all_to_delete,
                        scratch_gdb, f"Deleted_Overlap_{run_id}", logs["main"])
        wl(logs["main"], f"Deleting {len(all_to_delete):,} overlap records...")
        batch_delete(out_fc, oid_field, all_to_delete,
                     logs["main"], "Overlap")

    count_after = int(arcpy.management.GetCount(out_fc)[0])

    wl(logs["geom"], "--- Per-County LUC Breakdown (overlap-deleted) ---")
    for cty in sorted(county_luc_del.keys()):
        wl(logs["geom"], f"  {cty}:")
        for luc, cnt in sorted(county_luc_del[cty].items(), key=lambda x: -x[1]):
            wl(logs["geom"], f"    {luc:<52}: {cnt:,}")

    wl(logs["geom"], "--- PHASE 4 STATEWIDE SUMMARY ---")
    wl(logs["geom"], f"  Overlap groups found  : {sum(county_grps.values()):,}")
    wl(logs["geom"], f"  Records deleted       : {len(all_to_delete):,}")
    wl(logs["geom"], f"  Records remaining     : {count_after:,}")
    wl(logs["main"],
       f"Phase 4 complete. Groups: {sum(county_grps.values()):,}  "
       f"Deleted: {len(all_to_delete):,}  "
       f"Records: {count_before:,} -> {count_after:,}")

    if ov_grp_rows:
        write_csv(logs["ov_grp"],
                  ["Group_ID","County","Group_Size","Keeper_OID",
                   "Keeper_LocalID","Keeper_StateLUC","Keeper_Acres",
                   "Keeper_Reason","Deleted_OIDs","Deleted_LocalIDs",
                   "Deleted_StateLUCs","Max_IoU","Min_IoU",
                   "Same_LocalID","Mixed_LUC","All_LUC_Codes"],
                  ov_grp_rows)
        wl(logs["main"], f"  Overlap groups CSV : {logs['ov_grp']}")
    if ov_det_rows:
        write_csv(logs["ov_det"],
                  ["Group_ID","County","Deleted_OID","Deleted_LocalID",
                   "Deleted_StateLUC","Deleted_Acres","Keeper_OID",
                   "IoU_vs_Keeper","Deletion_Reason"],
                  ov_det_rows)
        wl(logs["main"], f"  Overlap detail CSV : {logs['ov_det']}")

    return count_before, count_after

# ---------------------------------------------------------------------------
# PHASE 4c — Erase-based partial overlap resolution
# ---------------------------------------------------------------------------
def phase4c_erase_overlaps(out_fc, scratch_gdb, run_id,
                            min_overlap_sqft, logs, n_workers=1,
                            boundary_buffer_ft=500.0):
    """
    Phase 4c: Erase-based partial overlap resolution at county boundaries.

    Uses the same boundary buffer approach as Phase 4b — only processes
    parcels within boundary_buffer_ft of an adjacent county. This keeps
    the parcel count per pair small (boundary zone only) making the
    pairwise intersection fast.

    For each overlapping cross-boundary pair below IoU threshold:
    - Higher-priority parcel keeps full geometry
    - Lower-priority parcel loses the intersection zone (geometry.Difference)
    - Priority: Mapped LUC > unmapped; lower overlap% wins; larger area tiebreak
    - Empty result → deleted, saved to scratch GDB for audit

    Runs AFTER Phase 4b. Phase 4b deletes high-IoU cross-county duplicates.
    Phase 4c erases the remaining partial overlaps that Phase 4b missed.
    """
    section(logs, "PHASE 4c — ERASE PARTIAL OVERLAPS [PARALLEL]", "main")
    section(logs, "PHASE 4c — ERASE PARTIAL OVERLAPS [PARALLEL]", "geom")

    oid_field    = arcpy.Describe(out_fc).OIDFieldName
    count_before = int(arcpy.management.GetCount(out_fc)[0])
    gdb_path     = os.path.dirname(os.path.dirname(out_fc))

    wl(logs["main"], f"  Input records  : {count_before:,}")
    wl(logs["main"], f"  Min overlap    : {min_overlap_sqft:.1f} sq ft "
                     f"({min_overlap_sqft/43560:.4f} ac)")
    wl(logs["main"], f"  Buffer         : {boundary_buffer_ft} ft (boundary only)")
    wl(logs["main"], f"  Workers        : {n_workers}")

    # ── Reuse Phase 4b infrastructure: county polys + adjacent pairs ─────────
    taz_fc  = os.path.join(gdb_path, "raw_inputs", "TAZ_Polygons")
    cfg_tiers = os.path.join(gdb_path, "CFG_County_Tiers")

    county_polys = "memory/cp4c_county_polys"
    if arcpy.Exists(county_polys): arcpy.management.Delete(county_polys)
    arcpy.management.Dissolve(taz_fc, county_polys, "COUNTY")

    num_to_name = {}
    if arcpy.Exists(cfg_tiers):
        tier_fields = {f.name for f in arcpy.ListFields(cfg_tiers)}
        num_col  = next((f for f in ["County_Num","COUNTY_NUM"] if f in tier_fields), None)
        name_col = next((f for f in ["County_Name","COUNTY_NAME"] if f in tier_fields), None)
        if num_col and name_col:
            with arcpy.da.SearchCursor(cfg_tiers, [num_col, name_col]) as cur:
                for row in cur:
                    if row[0] is not None and row[1]:
                        num_to_name[int(row[0])] = str(row[1]).strip()

    neighbors_tbl = "memory/cp4c_neighbors"
    if arcpy.Exists(neighbors_tbl): arcpy.management.Delete(neighbors_tbl)
    arcpy.analysis.PolygonNeighbors(
        county_polys, neighbors_tbl,
        in_fields="COUNTY",
        area_overlap="NO_AREA_OVERLAP",
        both_sides="BOTH_SIDES")

    adjacent_pairs = set()
    with arcpy.da.SearchCursor(neighbors_tbl, ["src_COUNTY","nbr_COUNTY"]) as cur:
        for row in cur:
            if row[0] is None or row[1] is None: continue
            na = num_to_name.get(int(row[0]), str(row[0]))
            nb = num_to_name.get(int(row[1]), str(row[1]))
            if na and nb and na != nb:
                adjacent_pairs.add(tuple(sorted([na, nb])))

    for fc in [county_polys, neighbors_tbl]:
        if arcpy.Exists(fc): arcpy.management.Delete(fc)

    wl(logs["main"], f"  Adjacent pairs : {len(adjacent_pairs)}")

    # ── For each pair: select boundary parcels, run OGR erase ────────────────
    def _read_lyr_parcels(lyr):
        """Read parcels from an arcpy layer as list of dicts."""
        recs = []
        with arcpy.da.SearchCursor(
            lyr, [oid_field, "StateLUC", "SHAPE@"]
        ) as cur:
            for row in cur:
                oid, stateluc, shape = row
                if shape is None: continue
                recs.append({
                    'oid':      oid,
                    'stateluc': str(stateluc or ''),
                    'has_luc':  has_valid_luc(stateluc),
                    'area':     shape.getArea("PLANAR"),
                    'wkb':      bytes(shape.WKB),
                })
        return recs

    def _run_pair_erase(county_a, county_b, recs_a, recs_b):
        """
        OGR erase for one county pair.
        Returns {oid: new_wkb} for updates and set of OIDs to delete.
        """
        from osgeo import ogr as _ogr

        mem_ds  = _ogr.GetDriverByName('Memory').CreateDataSource('')
        mem_lyr = mem_ds.CreateLayer('', geom_type=_ogr.wkbPolygon)
        mem_lyr.CreateField(_ogr.FieldDefn('bidx', _ogr.OFTInteger))
        geom_b  = {}

        for i, p in enumerate(recs_b):
            g = _ogr.CreateGeometryFromWkb(p['wkb'])
            if g is None or g.IsEmpty(): continue
            gt = g.GetGeometryType() & ~0x80000000
            if gt not in (_ogr.wkbPolygon, _ogr.wkbMultiPolygon,
                          _ogr.wkbPolygon25D, _ogr.wkbMultiPolygon25D):
                g = g.GetLinearGeometry()
                if g is None or g.IsEmpty(): continue
            geom_b[i] = g
            feat = _ogr.Feature(mem_lyr.GetLayerDefn())
            feat.SetGeometry(g)
            feat.SetField('bidx', i)
            mem_lyr.CreateFeature(feat)
            feat = None

        erase_map = defaultdict(list)

        for p_a in recs_a:
            g_a = _ogr.CreateGeometryFromWkb(p_a['wkb'])
            if g_a is None or g_a.IsEmpty(): continue
            gt = g_a.GetGeometryType() & ~0x80000000
            if gt not in (_ogr.wkbPolygon, _ogr.wkbMultiPolygon,
                          _ogr.wkbPolygon25D, _ogr.wkbMultiPolygon25D):
                g_a = g_a.GetLinearGeometry()
                if g_a is None or g_a.IsEmpty(): continue

            env = g_a.GetEnvelope()
            mem_lyr.SetSpatialFilterRect(env[0], env[2], env[1], env[3])
            mem_lyr.ResetReading()

            for cand in mem_lyr:
                j   = cand.GetField('bidx')
                p_b = recs_b[j]
                g_b = geom_b.get(j)
                if g_b is None: continue
                try:
                    isect = g_a.Intersection(g_b)
                except Exception:
                    continue
                if isect is None or isect.IsEmpty(): continue
                ia = isect.Area()
                if ia < min_overlap_sqft: continue

                pct_a = ia / max(p_a['area'], 1.0)
                pct_b = ia / max(p_b['area'], 1.0)
                pri_a = (1 if p_a['has_luc'] else 0, -pct_a, p_a['area'])
                pri_b = (1 if p_b['has_luc'] else 0, -pct_b, p_b['area'])

                if pri_a >= pri_b:
                    erase_map[p_b['oid']].append((isect, p_b))
                else:
                    erase_map[p_a['oid']].append((isect, p_a))

        mem_ds = None
        if not erase_map:
            return {}, set()

        oid_to_rec = {p['oid']: p for p in recs_a + recs_b}
        update_geoms = {}
        delete_oids  = set()

        for oid, isect_list in erase_map.items():
            p = oid_to_rec.get(oid)
            if p is None: continue
            new_geom = _ogr.CreateGeometryFromWkb(p['wkb'])
            if new_geom is None: continue
            for ig, _ in isect_list:
                if new_geom is None or new_geom.IsEmpty(): break
                try: new_geom = new_geom.Difference(ig)
                except Exception: pass
            if new_geom is None or new_geom.IsEmpty():
                delete_oids.add(oid)
            else:
                gt = new_geom.GetGeometryType() & ~0x80000000
                if gt not in (_ogr.wkbPolygon, _ogr.wkbMultiPolygon):
                    new_geom = new_geom.GetLinearGeometry()
                if new_geom and not new_geom.IsEmpty():
                    update_geoms[oid] = new_geom.ExportToWkb()
                else:
                    delete_oids.add(oid)
        return update_geoms, delete_oids

    # ── Dispatch pairs via ThreadPoolExecutor ─────────────────────────────────
    # Read boundary parcel data for each pair first (arcpy, main thread)
    # then dispatch OGR erase work to threads
    all_update   = {}
    all_delete   = []
    total_erased = 0
    total_del    = 0

    pair_data = []
    sorted_pairs = sorted(adjacent_pairs)
    for county_a, county_b in sorted_pairs:
        lyr_a = "cp4c_lyr_a"
        lyr_b = "cp4c_lyr_b"
        for lyr in [lyr_a, lyr_b]:
            if arcpy.Exists(lyr): arcpy.management.Delete(lyr)

        arcpy.management.MakeFeatureLayer(out_fc, lyr_a,
                                          f"County = '{county_a}'")
        arcpy.management.MakeFeatureLayer(out_fc, lyr_b,
                                          f"County = '{county_b}'")

        arcpy.management.SelectLayerByLocation(
            lyr_a, "WITHIN_A_DISTANCE", lyr_b,
            f"{boundary_buffer_ft} Feet", "NEW_SELECTION")
        n_a = int(arcpy.management.GetCount(lyr_a)[0])

        arcpy.management.SelectLayerByLocation(
            lyr_b, "WITHIN_A_DISTANCE", lyr_a,
            f"{boundary_buffer_ft} Feet", "NEW_SELECTION")
        n_b = int(arcpy.management.GetCount(lyr_b)[0])

        if n_a == 0 or n_b == 0:
            continue

        recs_a = _read_lyr_parcels(lyr_a)
        recs_b = _read_lyr_parcels(lyr_b)
        pair_data.append((county_a, county_b, recs_a, recs_b))

    wl(logs["main"], f"  Pairs with boundary parcels: {len(pair_data)}")
    wl(logs["main"], "Dispatching OGR erase workers...")

    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = {
            executor.submit(_run_pair_erase, ca, cb, ra, rb): (ca, cb)
            for ca, cb, ra, rb in pair_data
        }
        for done, fut in enumerate(
            __import__('concurrent.futures', fromlist=['as_completed'])
                      .as_completed(futures), 1):
            ca, cb = futures[fut]
            try:
                upd, dels = fut.result()
                for oid, wkb in upd.items():
                    if oid not in all_update and oid not in all_delete:
                        all_update[oid] = wkb
                for oid in dels:
                    if oid not in all_update and oid not in all_delete:
                        all_delete.append(oid)
                total_erased += len(upd)
                total_del    += len(dels)
            except Exception as e:
                wl(logs["main"], f"  WARNING {ca}×{cb}: {e}")
            if done % 20 == 0 or done == len(pair_data):
                wl(logs["main"],
                   f"  [{done:>3}/{len(pair_data)}] pairs done — "
                   f"{total_erased:,} erased  {total_del:,} deleted")

    # ── Apply results ─────────────────────────────────────────────────────────
    if all_update:
        wl(logs["main"], f"Writing {len(all_update):,} geometry updates...")
        fids = list(all_update.keys())
        for i in range(0, len(fids), BATCH_SIZE):
            chunk = fids[i:i+BATCH_SIZE]
            where = (f"{oid_field} IN "
                     f"({','.join(str(o) for o in chunk)})")
            with arcpy.da.UpdateCursor(
                out_fc, [oid_field, "SHAPE@WKB"], where
            ) as cur:
                for row in cur:
                    wkb = all_update.get(row[0])
                    if wkb:
                        row[1] = wkb
                        cur.updateRow(row)

    if all_delete:
        save_to_scratch(out_fc, oid_field, all_delete,
                        scratch_gdb, f"Deleted_Erase_{run_id}", logs["main"])
        batch_delete(out_fc, oid_field, all_delete,
                     logs["main"], "Erase")

    count_after = int(arcpy.management.GetCount(out_fc)[0])
    wl(logs["main"],
       f"Phase 4c complete. "
       f"Erased: {total_erased:,}  "
       f"Deleted (empty): {len(all_delete):,}  "
       f"Records: {count_before:,} -> {count_after:,}")
    return count_before, count_after
def phase4b_cross_county_overlap(out_fc, scratch_gdb, run_id,
                                  iou_threshold, boundary_buffer_ft, logs):
    """
    Phase 4b: Cross-county boundary IoU overlap detection.

    Phase 4 catches overlaps WITHIN each county. This catches overlaps
    ACROSS county boundaries — e.g. a Champaign County parcel whose
    geometry bleeds into a Union County TAZ, overlapping a Union County
    parcel representing the same physical land.

    Methodology is identical to Phase 4:
      - IoU threshold, keeper selection (valid LUC > largest area > lowest OID)
      - Deleted records saved to scratch GDB for audit trail
      - DFS connected components for groups of overlapping parcels

    Runs after Phase 4. Both are needed; neither replaces the other.
    """
    section(logs, "PHASE 4b — CROSS-COUNTY BOUNDARY OVERLAP", "main")
    section(logs, "PHASE 4b — CROSS-COUNTY BOUNDARY OVERLAP", "geom")

    wl(logs["main"], f"  IoU threshold    : {iou_threshold}")
    wl(logs["main"], f"  Boundary buffer  : {boundary_buffer_ft} ft")

    oid_field    = arcpy.Describe(out_fc).OIDFieldName
    count_before = int(arcpy.management.GetCount(out_fc)[0])
    wl(logs["main"], f"  Input records    : {count_before:,}")

    # ── Step 1: Dissolve by County → county polygons ─────────────────────
    # ── Step 1: Build county boundaries from TAZ_Polygons ───────────────
    # Dissolve TAZ_Polygons (3,660 rows) by COUNTY (numeric) — fast.
    # Dissolving 6.5M parcel polygons produces hyper-complex county outlines
    # that hang PolygonNeighbors. TAZ_Polygons gives clean 88-polygon result
    # in seconds.
    wl(logs["main"], "Step 1: Building county boundaries from TAZ_Polygons...")
    gdb_path = os.path.dirname(os.path.dirname(out_fc))
    taz_fc   = os.path.join(gdb_path, "raw_inputs", "TAZ_Polygons")

    county_polys = "memory/cp4b_county_polys"
    if arcpy.Exists(county_polys):
        arcpy.management.Delete(county_polys)
    arcpy.management.Dissolve(taz_fc, county_polys, "COUNTY")
    n_counties = int(arcpy.management.GetCount(county_polys)[0])
    wl(logs["main"], f"  {n_counties} county polygons built from TAZ_Polygons")

    # ── Build County_Num -> County_Name map from CFG_County_Tiers ────────
    # TAZ_Polygons.COUNTY is numeric; Parcels_Cleaned.County is string name.
    # CFG_County_Tiers has both County_Num and County_Name to bridge them.
    cfg_tiers = os.path.join(gdb_path, "CFG_County_Tiers")
    num_to_name = {}
    if arcpy.Exists(cfg_tiers):
        tier_fields = {f.name for f in arcpy.ListFields(cfg_tiers)}
        num_col  = next((f for f in ["County_Num","COUNTY_NUM"]
                         if f in tier_fields), None)
        name_col = next((f for f in ["County_Name","COUNTY_NAME"]
                         if f in tier_fields), None)
        if num_col and name_col:
            with arcpy.da.SearchCursor(cfg_tiers, [num_col, name_col]) as cur:
                for row in cur:
                    if row[0] is not None and row[1]:
                        num_to_name[int(row[0])] = str(row[1]).strip()
    wl(logs["main"], f"  County num->name map: {len(num_to_name)} entries")

    # ── Step 2: PolygonNeighbors on 88 dissolved county polygons ─────────
    wl(logs["main"], "Step 2: Finding adjacent county pairs...")
    neighbors_tbl = "memory/cp4b_neighbors"
    if arcpy.Exists(neighbors_tbl):
        arcpy.management.Delete(neighbors_tbl)
    arcpy.analysis.PolygonNeighbors(
        county_polys, neighbors_tbl,
        in_fields="COUNTY",
        area_overlap="NO_AREA_OVERLAP",
        both_sides="BOTH_SIDES"
    )

    adjacent_pairs = set()
    with arcpy.da.SearchCursor(
        neighbors_tbl, ["src_COUNTY", "nbr_COUNTY"]
    ) as cur:
        for row in cur:
            if row[0] is None or row[1] is None:
                continue
            num_a  = int(row[0])
            num_b  = int(row[1])
            name_a = num_to_name.get(num_a, str(num_a))
            name_b = num_to_name.get(num_b, str(num_b))
            if name_a and name_b and name_a != name_b:
                adjacent_pairs.add(tuple(sorted([name_a, name_b])))

    for fc in [neighbors_tbl, county_polys]:
        if arcpy.Exists(fc):
            arcpy.management.Delete(fc)

    wl(logs["main"], f"  {len(adjacent_pairs)} adjacent county pairs")

    # ── Step 3: Cross-intersect each adjacent pair ────────────────────────
    wl(logs["main"], "Step 3: Cross-intersecting boundary subsets...")
    all_to_delete  = []
    total_groups   = 0

    for idx, (county_a, county_b) in enumerate(sorted(adjacent_pairs), 1):
        lyr_a = "cp4b_lyr_a"
        lyr_b = "cp4b_lyr_b"
        for lyr in [lyr_a, lyr_b]:
            if arcpy.Exists(lyr):
                arcpy.management.Delete(lyr)

        arcpy.management.MakeFeatureLayer(
            out_fc, lyr_a, f"County = '{county_a}'")
        arcpy.management.MakeFeatureLayer(
            out_fc, lyr_b, f"County = '{county_b}'")

        arcpy.management.SelectLayerByLocation(
            lyr_a, "WITHIN_A_DISTANCE", lyr_b,
            f"{boundary_buffer_ft} Feet", "NEW_SELECTION")
        n_a = int(arcpy.management.GetCount(lyr_a)[0])

        arcpy.management.SelectLayerByLocation(
            lyr_b, "WITHIN_A_DISTANCE", lyr_a,
            f"{boundary_buffer_ft} Feet", "NEW_SELECTION")
        n_b = int(arcpy.management.GetCount(lyr_b)[0])

        if n_a == 0 or n_b == 0:
            arcpy.management.Delete(lyr_a)
            arcpy.management.Delete(lyr_b)
            continue

        sub_a = "memory/cp4b_sub_a"
        sub_b = "memory/cp4b_sub_b"
        for fc in [sub_a, sub_b]:
            if arcpy.Exists(fc):
                arcpy.management.Delete(fc)

        arcpy.management.CopyFeatures(lyr_a, sub_a)
        arcpy.management.CopyFeatures(lyr_b, sub_b)
        arcpy.management.Delete(lyr_a)
        arcpy.management.Delete(lyr_b)

        # Centroid → out_fc OID map for both subsets
        centroid_to_out = {}
        where_both = (f"County IN ('{county_a}', '{county_b}')")
        with arcpy.da.SearchCursor(
            out_fc, [oid_field, "SHAPE@XY"], where_clause=where_both
        ) as cur:
            for row in cur:
                if row[1] and row[1][0] is not None:
                    centroid_to_out[
                        (round(row[1][0], 4), round(row[1][1], 4))
                    ] = row[0]

        def build_map(fc):
            m = {}
            with arcpy.da.SearchCursor(fc, ["OID@", "SHAPE@XY"]) as c:
                for r in c:
                    if r[1] and r[1][0] is not None:
                        k = (round(r[1][0], 4), round(r[1][1], 4))
                        if k in centroid_to_out:
                            m[r[0]] = centroid_to_out[k]
            return m

        map_a = build_map(sub_a)
        map_b = build_map(sub_b)

        isect = "memory/cp4b_isect"
        if arcpy.Exists(isect):
            arcpy.management.Delete(isect)
        try:
            arcpy.analysis.Intersect(
                [sub_a, sub_b], isect,
                join_attributes="ONLY_FID",
                output_type="INPUT")
        except Exception as e:
            wl(logs["main"],
               f"  [{idx}] {county_a}×{county_b}: Intersect failed — {e}")
            for p in [sub_a, sub_b, isect]:
                if arcpy.Exists(p): arcpy.management.Delete(p)
            continue
        finally:
            for p in [sub_a, sub_b]:
                if arcpy.Exists(p): arcpy.management.Delete(p)

        isect_fields = [f.name for f in arcpy.ListFields(isect)]
        fid_cols     = [f for f in isect_fields
                        if f.upper().startswith("FID_")]
        if len(fid_cols) < 2:
            arcpy.management.Delete(isect)
            continue

        fa, fb   = fid_cols[0], fid_cols[1]
        pair_dict = {}
        with arcpy.da.SearchCursor(
            isect, [fa, fb, "SHAPE@AREA"]
        ) as cur:
            for row in cur:
                ta, tb, ia = row[0], row[1], (row[2] or 0.0)
                if ia <= 0:
                    continue
                oa = map_a.get(ta)
                ob = map_b.get(tb)
                if oa is None or ob is None:
                    continue
                key = (min(oa, ob), max(oa, ob))
                if key not in pair_dict or ia > pair_dict[key]:
                    pair_dict[key] = ia

        arcpy.management.Delete(isect)
        if not pair_dict:
            continue

        involved = set()
        for a, b in pair_dict:
            involved.add(a)
            involved.add(b)

        oid_info = {}
        inv_list = list(involved)
        for i in range(0, len(inv_list), BATCH_SIZE):
            batch = inv_list[i:i+BATCH_SIZE]
            where = (f"{oid_field} IN "
                     f"({','.join(str(o) for o in batch)})")
            with arcpy.da.SearchCursor(
                out_fc,
                [oid_field, "LocalParcelID", "StateLUC",
                 "County", "SHAPE@AREA", "SHAPE@"],
                where
            ) as cur:
                for row in cur:
                    shape = row[5]
                    acres = (shape.getArea("GEODESIC", "ACRES")
                             if shape else 0.0)
                    oid_info[row[0]] = {
                        "oid":            row[0],
                        "local_id":       str(row[1]) if row[1] else "",
                        "stateluc":       str(row[2]) if row[2] else "",
                        "county":         str(row[3]) if row[3] else "",
                        "shape_area":     row[4] or 0.0,
                        "geodesic_acres": acres,
                    }

        adjacency = defaultdict(set)
        for (oa, ob), ia in pair_dict.items():
            inf_a = oid_info.get(oa)
            inf_b = oid_info.get(ob)
            if not inf_a or not inf_b:
                continue
            aa = (inf_a["geodesic_acres"] * 43560
                  if inf_a["geodesic_acres"] > 0
                  else inf_a["shape_area"])
            ab_ = (inf_b["geodesic_acres"] * 43560
                   if inf_b["geodesic_acres"] > 0
                   else inf_b["shape_area"])
            if _iou(aa, ab_, ia) >= iou_threshold:
                adjacency[oa].add(ob)
                adjacency[ob].add(oa)

        if not adjacency:
            continue

        visited    = set()
        components = []

        def dfs4b(start):
            stack, comp = [start], []
            while stack:
                node = stack.pop()
                if node in visited:
                    continue
                visited.add(node)
                comp.append(node)
                stack.extend(adjacency[node] - visited)
            return comp

        for node in set(adjacency.keys()):
            if node not in visited:
                comp = dfs4b(node)
                if len(comp) >= 2:
                    components.append(comp)

        pair_del = 0
        for comp in components:
            total_groups += 1
            recs = [oid_info[o] for o in comp if o in oid_info]
            keeper_oid, reason = select_keeper(recs)
            del_recs = [r for r in recs if r["oid"] != keeper_oid]
            for r in del_recs:
                all_to_delete.append(r["oid"])
                pair_del += 1

        if pair_del:
            wl(logs["main"],
               f"  [{idx:>3}] {county_a:<18}×{county_b:<18}"
               f"  boundary: {n_a}+{n_b}  "
               f"deleted: {pair_del}")
            wl(logs["geom"],
               f"  [{idx:>3}] {county_a}×{county_b}: "
               f"{pair_del} cross-county duplicates removed")

    wl(logs["main"],
       f"\nCross-county groups found  : {total_groups}")
    wl(logs["main"],
       f"Records to delete          : {len(all_to_delete)}")

    if not all_to_delete:
        wl(logs["main"],
           "No cross-county overlaps above IoU threshold.")
        return count_before, count_before

    save_to_scratch(
        out_fc, oid_field, all_to_delete, scratch_gdb,
        f"Deleted_CrossCounty_{run_id}", logs["main"])

    batch_delete(
        out_fc, oid_field, all_to_delete,
        logs["main"], label="Parcels_Cleaned (cross-county)")

    count_after = int(arcpy.management.GetCount(out_fc)[0])
    wl(logs["main"],
       f"Phase 4b complete. Removed {count_before - count_after:,} records.")
    return count_before, count_after



def phase5_attribute_clean(out_fc, gdb_path, logs):
    section(logs, "PHASE 5 — ATTRIBUTE CLEANING + GEODESIC LAND_ACRES", "main")

    existing = [f.name for f in arcpy.ListFields(out_fc)]
    for fname in ["LUC_Code","Land_Acres","Data_Flag"]:
        if fname in existing:
            arcpy.management.DeleteField(out_fc, fname)

    arcpy.management.AddField(
        out_fc,"LUC_Code","LONG",
        field_alias="OAC Code (Integer)",
        field_is_nullable="NULLABLE")
    arcpy.management.AddField(
        out_fc,"Land_Acres","DOUBLE",
        field_alias="Parcel Area Geodesic (US Survey Acres)",
        field_is_nullable="NULLABLE")
    arcpy.management.AddField(
        out_fc,"Data_Flag","TEXT",
        field_length=150,
        field_alias="Data Quality Flags",
        field_is_nullable="NULLABLE")

    wl(logs["main"],"  Fields added: LUC_Code, Land_Acres, Data_Flag")

    total = null_luc = blank_luc = parse_error = 0
    zero_area = negative_area = null_id = flagged_total = 0
    total_acres = 0.0

    county_acres       = defaultdict(lambda: {
        "count":0,"total":0.0,"zero":0,
        "min":float("inf"),"max":0.0})
    county_luc_counts  = defaultdict(lambda: defaultdict(int))
    county_flag_counts = defaultdict(lambda: defaultdict(int))

    # ── Step 5a: Geodesic area via CalculateField ─────────────────────────
    # ArcGIS internal engine — no Python loop, no per-row geometry load.
    # Much faster than calling shape.getArea() inside UpdateCursor.
    wl(logs["main"],
       "  Step 5a: Computing Land_Acres via CalculateField (geodesic)...")
    t5a = datetime.datetime.now()
    arcpy.management.CalculateField(
        out_fc, "Land_Acres",
        "!Shape.getArea('GEODESIC', 'ACRES')!",
        "PYTHON3"
    )
    el5a = round((datetime.datetime.now() - t5a).total_seconds(), 1)
    wl(logs["main"], f"  Step 5a complete in {el5a:.0f}s")

    # ── Step 5b: LUC parsing + flagging — no geometry in cursor ──────────
    # Land_Acres already computed; cursor reads it as a plain float.
    # Removing SHAPE@ eliminates per-row geometry loading — key speedup.
    wl(logs["main"], "  Step 5b: Parsing LUC codes and setting Data_Flag...")
    t5b = datetime.datetime.now()

    cursor_fields = [
        "StateLUC","LocalParcelID","County",
        "LUC_Code","Land_Acres","Data_Flag"
    ]

    arcpy.SetProgressor("default","Phase 5b — LUC parsing and flagging...")
    with arcpy.da.UpdateCursor(out_fc, cursor_fields) as cursor:
        for row in cursor:
            total         += 1
            stateluc       = row[0]
            parcel_id      = row[1]
            county         = str(row[2]) if row[2] else ""
            geodesic_acres = row[4] if row[4] is not None else 0.0
            flags          = ""

            total_acres += geodesic_acres

            cas = county_acres[county]
            cas["count"] += 1
            cas["total"] += geodesic_acres
            if geodesic_acres == 0.0:
                cas["zero"] += 1
            else:
                cas["min"] = min(cas["min"], geodesic_acres)
                cas["max"] = max(cas["max"], geodesic_acres)

            if geodesic_acres == 0.0:
                zero_area += 1
                flags = append_flag(flags, FLAG_ZERO_AREA)
                county_flag_counts[county][FLAG_ZERO_AREA] += 1
            elif geodesic_acres < 0.0:
                negative_area += 1
                flags = append_flag(flags, FLAG_NEGATIVE_AREA)
                county_flag_counts[county][FLAG_NEGATIVE_AREA] += 1

            luc_code, valid = parse_luc_code(stateluc)
            row[3] = luc_code
            county_luc_counts[county][str(stateluc) if stateluc else ""] += 1

            if stateluc is None:
                null_luc += 1
                flags = append_flag(flags, FLAG_NULL_LUC)
                county_flag_counts[county][FLAG_NULL_LUC] += 1
            elif not str(stateluc).strip():
                blank_luc += 1
                flags = append_flag(flags, FLAG_BLANK_LUC)
                county_flag_counts[county][FLAG_BLANK_LUC] += 1
            elif not valid and luc_code == -1:
                parse_error += 1
                flags = append_flag(flags, FLAG_PARSE_ERROR)
                county_flag_counts[county][FLAG_PARSE_ERROR] += 1

            if parcel_id is None or str(parcel_id).strip() == "":
                null_id += 1
                flags = append_flag(flags, FLAG_NULL_ID)
                county_flag_counts[county][FLAG_NULL_ID] += 1

            row[5] = flags if flags else ""
            if flags:
                flagged_total += 1
            cursor.updateRow(row)

    el5b = round((datetime.datetime.now() - t5b).total_seconds(), 1)
    wl(logs["main"], f"  Step 5b complete in {el5b:.0f}s")
    arcpy.ResetProgressor()
    final_count = int(arcpy.management.GetCount(out_fc)[0])

    # Geom log — per-county area table
    wl(logs["geom"],"")
    wl(logs["geom"],"--- Phase 5: Per-County Geodesic Area Summary ---")
    wl(logs["geom"],
       f"  {'County':<22} {'Records':>10} {'TotalAcres':>14} "
       f"{'MinAcres':>12} {'MaxAcres':>12} {'ZeroGeo':>8}")
    wl(logs["geom"],"  " + "-"*82)
    for county in sorted(county_acres.keys()):
        cas   = county_acres[county]
        min_a = f"{cas['min']:.4f}" if cas["min"]!=float("inf") else "N/A"
        wl(logs["geom"],
           f"  {county:<22} {cas['count']:>10,} "
           f"{cas['total']:>14.2f} "
           f"{min_a:>12} "
           f"{cas['max']:>12.4f} "
           f"{cas['zero']:>8,}")
    wl(logs["geom"],
       f"\n  Total statewide Land_Acres (geodesic): {total_acres:,.2f}")

    # Geom log — per-county LUC distribution
    wl(logs["geom"],"")
    wl(logs["geom"],"--- Phase 5: Per-County LUC Distribution ---")
    for county in sorted(county_luc_counts.keys()):
        wl(logs["geom"], f"  {county}:")
        for luc, cnt in sorted(
                county_luc_counts[county].items(), key=lambda x: -x[1]):
            wl(logs["geom"], f"    {luc:<52}: {cnt:,}")

    # Geom log — per-county flag breakdown
    wl(logs["geom"],"")
    wl(logs["geom"],"--- Phase 5: Per-County Flag Breakdown ---")
    for county in sorted(county_flag_counts.keys()):
        fd = county_flag_counts[county]
        if not fd:
            continue
        wl(logs["geom"], f"  {county}:")
        for flag, cnt in sorted(fd.items()):
            wl(logs["geom"], f"    {flag:<20}: {cnt:,}")

    # Main log summary
    wl(logs["main"],f"  Records processed  : {total:,}")
    wl(logs["main"],
       f"  Records flagged    : {flagged_total:,}  "
       f"({100*flagged_total/max(total,1):.2f}%)")
    wl(logs["main"],f"  NULL_LUC           : {null_luc:,}")
    wl(logs["main"],f"  BLANK_LUC          : {blank_luc:,}")
    wl(logs["main"],f"  PARSE_ERROR        : {parse_error:,}")
    wl(logs["main"],f"  ZERO_AREA          : {zero_area:,}")
    wl(logs["main"],f"  NEGATIVE_AREA      : {negative_area:,}")
    wl(logs["main"],f"  NULL_PARCEL_ID     : {null_id:,}")
    wl(logs["main"],
       f"  Total Land_Acres   : {total_acres:,.2f} (geodesic US Survey Acres)")

    return {
        "total": total, "flagged_total": flagged_total,
        "null_luc": null_luc, "blank_luc": blank_luc,
        "parse_error": parse_error, "zero_area": zero_area,
        "negative_area": negative_area, "null_id": null_id,
        "final_count": final_count, "total_acres": total_acres,
    }


# ---------------------------------------------------------------------------
# Toolbox
# ---------------------------------------------------------------------------
class Toolbox:
    def __init__(self):
        self.label  = "Clean Parcels"
        self.alias  = "CleanParcels"
        self.tools  = [CleanParcels]


# ---------------------------------------------------------------------------
# Tool
# ---------------------------------------------------------------------------
class CleanParcels:

    def __init__(self):
        self.label       = "Clean Parcels"
        self.description = (
            "Cleans Parcels_Raw -> Parcels_Cleaned. "
            "Phase 2: Explode multiparts. "
            "Phase 3: FindIdentical deduplication. "
            "Phase 4: IoU overlap deduplication. "
            "Phase 5: Attribute cleaning + geodesic Land_Acres. "
            "Sequential arcpy-only version."
        )
        self.canRunInBackground = False

    def getParameterInfo(self):
        p0 = arcpy.Parameter(
            displayName="Project Geodatabase",
            name="project_gdb",
            datatype="DEWorkspace",
            parameterType="Required",
            direction="Input")
        p0.filter.list = ["Local Database"]

        p1 = arcpy.Parameter(
            displayName="Log Folder",
            name="log_folder",
            datatype="DEFolder",
            parameterType="Required",
            direction="Input")

        p2 = arcpy.Parameter(
            displayName="Scratch GDB (intermediates + deleted record archives)",
            name="scratch_gdb",
            datatype="DEWorkspace",
            parameterType="Required",
            direction="Input")
        p2.filter.list = ["Local Database"]

        p3 = arcpy.Parameter(
            displayName="XY Tolerance — Phase 3 FindIdentical (e.g. '10 Feet')",
            name="xy_tolerance",
            datatype="GPString",
            parameterType="Optional",
            direction="Input")
        p3.value = "10 Feet"

        p4 = arcpy.Parameter(
            displayName="IoU Threshold — Phase 4 Overlap (0.0 to 1.0)",
            name="iou_threshold",
            datatype="GPDouble",
            parameterType="Optional",
            direction="Input")
        p4.value   = 0.85
        p4.enabled = True

        p5 = arcpy.Parameter(
            displayName="Run Phase 2 — Explode Multipart Polygons",
            name="run_phase2",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")
        p5.value = True

        p6 = arcpy.Parameter(
            displayName="Run Phase 3 — FindIdentical Deduplication",
            name="run_phase3",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")
        p6.value = True

        p7 = arcpy.Parameter(
            displayName="Run Phase 4 — IoU Overlap Deduplication",
            name="run_phase4",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")
        p7.value = True

        p8 = arcpy.Parameter(
            displayName="Force Fresh Copy (overwrite existing Parcels_Cleaned)",
            name="force_fresh",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")
        p8.value = False

        p9 = arcpy.Parameter(
            displayName="Run Phase 5 — Attribute Cleaning + Geodesic Land_Acres",
            name="run_phase5",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")
        p9.value = True

        p10 = arcpy.Parameter(
            displayName="Run Phase 4b — Cross-County Boundary Overlap",
            name="run_phase4b",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")
        p10.value = True

        p11 = arcpy.Parameter(
            displayName="Phase 4b — Boundary Buffer Distance (feet)",
            name="boundary_buffer_ft",
            datatype="GPDouble",
            parameterType="Optional",
            direction="Input")
        p11.value   = 500.0
        p11.enabled = True

        p12 = arcpy.Parameter(
            displayName="Run Phase 4c — Erase Partial Overlaps",
            name="run_phase4c",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input")
        p12.value = True

        p13 = arcpy.Parameter(
            displayName="Phase 4c — Minimum Overlap to Erase (sq ft)",
            name="min_overlap_sqft",
            datatype="GPDouble",
            parameterType="Optional",
            direction="Input")
        p13.value   = 43.56   # 0.001 acres
        p13.enabled = True

        p14 = arcpy.Parameter(
            displayName="Number of Parallel Workers (Phase 4 + 4c)",
            name="n_workers",
            datatype="GPLong",
            parameterType="Optional",
            direction="Input")
        p14.value = 20

        return [p0,p1,p2,p3,p4,p5,p6,p7,p8,p9,p10,p11,p12,p13,p14]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        parameters[4].enabled  = bool(parameters[7].value)
        parameters[11].enabled = bool(parameters[10].value)
        parameters[13].enabled = bool(parameters[12].value)
        # Force Phase 5 ON when Force Fresh Copy is ON:
        # a fresh Parcels_Cleaned has no LUC_Code/Land_Acres/Data_Flag.
        if bool(parameters[8].value):
            parameters[9].value = True
            parameters[9].enabled = False
        else:
            parameters[9].enabled = True
        return

    def updateMessages(self, parameters):
        if parameters[0].value and not parameters[0].hasError():
            gdb  = str(parameters[0].value)
            in_fc = os.path.join(gdb, OUTPUT_DATASET, INPUT_FC)
            if not arcpy.Exists(in_fc):
                parameters[0].setErrorMessage(
                    f"Parcels_Raw not found in {gdb}. "
                    "Run ExportParcels first.")

        run_p2 = parameters[5].value
        run_p3 = parameters[6].value
        run_p4 = parameters[7].value
        if (run_p3 or run_p4) and not run_p2:
            parameters[5].setWarningMessage(
                "WARNING: Phase 2 (Multipart Explosion) is not selected. "
                "Multipart polygons may still be present and could affect "
                "Phase 3 and Phase 4 results. Recommended: run Phase 2 first "
                "on a fresh copy before running Phase 3 or 4.")
        run_p5 = parameters[9].value
        if not run_p5 and (run_p2 or run_p3 or run_p4):
            parameters[9].setWarningMessage(
                "Phase 5 is OFF. LUC_Code, Land_Acres, and Data_Flag will NOT "
                "be updated. Safe to skip only if Phase 5 has already run on "
                "this Parcels_Cleaned and no fresh copy is being made.")
        return

    def execute(self, parameters, messages):
        gdb_path    = str(parameters[0].value)
        log_folder  = str(parameters[1].value)
        scratch_gdb = str(parameters[2].value)
        xy_tol      = str(parameters[3].value) if parameters[3].value else "10 Feet"
        iou_thresh  = float(parameters[4].value) if parameters[4].value else 0.85
        run_p2      = bool(parameters[5].value)
        run_p3      = bool(parameters[6].value)
        run_p4      = bool(parameters[7].value)
        force_fresh = bool(parameters[8].value)
        run_p5         = bool(parameters[9].value) if len(parameters) > 9 else True
        run_p4b        = bool(parameters[10].value) if len(parameters) > 10 else True
        boundary_buf   = float(parameters[11].value) if len(parameters) > 11 and parameters[11].value else 500.0
        run_p4c        = bool(parameters[12].value) if len(parameters) > 12 else True
        min_overlap    = float(parameters[13].value) if len(parameters) > 13 and parameters[13].value else 43.56
        n_workers      = int(parameters[14].value)   if len(parameters) > 14 and parameters[14].value else 20

        run_id    = generate_run_id()
        logs      = setup_logs(log_folder, run_id)
        run_start = datetime.datetime.now()

        in_fc  = os.path.join(gdb_path, OUTPUT_DATASET, INPUT_FC)
        out_fc = os.path.join(gdb_path, OUTPUT_DATASET, OUTPUT_FC)

        # Header
        for lk in ["main","geom"]:
            wl(logs[lk],"=" * 68)
            wl(logs[lk],"CleanParcels  (Sequential arcpy version)")
            wl(logs[lk],f"Run ID        : {run_id}")
            wl(logs[lk],f"Input         : {in_fc}")
            wl(logs[lk],f"Output        : {out_fc}")
            wl(logs[lk],f"Scratch GDB   : {scratch_gdb}")
            wl(logs[lk],f"XY Tolerance  : {xy_tol}")
            wl(logs[lk],f"IoU Threshold : {iou_thresh}")
            wl(logs[lk],f"Phase 2 (MP)  : {run_p2}")
            wl(logs[lk],f"Phase 3 (FI)  : {run_p3}")
            wl(logs[lk],f"Phase 4 (IoU) : {run_p4}")
            wl(logs[lk],f"Phase 4b(XCty): {run_p4b}")
            wl(logs[lk],f"Phase 5 (Attr): {run_p5}")
            wl(logs[lk],f"Force Fresh   : {force_fresh}")
            wl(logs[lk],"=" * 68)

        # Ensure scratch GDB exists
        ensure_scratch_gdb(scratch_gdb, logs["main"])

        # Phase 1 — Copy
        section(logs,"PHASE 1 — COPY Parcels_Raw -> Parcels_Cleaned","main")
        cleaned_exists = arcpy.Exists(out_fc)
        if force_fresh or not cleaned_exists:
            delete_fc_anywhere(gdb_path, OUTPUT_FC, logs["main"])
            wl(logs["main"],"Copying Parcels_Raw -> Parcels_Cleaned...")
            arcpy.conversion.FeatureClassToFeatureClass(
                in_features=in_fc,
                out_path=os.path.join(gdb_path,OUTPUT_DATASET),
                out_name=OUTPUT_FC)
            count_raw = int(arcpy.management.GetCount(out_fc)[0])
            wl(logs["main"],f"  Fresh copy complete. {count_raw:,} records.")
            fresh_copy_made = True
        else:
            count_raw = int(arcpy.management.GetCount(out_fc)[0])
            desc = arcpy.Describe(out_fc)
            wl(logs["main"],
               f"  Using existing Parcels_Cleaned "
               f"({count_raw:,} records). Force Fresh Copy is OFF.")
            wl(logs["main"],f"  Last modified: {desc.dateModified}")
            fresh_copy_made = False

        t1 = datetime.datetime.now()
        wl(logs["main"],
           f"  Phase 1 runtime: {(t1-run_start).total_seconds():.1f}s")

        count_p2_before = count_p2_after = count_raw
        count_p3_before = count_p3_after = count_raw
        count_p4_before = count_p4_after = count_raw

        # Phase 1b — RepairGeometry (always runs, never skippable)
        # Must run before Phase 2 and Phase 4 — both require valid geometry.
        ts = datetime.datetime.now()
        count_p1b_before, count_p1b_after = phase1b_repair_geometry(
            out_fc, logs)
        wl(logs["main"],
           f"  Elapsed: {(datetime.datetime.now()-ts).total_seconds():.1f}s")

        # Phase 2
        if run_p2:
            ts = datetime.datetime.now()
            count_p2_before, count_p2_after = phase2_explode_multiparts(
                out_fc, scratch_gdb, run_id, logs)
            wl(logs["main"],
               f"  Phase 2 runtime: "
               f"{(datetime.datetime.now()-ts).total_seconds():.1f}s")
        else:
            wl(logs["main"],"Phase 2 skipped.")

        # Phase 3
        if run_p3:
            ts = datetime.datetime.now()
            count_p3_before, count_p3_after = phase3_find_identical(
                out_fc, scratch_gdb, run_id, xy_tol, logs)
            wl(logs["main"],
               f"  Phase 3 runtime: "
               f"{(datetime.datetime.now()-ts).total_seconds():.1f}s")
        else:
            wl(logs["main"],"Phase 3 skipped.")

        # Phase 4
        if run_p4:
            ts = datetime.datetime.now()
            count_p4_before, count_p4_after = phase4_overlap_detection(
                out_fc, scratch_gdb, run_id, iou_thresh, logs, n_workers=n_workers)
            wl(logs["main"],
               f"  Phase 4 runtime: "
               f"{(datetime.datetime.now()-ts).total_seconds():.1f}s")
        else:
            wl(logs["main"],"Phase 4 skipped.")

        # Phase 4b — cross-county boundary overlap
        if run_p4b:
            ts = datetime.datetime.now()
            count_p4b_before, count_p4b_after = \
                phase4b_cross_county_overlap(
                    out_fc, scratch_gdb, run_id,
                    iou_thresh, boundary_buf, logs)
            wl(logs["main"],
               f"  Phase 4b runtime: "
               f"{(datetime.datetime.now()-ts).total_seconds():.1f}s")
        else:
            wl(logs["main"], "Phase 4b skipped.")
            count_p4b_before = count_p4b_after = 0

        # Phase 4c — erase partial overlaps
        count_p4c_before = count_p4c_after = 0
        if run_p4c:
            ts = datetime.datetime.now()
            count_p4c_before, count_p4c_after = \
                phase4c_erase_overlaps(
                    out_fc, scratch_gdb, run_id,
                    min_overlap, logs, n_workers=n_workers)
            wl(logs["main"],
               f"  Phase 4c runtime: "
               f"{(datetime.datetime.now()-ts).total_seconds():.1f}s")
        else:
            wl(logs["main"], "Phase 4c skipped.")

        # Phase 5 — optional
        if run_p5:
            ts = datetime.datetime.now()
            p5_result = phase5_attribute_clean(out_fc, gdb_path, logs)
            wl(logs["main"],
               f"  Phase 5 runtime: "
               f"{(datetime.datetime.now()-ts).total_seconds():.1f}s")
        else:
            wl(logs["main"], "Phase 5 skipped.")
            p5_result = None

        # Overall summary
        run_end    = datetime.datetime.now()
        total_secs = (run_end - run_start).total_seconds()
        if p5_result:
            final_count = p5_result["final_count"]
        else:
            final_count = int(arcpy.management.GetCount(out_fc)[0])
        net_removed = count_raw - final_count

        summary = [
            "",
            "=" * 68,
            "CLEANPARCELS — OVERALL RUN SUMMARY",
            "=" * 68,
            f"  Run ID                              : {run_id}",
            f"  Fresh copy made                     : {fresh_copy_made}",
            f"  Input records (Parcels_Raw)         : {count_raw:,}",
            f"  After Phase 1b (repair geometry)    : "
            f"{count_p1b_after:,}  "
            f"(-{count_p1b_before-count_p1b_after:,} unrecoverable deleted)",
        ]
        if run_p2:
            summary.append(
                f"  After Phase 2 (multipart expl)     : "
                f"{count_p2_after:,}  "
                f"(net {count_p2_after-count_p2_before:+,})")
        if run_p3:
            summary.append(
                f"  After Phase 3 (FindIdentical)      : "
                f"{count_p3_after:,}  "
                f"(-{count_p3_before-count_p3_after:,} deleted)")
        if run_p4:
            summary.append(
                f"  After Phase 4 (IoU overlap)        : "
                f"{count_p4_after:,}  "
                f"(-{count_p4_before-count_p4_after:,} deleted)")
        if run_p4b:
            summary.append(
                f"  After Phase 4b (cross-county)      : "
                f"{count_p4b_after:,}  "
                f"(-{count_p4b_before-count_p4b_after:,} deleted)")
        if run_p4c:
            summary.append(
                f"  After Phase 4c (erase overlaps)    : "
                f"{count_p4c_after:,}  "
                f"(-{count_p4c_before-count_p4c_after:,} deleted, "
                f"plus geometry-modified parcels logged separately)")
        summary += [
            f"  Final output records                : {final_count:,}",
            f"  Net records removed                 : {net_removed:,}  "
            f"({100*net_removed/max(count_raw,1):.2f}%)",
            *([
                f"  Total Land_Acres (geodesic)         : "
                f"{p5_result['total_acres']:,.2f}",
                f"  Records with any flag               : "
                f"{p5_result['flagged_total']:,}  "
                f"({100*p5_result['flagged_total']/max(final_count,1):.2f}%)",
            ] if p5_result else [
                "  Phase 5 skipped — Land_Acres / flags not updated",
            ]),
            f"  Total runtime                       : "
            f"{total_secs:.1f}s  ({total_secs/60:.1f} min)",
            "=" * 68,
            "",
            "--- Output Files ---",
            f"  Main log        : {logs['main']}",
            f"  Geom detail     : {logs['geom']}",
        ]
        if run_p2:
            summary.append(f"  Multipart CSV   : {logs['mp_csv']}")
        if run_p3:
            summary.append(f"  FindIdent CSV   : {logs['fi_csv']}")
        if run_p4:
            summary.append(f"  Overlap grp CSV : {logs['ov_grp']}")
            summary.append(f"  Overlap det CSV : {logs['ov_det']}")
        summary.append(f"  Scratch GDB     : {scratch_gdb}")

        for line in summary:
            wl2(logs, line)

        # Validation_Log — Phase 5 rows only written when Phase 5 ran
        vl = [
            ("CP-01","Input records",float(count_raw),"INFO","Parcels_Raw"),
            ("CP-02","Final output records",float(final_count),"INFO",
             "Parcels_Cleaned"),
            ("CP-03","Net records removed",float(net_removed),"INFO",
             f"{100*net_removed/max(count_raw,1):.2f}%"),
        ]
        if p5_result:
            vl += [
                ("CP-04","Records with any flag",
                 float(p5_result["flagged_total"]),
                 "WARNING" if p5_result["flagged_total"]>0 else "OK",
                 f"{100*p5_result['flagged_total']/max(final_count,1):.2f}%"),
                ("CP-05","NULL_LUC",float(p5_result["null_luc"]),
                 "WARNING" if p5_result["null_luc"]>0 else "OK",
                 "Routes to NULL (OtherAcres residual)"),
                ("CP-06","BLANK_LUC",float(p5_result["blank_luc"]),
                 "WARNING" if p5_result["blank_luc"]>0 else "OK",
                 "Routes to NULL (OtherAcres residual)"),
                ("CP-07","PARSE_ERROR",float(p5_result["parse_error"]),
                 "WARNING" if p5_result["parse_error"]>0 else "OK",
                 "LUC_Code=-1"),
                ("CP-08","ZERO_AREA geodesic",float(p5_result["zero_area"]),
                 "WARNING" if p5_result["zero_area"]>0 else "OK",
                 "Contributes 0 acres"),
                ("CP-09","NULL_PARCEL_ID",float(p5_result["null_id"]),
                 "WARNING" if p5_result["null_id"]>0 else "OK",""),
                ("CP-10","Total Land_Acres geodesic",
                 float(p5_result["total_acres"]),"INFO","US Survey Acres"),
            ]
        write_validation_log(gdb_path, run_id, vl, logs["main"])
        wl(logs["main"],"Validation_Log updated.")

        arcpy.AddMessage(
            f"CleanParcels complete. "
            f"Input: {count_raw:,}  Output: {final_count:,}  "
            f"Removed: {net_removed:,}  Run ID: {run_id}")
        return

    def postExecute(self, parameters):
        return
