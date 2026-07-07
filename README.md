# Parcel_Data_Cleaning_Python_Toolbox

ParCL: A Python ArcGIS Pro Toolbox for Automated Parcel Data Topology Repair &amp; Attribute Standardization

## Clean Parcels Toolbox
## Overview
The **Clean Parcels** tool is designed to clean raw parcel data (`Parcels_Raw`) and output a finalized, production-ready `Parcels_Cleaned` dataset. It seamlessly integrates geometry cleaning—including multipart explosion, geometric deduplication, and overlap-based deduplication—with attribute cleaning—such as Land Use Code (LUC) parsing, geodesic area calculation, and quality flagging—into a single unified pipeline. Users can easily control which processing phases execute via parameter checkboxes in the tool interface.

---

## Processing Sequence
The tool processes the data through a sequential pipeline of discrete phases:

* **Phase 1:** Copies `Parcels_Raw` to the `Parcels_Cleaned` dataset. This phase always runs unless `Parcels_Cleaned` already exists and the *Force Fresh Copy* checkbox is unchecked (which allows for rerunning individual phases efficiently).
* **Phase 1b:** Performs a *Repair Geometry* operation on the copied dataset.
* **Phase 2 (Optional):** Explodes multipart polygons into singlepart features.
* **Phase 3 (Optional):** Executes geometric deduplication utilizing a *FindIdentical* operation. This geometric deduplication is run statewide across all counties simultaneously.
* **Phase 4 (Optional):** Executes Intersection over Union (IoU) overlap deduplication. This phase runs county-by-county utilizing parallel processing (OGR workers), mapping graph-based connected components for groups of 3 or more mutually overlapping records.
* **Phase 4b:** Performs cross-county boundary IoU overlap detection.
* **Phase 4c:** Resolves partial overlaps at county boundaries using an erase-based overlap resolution operation.
* **Phase 5 (Optional):** Conducts attribute cleaning and generates the geodesic `Land_Acres` calculation. It flags data anomalies such as null/blank LUC codes, parsing errors, and zero or negative areas.

---

## Input Parameters

| Parameter | Type | Default Value | Description |
| :--- | :--- | :--- | :--- |
| **Project Geodatabase** | Workspace | *Required* | The primary working environment (File Geodatabase). |
| **Log Folder** | Folder | *Required* | Destination for runtime text logs and CSV reports. |
| **Scratch GDB** | Workspace | *Required* | Destination for intermediate processing and recovery datasets. |
| **XY Tolerance** | String | `"10 Feet"` | Tolerance for spatial operations. |
| **IoU Threshold** | Double | `0.85` | The threshold required to trigger overlap processing. |
| **Run Phase 2 - Multiparts** | Boolean | `True` | Enables multipart explosion. |
| **Run Phase 3 - FindIdentical** | Boolean | `True` | Enables geometric deduplication. |
| **Run Phase 4 - Overlap** | Boolean | `True` | Enables overlap detection. |
| **Force Fresh Copy** | Boolean | `False` | Forces an overwrite of the existing target dataset. |
| **Run Phase 5 - Attr Clean** | Boolean | `True` | Enables attribute cleaning. |

---

## Outputs

### Main Outputs
* **`Parcels_Cleaned`**: The finalized output feature class, written to the `raw_inputs` dataset within the Project Geodatabase. Geometry duplicates are hard-deleted from this output during processing.
* **`Validation_Log`**: A table in the Geodatabase that tracks script runtimes, status checks, descriptions, and numerical statistics for auditing.

### Scratch Outputs (Preserved for Audit/Recovery)
Original records that are hard-deleted from the cleaned dataset are safely preserved in the **Scratch GDB** with unique run identifiers:
* `Deleted_Multipart_{RunID}`
* `Deleted_FindIdentical_{RunID}`
* `Deleted_Overlap_{RunID}`

---

## Logging & Reporting
Logs are saved to the **Log Folder** in a unique directory for each execution, designated by a `LI_YYYYMMDD_HHMMSS` timestamp.

* **`CleanParcels_Log_{RunID}.txt`**: The primary summary log tracking all execution phases.
* **`CleanParcels_GeomDetail_{RunID}.txt`**: A granular log capturing detailed operations at the county and LUC level.
* **`Multipart_Detail_{RunID}.csv`**: Contains one row for each exploded multipart feature.
* **`FindIdentical_Groups_{RunID}.csv`**: Contains one row per feature sequence group for identical records.
* **`Overlap_Groups_{RunID}.csv`**: Contains one row per identified overlap group.
* **`Overlap_Detail_{RunID}.csv`**: Contains one row detailing each record deleted due to overlap rules.

---

## Methodology & Key Design Decisions

### 1. Keeper Selection Logic
During deduplication (Identical and Overlap phases), the tool must choose a "keeper" record. It resolves ties using the following deterministic hierarchy:
1.  **Valid StateLUC Code:** Prioritizes keeping the record with a valid parsed `StateLUC` code.
2.  **Geodesic Area:** If no valid LUC exists, or if there is a tie, it prefers the feature with the largest geodesic area.
3.  **Lowest ObjectID:** Any remaining ties are broken by keeping the lowest `ObjectID`.

### 2. Geodesic Area Calculation
To ensure structural accuracy over large statewide areas, `Land_Acres` is calculated via geodesic tools (`shape.getArea('GEODESIC','ACRES')`) in US Survey Acres, replacing older planar calculation methods (`Shape_Area/43560`) which introduce distortion over large spatial scales.

### 3. LUC Parsing Flexibility
The tool handles multiple formatting variations for Land Use Codes across different counties. It intelligently strips descriptive text and handles standard delimiters to extract clean integer codes:
* `"510: Res-Single Family"` ➔ **`510`** (Colon separated)
* `"510- Res-Single Family"` ➔ **`510`** (Dash separated)
* `"999"` ➔ **`999`** (Bare integer strings)

### 4. Parallel Processing & Memory Thresholds
Overlap detection heavily relies on Python's `multiprocessing` and `concurrent.futures`. 
* **Worker Architecture:** To bypass ArcGIS Pro Windows execution limits, worker functions are housed in a companion `.py` file (`CleanParcels_workers.py`) since `.pyt` (Python Toolbox) files are not directly importable by child processes.
* **Memory Optimization:** Counties with fewer than 50,000 records process incredibly fast using an in-memory workspace (`in_memory` or `memory`), while larger counties fall back to disk-scratch processing to preserve stability and prevent out-of-memory crashes.
