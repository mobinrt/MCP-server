import pandas as pd
import unicodedata

# ---------------------------
# Helper functions
# ---------------------------

def strip_bom(val: str) -> str:
    if val and isinstance(val, str):
        return val.replace("\ufeff", "")
    return val


def normalize_text(val: str) -> str:
    """Normalize unicode text (Persian/Arabic friendly)."""
    if pd.isna(val):
        return val
    val = str(val)
    # Normalize Unicode (fix half-space, Arabic chars, etc.)
    val = unicodedata.normalize("NFKC", val)
    # Remove invisible spaces
    val = val.replace("\xa0", " ").replace("\u200b", "")
    # Replace smart quotes/dashes
    val = val.replace("“", '"').replace("”", '"').replace("’", "'")
    val = val.replace("–", "-").replace("—", "-")
    # Strip & collapse spaces
    val = " ".join(val.strip().split())
    return val

def normalize_value(val, skip_keys=None, key=None):
    """Normalize individual cell values."""
    if skip_keys and key in skip_keys:
        return val
    if pd.isna(val) or str(val).strip() == "#NAME?":
        return None
    if isinstance(val, (int, float, bool)):
        return val
    return strip_bom(normalize_text(val))

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column headers (strip spaces, BOM, unicode cleanup)."""
    cleaned_cols = []
    for col in df.columns:
        new_col = strip_bom(str(col).strip())
        new_col = str(col).strip().lstrip("\ufeff")  
        new_col = unicodedata.normalize("NFKC", new_col)
        cleaned_cols.append(new_col)
    df.columns = cleaned_cols
    return df

# ---------------------------
# Load CSV
# ---------------------------

input_file = "civil_places.csv"
output_file = "fixed_civil_places.csv"

# Load with BOM-safe option
df = pd.read_csv(input_file, encoding="utf-8-sig")

# Normalize headers
df = normalize_columns(df)

# Normalize all columns except some keys
skip_keys = {"map_link", "link", "url"}  # URLs must not be changed
for col in df.columns:
    df[col] = df[col].apply(lambda v, c=col: normalize_value(v, skip_keys, c))

# ---------------------------
# Detect and drop rows with missing fields
# ---------------------------

# Fields we allow to be empty
ignore_empty_fields = {"phone_number"}

# Mask of rows that have missing values in critical fields
missing_mask = df.drop(columns=ignore_empty_fields, errors="ignore").isnull().any(axis=1)
missing_rows = df[missing_mask]

if not missing_rows.empty:
    print("Rows with missing fields detected (excluding ignored fields):")
    for idx, row in missing_rows.iterrows():
        empty_fields = [col for col in df.columns if pd.isna(row[col]) and col not in ignore_empty_fields]
        print(f" - Row {idx+1} has empty fields: {empty_fields}")

    # Drop them
    df = df[~missing_mask]

# ---------------------------
# Ensure external_id exists
# ---------------------------

if "external_id" not in df.columns:
    # Generate sequential external_id if missing
    df.index = range(1, len(df) + 1)
    df.index.name = "external_id"
    df.to_csv(output_file, index=True, encoding="utf-8-sig")
else:
    # Clean and enforce integer external_id
    df["external_id"] = pd.to_numeric(df["external_id"], errors="coerce").fillna(-1).astype(int)
    # Reorder so external_id is the first column
    cols = ["external_id"] + [c for c in df.columns if c != "external_id"]
    df = df[cols]
    df.to_csv(output_file, index=False, encoding="utf-8-sig")

print(f"Normalized CSV saved to {output_file} (rows with empty fields removed, ignoring {ignore_empty_fields})")
