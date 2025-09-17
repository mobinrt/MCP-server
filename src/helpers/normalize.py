import pandas as pd
import unicodedata

# ---------------------------
# Helper functions
# ---------------------------

def normalize_text(val):
    if pd.isna(val):
        return val
    val = str(val)
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
    if skip_keys and key in skip_keys:
        return val
    if pd.isna(val) or str(val).strip() == "#NAME?":
        return None
    if isinstance(val, (int, float, bool)):
        return val
    return normalize_text(val)
# ---------------------------
# Load CSV
# ---------------------------

input_file = "civil_places.csv"
output_file = "fixed_civil_places.csv"

df = pd.read_csv(input_file, encoding="utf-8")

# Normalize all columns except 'map_link'
skip_keys = {"map_link"}
for col in df.columns:
    df[col] = df[col].apply(lambda v, c=col: normalize_value(v, skip_keys, c))

# Create a new index column named 'external_id'
df.index += 1  # start index from 1 (optional)
df.index.name = "external_id"

# Save CSV with the new index column
df.to_csv(output_file, index=True, encoding="utf-8-sig")

print(f"Normalized CSV saved to {output_file} with new index column 'external_id'")
