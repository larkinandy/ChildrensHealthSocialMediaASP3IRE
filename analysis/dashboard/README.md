# Children's Environmental Health Topics Dashboard

This folder contains a Streamlit dashboard prepared for public deployment through Streamlit Community Cloud.

## Entry Point

Use this file as the Streamlit app entry point:

```text
analysis/dashboard/app.py
```

## Data

The dashboard reads preprocessed parquet files from:

```text
analysis/dashboard/data
```

The Portland overview uses the prepared v007 dashboard packet files in:

```text
analysis/dashboard/data/portland
```

The parquet files are aggregate dashboard inputs. They do not include raw tweet text, post IDs, author IDs, or raw post/author counts.

## Local Run

From the repository root:

```powershell
uv --cache-dir .uv-cache run --with-requirements analysis\dashboard\requirements.txt streamlit run analysis\dashboard\app.py
```

Or with an existing Python environment:

```powershell
pip install -r analysis\dashboard\requirements.txt
streamlit run analysis\dashboard\app.py
```

## Notes

- The overview page uses Portland v007 prepared parquet data.
- Event overlays default to local-impact events only.
- The ontology page still uses prototype ontology/intersection parquet files until the final ontology-specific production tables are promoted.
