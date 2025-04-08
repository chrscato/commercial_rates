# Transparency Data Processing

This project processes transparency data files, supporting both provider-group and flat NPI structures.

## Project Structure

```
prod/
├── data/
│   ├── raw/                   # Input MRFs (if you cache them)
│   ├── processed/             # Output CSVs, Parquets, etc.
│
├── scripts/
│   ├── __init__.py
│   ├── cli.py                 # Command-line entry point
│   ├── detect_structure.py    # Determines format type (provider vs. direct NPI)
│   ├── parse_flat.py          # Parser for flat NPI structure
│   ├── parse_provider_map.py  # Parser for provider-group structure
│   ├── parse_router.py        # Chooses correct parser based on structure
│   ├── fetch_from_toc.py      # TOC crawler
│   └── utils.py               # Shared helpers (gzip, batching, etc.)
│
├── tests/
│   ├── test_parser_provider_map.py
│   ├── test_parser_flat.py
│
├── main.py                    # CLI dispatcher
├── requirements.txt
└── README.md
```

## Installation

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Run the main script with a URL:
```bash
python main.py --url <transparency_data_url> --output <output_directory>
```

## Testing

Run tests using pytest:
```bash
pytest tests/
``` 