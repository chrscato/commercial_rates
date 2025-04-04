# Transparency in Coverage - Commercial Rates Extraction

This project parses Machine Readable Files (MRFs) from health insurers to extract commercial negotiated rates for CPT codes.

## Key Features
- Efficient parsing using PyArrow and DuckDB
- Filter by CPT codes
- Outputs clean CSVs for repricing and analytics

## Usage
1. Install requirements: `pip install -r requirements.txt`
2. Run the sample parser:
```bash
python scripts/parse_mrf_sample.py
```

## Folder Structure
See docs/project_setup_guidelines.md for full breakdown.

## Project Structure

```
transparency_data/
├── data/
│   ├── raw/                  # Raw downloaded JSONs (compressed)
│   ├── processed/            # Clean CSV or Parquet outputs
│   └── index_cache/          # Metadata from index.json files
├── scripts/                  # Processing and analysis scripts
├── notebooks/               # Jupyter notebooks for exploration
└── docs/                    # Project documentation
```

## Setup

1. Clone this repository
2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Copy `.env.example` to `.env` and fill in any required credentials

## Documentation

Detailed documentation can be found in the `docs/` directory:
- `startup_goal.md`: Project objectives and goals
- `prior_knowledge_attack_plan.md`: Analysis strategy
- `project_setup_guidelines.md`: Development setup instructions
- `ai_agent_knowledge_base.md`: AI-assisted analysis guidelines

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

[Add your chosen license here] 