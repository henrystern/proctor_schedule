# Proctor Schedule

Convert the proctor schedule excel sheet into importable calendar events.

## Project structure

```
│   LICENSE                               # MIT license
│   pyproject.toml                        # Package metadata and configuration
│   README.md                             # Brief documentation
├───data
│   ├───interim                           # Interim data that has been transformed
│   ├───processed                         # The final, canonical datasets
│   └───raw                               # The original, immutable data dump
└───src
    └───proctor_schedule                  # Python package for this project
            __init__.py                   # Makes package installable
            config.py                     # Configuration parameters
            make_calendar.py              # Script to generate calendars
```


## Python environment

The python environment is managed by [uv](https://docs.astral.sh/uv/getting-started/installation/).

Once uv is installed, create the environment by running:

```bash
uv sync
```

Then activate the environment with:

```bash
source .venv/bin/activate
```

If you are using vscode, set your python interpretor to `.venv/bin/python`.

## Usage

To create a calendar, first store the raw excel sheet in `data/raw` and then run the command `make schedules`. 

The script will first prompt you to select the raw file to use and will then output a complete calendar to `data/interim` and a calendar for each proctor to `data/processed`. 

The output files will be prefixed with the year and month of the earliest exam in the raw excel sheet. 

## Troubleshooting

Slight variations in format between the schedules may break the script. The canonical format is the one used in the 2025-10 schedule. If you are getting `column does not exist` errors, change the column names in your file to match the 2025-10 schedule.

Another common error is incorrect date formatting. If you're getting `could not parse date` errors check that excel has parsed every exam date, start time and end time as a date rather than a string.