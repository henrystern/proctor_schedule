"""Check how many hours each proctor has been assigned and compare to their contracted assignments."""

import argparse
from pathlib import Path
from typing import List

import polars as pl
import polars_ds as pds

from proctor_schedule.config import RAW_DATA_DIR
from proctor_schedule.make_calendar import clean_proctor_schedule
from proctor_schedule.user_input import prompt_for_file


def read_schedules(
    schedule_files: List[Path],
    start_offset_mins: int,
):
    schedule = (
        pl.concat(
            [
                pl.read_excel(schedule_file, read_options=dict(header_row=2))
                .pipe(clean_proctor_schedule)
                .with_columns(
                    pl.col("Start time")
                    - pl.duration(minutes=start_offset_mins)
                )
                for schedule_file in schedule_files
            ],
            how="diagonal_relaxed",
        )
        .with_columns(Duration=pl.col("End time") - pl.col("Start time"))
        .explode("Proctor")
    )
    return schedule


def read_assignments(assignment_file: Path):
    assignments = (
        pl.read_excel(assignment_file)
        .rename({"Student": "Name", "Proctor hours": "Hours"})
        .select("Name", pl.col("Hours").cast(pl.Float64))
        .with_columns(
            Assigned=pl.lit(0, pl.Float64),
            Utilization=pl.lit(0, pl.Float64),
        )
        .filter(pl.col("Hours") != 0)
    )

    return assignments


def fuzzy_match_names(
    proctor_names: pl.DataFrame, assignment_names: pl.DataFrame
):
    scores = proctor_names.join(assignment_names, how="cross").with_columns(
        Score=pds.str_fuzz(pl.col("Proctor"), pl.col("Name"))
    )

    matches = (
        scores.sort("Score", descending=True)
        .group_by("Proctor", maintain_order=True)
        .first()
    )

    with pl.Config(tbl_rows=50):
        print(matches.with_row_index())
    incorrect = input("Enter incorrect matches: ").split()
    if incorrect:
        incorrect_matches = matches.with_row_index().filter(
            pl.col("index").cast(pl.String).is_in(incorrect)
        )
        fixed = fuzzy_match_names(
            proctor_names.filter(
                pl.col("Proctor").is_in(incorrect_matches["Proctor"].to_list())
            ),
            assignment_names.filter(
                ~pl.col("Name").is_in(incorrect_matches["Name"].to_list())
            ),
        )
        matches = pl.concat(
            [
                matches.filter(
                    ~pl.col("Proctor").is_in(
                        incorrect_matches["Proctor"].to_list()
                    )
                ),
                fixed,
            ]
        )

    return matches


def check_utilization(schedule: pl.DataFrame, assignments: pl.DataFrame):
    matches = fuzzy_match_names(
        proctor_names=schedule.select("Proctor").unique(),
        assignment_names=assignments.select("Name").unique(),
    )
    proctor_to_name = dict(zip(matches["Proctor"], matches["Name"]))
    assigned = (
        schedule.with_columns(
            pl.col("Proctor").replace_strict(proctor_to_name).alias("Name")
        )
        .group_by("Name")
        .agg(
            pl.col("Duration")
            .sum()
            .dt.total_hours(fractional=True)
            .alias("Assigned")
        )
    )
    utilization = assigned.join(
        assignments.select("Name", "Hours"), "Name", validate="1:1"
    ).with_columns(Utilization=pl.col("Assigned") / pl.col("Hours"))

    with pl.Config(tbl_rows=50):
        print(utilization.sort("Utilization"))
    return utilization.sort("Utilization")


if __name__ == "__main__":
    available_terms = [f.name for f in RAW_DATA_DIR.iterdir() if f.is_dir()]
    term = "2026-01"
    term_dir = RAW_DATA_DIR / term
    start_offset_mins = 30
    assignment_file = next(term_dir.glob("Proctor Assignments.xlsx"))
    schedule_files = [
        term_dir / f for f in term_dir.glob("*Proctoring Schedule.xlsx")
    ]

    schedule = read_schedules(schedule_files, start_offset_mins)
    assignments = read_assignments(assignment_file)

    utilization = check_utilization(schedule, assignments)
    utilization.sort("Assigned")

    schedule.with_columns(
        pl.col("Duration").dt.total_hours(fractional=True)
    ).write_clipboard()
