"""Generate a schedule by assigning exams to available proctors according to their hours."""

import argparse
from typing import Dict

from loguru import logger
import numpy as np
import polars as pl

from proctor_schedule.config import LOGS_DIR, RAW_DATA_DIR
from proctor_schedule.make_calendar import clean_proctor_schedule


def make_schedule(
    exams: pl.DataFrame,
    proctor_timetable: pl.DataFrame,
    proctor_assignments: pl.DataFrame,
):
    exam_assignments = {}
    for exam in exams.select(
        "ID", "Day", "Start time", "End time", "Required proctors"
    ).iter_rows(named=True):
        exam_duration = (
            exam["End time"] - exam["Start time"]
        ).total_seconds() / 3600
        available_proctors = get_available_proctors(
            exams,
            proctor_timetable,
            proctor_assignments,
            exam_duration,
            exam_assignments,
            exam,
        )
        choices = available_proctors.sort("Utilization")
        if choices.height < int(exam["Required proctors"]):
            logger.warning(f"Couldn't assign enough proctors to {exam}.")
            exam["Required proctors"] = choices.height
        chosen_proctors = (
            choices.head(int(exam["Required proctors"]))
            .get_column("Name")
            .to_list()
        )
        proctor_assignments = proctor_assignments.with_columns(
            pl.when(pl.col("Name").is_in(chosen_proctors))
            .then(pl.col("Assigned") + exam_duration)
            .otherwise(pl.col("Assigned")),
            Utilization=pl.col("Assigned") / pl.col("Hours"),
        )
        exam_assignments[exam["ID"]] = chosen_proctors

    exams = exams.with_columns(
        pl.col("ID")
        .replace_strict(exam_assignments, return_dtype=pl.List(pl.String))
        .alias("Proctors")
    )
    exams.explode("Proctors").filter(pl.col("Proctors").str.contains("Stern"))

    exams.with_columns(
        (pl.col("End time") - pl.col("Start time"))
        .dt.total_hours()
        .alias("Duration")
    ).explode("Proctors").group_by("Proctors").agg(
        pl.col("Duration").sum()
    ).sort("Duration", descending=True).to_numpy()


def get_available_proctors(
    exams: pl.DataFrame,
    proctor_timetable: pl.DataFrame,
    proctor_assignments: pl.DataFrame,
    exam_duration: float,
    exam_assignments: Dict,
    exam: Dict,
):
    unavailable_proctors = get_unavailable_proctors(proctor_timetable, exam)
    already_assigned_proctors = get_assigned_proctors(
        exams, exam_assignments, exam
    )
    available_proctors = proctor_assignments.filter(
        ~pl.col("Name").is_in(unavailable_proctors),
        ~pl.col("Name").is_in(already_assigned_proctors),
        # Has enough hours remaining to be assigned to this exam
        (pl.col("Hours") - pl.col("Assigned")) >= exam_duration,
    )

    return available_proctors


def get_assigned_proctors(
    exams: pl.DataFrame,
    exam_assignments: Dict,
    exam: Dict,
):
    already_assigned_proctors = [
        proctor
        for exam_id in exams.filter(
            pl.col("ID") != exam["ID"],
            pl.col("Start time").is_between(
                exam["Start time"], exam["End time"]
            )
            | pl.col("End time").is_between(
                exam["Start time"], exam["End time"]
            ),
        )["ID"]
        .unique()
        .to_list()
        for proctor in exam_assignments.get(exam_id, [])
    ]

    return already_assigned_proctors


def get_unavailable_proctors(proctor_timetable: pl.DataFrame, exam: Dict):
    unavailable_proctors = (
        proctor_timetable.filter(
            pl.col("Day") == exam["Day"],
            pl.col("Time").is_between(
                exam["Start time"].time(), exam["End time"].time()
            ),
        )
        .filter(pl.col("Busy").any().over("Name"))
        .select(pl.col("Name").unique())
        .to_series()
        .to_list()
    )

    return unavailable_proctors


if __name__ == "__main__":
    logger.add(LOGS_DIR / "dataset.log")

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    available_terms = [f.name for f in RAW_DATA_DIR.iterdir() if f.is_dir()]
    parser.add_argument(
        "--term",
        help="The term to check. Must be a folder in RAW_DATA_DIR with `Proctor Assignments.xlsx` and `*.Proctoring Schedule.xlsx` files.",
        choices=available_terms,
    )
    parser.add_argument(
        "--students-per-proctor",
        help="The maximum number of students per proctor.",
        default=40,
    )
    parser.add_argument(
        "--proctors-per-exam",
        help="The minimum number of proctors per exam.",
        default=2,
    )
    parsed_args = parser.parse_args()
    term_dir = RAW_DATA_DIR / parsed_args.term

    proctors_per_exam = 2
    students_per_proctor = 30
    exams = (
        pl.read_excel(
            term_dir / "2026-03 - Midterm II Proctoring Schedule.xlsx",
            read_options=dict(header_row=2),
        )
        .pipe(clean_proctor_schedule)
        .with_columns(pl.col("Start time") - pl.duration(minutes=30))
        .drop("Proctor")
        .with_columns(
            pl.max_horizontal(
                (pl.col("Students enrolled") / students_per_proctor).ceil(),
                proctors_per_exam,
            ).alias("Required proctors"),
            pl.col("Day").replace(
                {
                    "Mon": "Monday",
                    "Tue": "Tuesday",
                    "Wed": "Wednesday",
                    "Thur": "Thursday",
                    "Fri": "Friday",
                    "Sat": "Saturday",
                    "Sun": "Sunday",
                }
            ),
            pl.row_index("ID"),
        )
    )
    proctor_timetable_sheets = pl.read_excel(
        term_dir / "Proctor Timetable.xlsx",
        sheet_id=0,
        read_options=dict(dtypes="string"),
    )
    proctor_timetable = pl.concat(
        [
            df.with_columns(Day=pl.lit(k.title()))
            for k, df in proctor_timetable_sheets.items()
        ],
    )
    proctor_timetable = proctor_timetable.unpivot(
        index=["Name", "Day"], variable_name="Time", value_name="Busy"
    ).with_columns(
        pl.col("Busy").is_not_null(), pl.col("Time").str.to_time("%I:%M:%S %p")
    )
    proctor_assignments = (
        pl.read_excel(term_dir / "Proctor Assignments.xlsx")
        .rename({"Student": "Name", "Proctor hours": "Hours"})
        .select("Name", pl.col("Hours").cast(pl.Float64))
        .with_columns(
            Assigned=pl.lit(0, pl.Float64),
            Utilization=pl.lit(0, pl.Float64),
        )
        .filter(pl.col("Hours") != 0)
    )

    make_schedule(
        exams=exams,
        proctor_timetable=proctor_timetable,
        proctor_assignments=proctor_assignments,
    )
