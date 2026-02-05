"""Create an ICS calendar from an excel proctoring schedule."""

import argparse
from pathlib import Path
from typing import List
from uuid import uuid4

from icalendar import Calendar, Event
from loguru import logger
import polars as pl
import polars.selectors as cs

from proctor_schedule.config import (
    INTERIM_DATA_DIR,
    LOGS_DIR,
    PROCESSED_DATA_DIR,
    RAW_DATA_DIR,
)


def main(schedule_file: Path, start_offset_mins: int):
    """Create an ICS calendar from an excel proctoring schedule."""
    sched = (
        pl.read_excel(schedule_file, read_options=dict(header_row=2))
        .pipe(clean_proctor_schedule)
        .with_columns(
            pl.col("Start time") - pl.duration(minutes=start_offset_mins)
        )
        .pipe(
            lambda df: df.with_columns(
                pl.Series("Event", create_events(df), pl.Object)
            )
        )
    )

    schedule_period = sched.select(
        pl.col("Date").min().dt.strftime("%Y-%m")
    ).item()

    check_for_double_bookings(sched)

    cal = Calendar()
    [cal.add_component(e) for e in sched["Event"]]
    with (INTERIM_DATA_DIR / f"{schedule_period}_proctoring.ics").open(
        "wb"
    ) as f:
        f.write(cal.to_ical())

    # Create a different calendar for each proctor's events
    sched = sched.explode("Proctor")
    for proctor, data in sched.group_by("Proctor"):
        cal = Calendar()
        [cal.add_component(event) for event in data["Event"]]
        with (
            PROCESSED_DATA_DIR
            / f"{schedule_period}_{proctor[0]}_proctoring.ics"
        ).open("wb") as f:
            f.write(cal.to_ical())

    logger.success(
        f"Created ICS calendars for {schedule_period} from {schedule_file.name}"
    )


def clean_proctor_schedule(sched: pl.DataFrame):
    """Clean the excel sheet."""
    df = (
        sched.rename(lambda x: x.capitalize())
        .unpivot(
            cs.starts_with("Proctor"),
            index=~cs.starts_with("Proctor"),
            value_name="Proctor",
        )
        .drop("variable")
        .drop_nulls("Proctor")
        # This fill is imperfect especially around make-up exams
        .fill_null(strategy="forward")
        # Convert duplicate rows into one row per event with a list column of proctors
        .group_by(~cs.by_name("Proctor"))
        .agg(pl.col("Proctor").unique())
        .with_columns(
            # Need to combine Date and time columns as some times are 1899.
            pl.col("Date")
            .dt.combine(pl.col(f"{t} time").dt.time(), "ms")
            .dt.replace_time_zone("America/Toronto")
            .alias(f"{t} time")
            for t in ("Start", "End")
        )
    )

    return df


def create_events(sched: pl.DataFrame):
    """Create a list of icalendar events from the cleaned dataframe of event information."""
    abbreviations = (
        pl.read_csv(INTERIM_DATA_DIR / "building_abbreviations.csv")
        .with_columns(pl.concat_str("Building", pl.lit(": "), "Address"))
        .sort(pl.col("Abbreviation").str.len_chars(), descending=True)
    )
    events = []
    for row in sched.iter_rows(named=True):
        building = row["Location"].split("-")[0]
        for abbrv_row in abbreviations.iter_rows(named=True):
            building = building.replace(
                abbrv_row["Abbreviation"], abbrv_row["Building"]
            )
        event = Event()
        event.add("uid", str(uuid4()))
        event.add("summary", "Proctoring")
        event.add(
            "description",
            f"{row['Subject']} {row['Course']}-{row['Section']} for {row['Instructor']}, {row['Students enrolled']} students\nProctors: {', '.join(row['Proctor'])}\nBuilding: {building}"
            if row["Subject"] != "Make-up Exam"
            else f"Make-up exam\nProctors: {', '.join(row['Proctor'])}",
        )
        event.add("location", row["Location"])
        event.add("dtstart", row["Start time"])
        event.add("dtend", row["End time"])
        events.append(event)
    return events


def check_for_double_bookings(sched: pl.DataFrame):
    """Check if any proctors are double booked in the schedule."""
    appointments = sched.explode("Proctor")
    double_bookings = appointments.join_where(
        appointments,
        # The same proctor is assigned an appointment that:
        pl.col("Proctor") == pl.col("Proctor_right"),
        # Starts before another appointment starts
        (pl.col("Start time") < pl.col("Start time_right")),
        # Ends after that other appointment starts
        (pl.col("End time") > pl.col("Start time_right")),
        # and is not for the same location
        pl.col("Location") != pl.col("Location_right"),
    )

    for row in double_bookings.iter_rows(named=True):
        logger.warning(
            f"""
            {row["Proctor"]} is double-booked on {row["Date"]}:
                {row["Course"]}-{row["Section"]}: assigned from {row["Start time"].time()} to {row["End time"].time()}.
                {row["Course_right"]}-{row["Section_right"]}: assigned from {row["Start time_right"].time()} to {row["End time_right"].time()}.
            """
        )


def prompt_for_file(files: List[str]):
    """Prompt the user to select from a list of options."""
    print("Select a file to convert to ICS:")
    for i, file in enumerate(files):
        print(f"{i + 1}. {file}")

    while True:
        try:
            choice = int(input("Enter the number of the file: "))
            if 1 <= choice <= len(files):
                return files[choice - 1]
            else:
                print("Invalid number. Try again.")
        except ValueError:
            print("Please enter a valid number.")


if __name__ == "__main__":
    logger.add(LOGS_DIR / "dataset.log")

    schedules = [f.name for f in RAW_DATA_DIR.glob("*.xlsx")]
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--schedule",
        help="Which schedule to convert to ICS. The raw schedule must be an xlsx file in 'data/raw'",
        choices=schedules,
    )
    parser.add_argument(
        "--start-offset",
        help="How many minutes to subtract from the start of the exam for the event start.",
        default=30,
    )
    parsed_args = parser.parse_args()
    if parsed_args.schedule:
        schedule_file = RAW_DATA_DIR / parsed_args.schedule
    else:
        schedule_file = RAW_DATA_DIR / prompt_for_file(schedules)

    main(schedule_file, parsed_args.start_offset)
