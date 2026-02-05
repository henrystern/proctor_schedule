"""Extract building abbreviations, names and addresses from the UWO index."""

from bs4 import BeautifulSoup
import polars as pl

from proctor_schedule.config import INTERIM_DATA_DIR, RAW_DATA_DIR


def main():
    """Extract and store the abbreviations."""
    with (RAW_DATA_DIR / "building_abbreviations_index.htm").open(
        "r",
        encoding="utf-8",
    ) as file:
        soup = BeautifulSoup(file, "html.parser")

    extracted_data = extract_building_info(soup)
    extracted_data.write_csv(INTERIM_DATA_DIR / "building_abbreviations.csv")


def extract_building_info(soup: BeautifulSoup):
    """Extract the building abbreviations, names and addresses from the UWO index."""
    buildings_data = []
    entries = soup.find_all("div", class_="ui-accordion-content")
    for entry in entries:  # Gemini wrote this part.
        # Extract name and abbreviation from the left column
        left_col = entry.find("div", class_="left-2column")
        full_name = ""
        abbreviation = ""

        if left_col:
            # Look for strong tags that label the data
            for strong in left_col.find_all("strong"):
                label = strong.get_text(strip=True)
                value = strong.next_sibling
                if not isinstance(value, str):
                    continue
                if "Full Name:" in label:
                    full_name = value.strip()
                elif "Abbreviation:" in label:
                    abbreviation = value.strip()

        # Extract mailing address from the right column
        right_col = entry.find("div", class_="right-2column")
        address = ""
        if right_col:
            # The address starts after the "Mailing Address:" label if present
            # or is just the text content of the div
            raw_text = right_col.get_text(separator=" ", strip=True)
            address = raw_text.replace("Mailing Address:", "").strip()

        if abbreviation or full_name:
            buildings_data.append(
                {
                    "Abbreviation": abbreviation,
                    "Building": full_name,
                    "Address": address,
                }
            )
    return pl.DataFrame(buildings_data)


if __name__ == "__main__":
    main()
