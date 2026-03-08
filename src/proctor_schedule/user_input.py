"""Functions for handling user input to scripts."""

from typing import List


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
