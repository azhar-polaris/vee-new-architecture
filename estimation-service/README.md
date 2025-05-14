# Estimation Service

## Installation

To install the validation service, follow these steps:

1. Make sure you have **Poetry** installed. If not, follow the installation instructions from [Poetry's official website](https://python-poetry.org/docs/#installation).

2. Install the dependencies by running the following command:

   ```bash
   poetry install

2. To run the validation service locally, use the following command:

   ```bash
   poetry run uvicorn app.main:app --reload