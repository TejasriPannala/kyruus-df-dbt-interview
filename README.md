# dbt-interview

## Overview
This assessment tests knowledge of the following areas
* git
* system setup
* python environment setup
* sql and dbt
* basic pandas usage

In some cases instructions are purposefully short, so you may need to research how to do what is asked if you do not already know.

The code will be evaluated on the following criteria, in order
of significance
* Correctness
* Readability and maintainability
* Code documentation, clarity and ease of use
* Extensibility
* Performance

Candidates will be expected to talk to and explain their
work in a follow up discussion

## Dependency Setup
You will need the following set up on your machine to complete the exercise. Suggest using the brew package manager for mac to install them. If you 
are on another system or use another package manager just make sure you have `git`, `poetry`, and `duckdb` installed and accessible via the path. 
* git and/or github CLI - `brew install git` and `brew install gh`
* [python 3.11](https://realpython.com/installing-python/) - suggest installing with homebrew via pyenv `brew install python`
* [pipx](https://pipx.pypa.io/stable/installation/) - required to install poetry the suggested way `brew install pipx` and `pipx ensurepath`
* [poetry](https://python-poetry.org/docs/#installing-with-pipx) - suggest following the instructions to install with pipx, `pipx install poetry`
* duckdb - `brew install duckdb`


## Project Setup

1. Clone the repo and create a virtual environment using poetry and install the dependencies with `poetry install`. 
All future steps will assume you have this environment active such as via `poetry shell`

2. Navigate to the dbt project `provider_pipeline` and run `dbt init` to set up your profile files, make sure to follow the prompt to setup the connection for duckDb

After that you can pause to make sure your the [dbt profile](https://docs.getdbt.com/docs/core/connect-data-platform/connection-profiles)
used by the project is pointing to duckdb.

NOTE - make sure your dbt profile points to `/tmp/dev.duckdb` to read/write so tests can pass


3. Run `dbt seed` to load the seed data. If at any time you delete your local duckDb instance you will need to re-run this command


## Exercises

Before starting the exercises create a new branch called `feature/exercises` 
and commit all subsequent code to that branch

For each exercise please make exactly 1 commit with a message describing the contents of the exercise. It is important to follow instructions precisely and use the table and column names provided as if you do not the automated evaluation will fail. 

For any DBT models they should build if `dbt run` is run for the `provider_pipeline` folder. `provider_count` has been provided as an example dbt model.

### 1. Provider Addresses
Write a new dbt model, `provider_address_agg` that combines the provider and address information in a way that the result is a table with the schema as defined 
in `provider_pipeline/models/models.yaml`. If a provider has no addresses there should
still be a row, but the addresses column should be an empty array. 


### 2.Provider Degrees
Write a new dbt model, `provider_types` that joins the providers with the degree type information to get 1 row per provider, following the schema in `provider_pipeline/models/models.yaml`. Each row should have the `ptui`
that corresponds to the degree of the lowest `rank`. 

Hint - the `degrees` data in the `providers` table is stored as a string, but needs to be treated
more like an array. 


### 3. Adding a Project Dependency and Querying with Pandas
Add pandas as a dependency to the project using poetry. Then write a python script to select all the rows from the `provider_address_agg` table and print them. Do this by creaing a new file, `query_duckdb.py` and a method in that file called `query_duckdb` 
that takes no arguments and returns the entire `provider_address_agg` table when run. 


### Wrap up
Once all exercise are complete please create a pull request and merge that into the `main` branch

