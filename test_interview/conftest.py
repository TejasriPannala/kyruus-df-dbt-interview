import pytest
from dbt.cli.main import dbtRunner


def pytest_sessionstart(session):
    print("building DBT project")
    build_args = [
        "build",
        "--project-dir",
        "provider_pipeline",
        "--profiles-dir",
        "test_interview/dbt_profile",
    ]
    dbt = dbtRunner()
    try:
        dbtRunnerResult = dbt.invoke(build_args)
    except Exception as e:
        print(str(e))
        pytest.exit('Unable to build dbt project')
    for r in dbtRunnerResult.result:
        if r.status != 'success':
            pytest.exit('Unable to build dbt project')
