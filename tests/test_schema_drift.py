from dagster_project.checks.schema_drift import check_missing_columns, check_type_mismatches, check_extra_columns


def test_check_missing_columns_none_missing():
    actual = {"employee_id": "VARCHAR", "name": "VARCHAR"}
    expected = {"employee_id": "VARCHAR", "name": "VARCHAR"}
    assert check_missing_columns(actual, expected) == []


def test_check_missing_columns_some_missing():
    actual = {"employee_id": "VARCHAR"}
    expected = {"employee_id": "VARCHAR", "name": "VARCHAR", "email": "VARCHAR"}
    result = check_missing_columns(actual, expected)
    assert sorted(result) == ["email", "name"]


def test_check_type_mismatches_none():
    actual = {"employee_id": "VARCHAR", "amount": "DOUBLE"}
    expected = {"employee_id": "VARCHAR", "amount": "DOUBLE"}
    assert check_type_mismatches(actual, expected) == {}


def test_check_type_mismatches_found():
    actual = {"employee_id": "VARCHAR", "amount": "VARCHAR"}
    expected = {"employee_id": "VARCHAR", "amount": "DOUBLE"}
    result = check_type_mismatches(actual, expected)
    assert result == {"amount": {"expected": "DOUBLE", "actual": "VARCHAR"}}


def test_check_type_mismatches_skips_missing():
    actual = {"employee_id": "VARCHAR"}
    expected = {"employee_id": "VARCHAR", "amount": "DOUBLE"}
    result = check_type_mismatches(actual, expected)
    assert result == {}


def test_check_extra_columns_none():
    actual = {"employee_id": "VARCHAR", "name": "VARCHAR"}
    expected = {"employee_id": "VARCHAR", "name": "VARCHAR"}
    assert check_extra_columns(actual, expected) == []


def test_check_extra_columns_found():
    actual = {"employee_id": "VARCHAR", "name": "VARCHAR", "bonus": "DOUBLE"}
    expected = {"employee_id": "VARCHAR", "name": "VARCHAR"}
    assert check_extra_columns(actual, expected) == ["bonus"]
