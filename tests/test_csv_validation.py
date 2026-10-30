import pytest

from app.csv_validation import CsvValidationError, validate_csv
from app.models import HospitalRow

MAX_ROWS = 20
MAX_BYTES = 10_000


def _validate(content: bytes | str, filename: str | None = "hospitals.csv", **overrides) -> list[HospitalRow]:
    if isinstance(content, str):
        content = content.encode("utf-8")
    kwargs = {"max_rows": MAX_ROWS, "max_bytes": MAX_BYTES, **overrides}
    return validate_csv(content, filename=filename, **kwargs)


def _reject(content: bytes | str, filename: str | None = "hospitals.csv", **overrides) -> CsvValidationError:
    with pytest.raises(CsvValidationError) as exc_info:
        _validate(content, filename, **overrides)
    return exc_info.value


# --- valid input --------------------------------------------------------------


def test_valid_csv_returns_rows() -> None:
    rows = _validate("name,address,phone\nCity Hospital,1 Main St,555-0100\nGeneral,2 Oak Ave,\n")

    assert rows == [
        HospitalRow(row_number=1, name="City Hospital", address="1 Main St", phone="555-0100"),
        HospitalRow(row_number=2, name="General", address="2 Oak Ave", phone=None),
    ]


def test_strips_utf8_bom() -> None:
    rows = _validate(b"\xef\xbb\xbfname,address,phone\nCity Hospital,1 Main St,555-0100\n")

    assert rows[0].name == "City Hospital"


def test_headers_are_case_and_whitespace_insensitive_and_any_order() -> None:
    rows = _validate(" Address , NAME ,Phone\n1 Main St,City Hospital,555-0100\n")

    assert rows[0].name == "City Hospital"
    assert rows[0].address == "1 Main St"


def test_phone_column_is_optional() -> None:
    rows = _validate("name,address\nCity Hospital,1 Main St\n")

    assert rows[0].phone is None


def test_trims_values_and_handles_quoted_commas() -> None:
    rows = _validate('name,address,phone\n  City Hospital , "1 Main St, Springfield" , 555-0100 \n')

    assert rows[0].name == "City Hospital"
    assert rows[0].address == "1 Main St, Springfield"
    assert rows[0].phone == "555-0100"


def test_skips_blank_lines() -> None:
    rows = _validate("name,address,phone\n\nCity Hospital,1 Main St,\n\n,,\nGeneral,2 Oak Ave,\n")

    assert [r.name for r in rows] == ["City Hospital", "General"]
    assert [r.row_number for r in rows] == [1, 2]


def test_handles_crlf_line_endings() -> None:
    rows = _validate("name,address,phone\r\nCity Hospital,1 Main St,555\r\n")

    assert rows[0].phone == "555"


def test_accepts_exactly_max_rows() -> None:
    body = "".join(f"H{i},Addr {i},\n" for i in range(MAX_ROWS))
    rows = _validate("name,address,phone\n" + body)

    assert len(rows) == MAX_ROWS


# --- file-level errors (400) --------------------------------------------------


@pytest.mark.parametrize("filename", ["hospitals.txt", "hospitals.xlsx", "hospitals", None])
def test_rejects_non_csv_filename(filename: str | None) -> None:
    err = _reject("name,address,phone\nA,B,\n", filename=filename)

    assert err.status_code == 400
    assert "csv" in err.message.lower()


def test_accepts_uppercase_csv_extension() -> None:
    assert _validate("name,address,phone\nA,B,\n", filename="HOSPITALS.CSV")


def test_rejects_file_over_size_limit() -> None:
    err = _reject("name,address,phone\n" + "A,B,\n" * 100, max_bytes=50)

    assert err.status_code == 400
    assert "size" in err.message.lower()


@pytest.mark.parametrize("content", [b"", b"   \n\n  "])
def test_rejects_empty_file(content: bytes) -> None:
    err = _reject(content)

    assert err.status_code == 400
    assert "empty" in err.message.lower()


def test_rejects_non_utf8() -> None:
    err = _reject("name,address,phone\nHôpital,1 Rue,\n".encode("latin-1"))

    assert err.status_code == 400
    assert "utf-8" in err.message.lower()


def test_rejects_header_only() -> None:
    err = _reject("name,address,phone\n")

    assert err.status_code == 400
    assert "no data rows" in err.message.lower()


@pytest.mark.parametrize(
    "header",
    ["name,phone", "address,phone", "hospital,address,phone", "name,address,phone,email", "name,name,address"],
)
def test_rejects_bad_headers(header: str) -> None:
    err = _reject(f"{header}\nA,B,C\n")

    assert err.status_code == 400
    assert "header" in err.message.lower()


def test_rejects_too_many_rows() -> None:
    body = "".join(f"H{i},Addr {i},\n" for i in range(MAX_ROWS + 1))
    err = _reject("name,address,phone\n" + body)

    assert err.status_code == 400
    assert str(MAX_ROWS) in err.message


def test_row_limit_is_configurable() -> None:
    err = _reject("name,address,phone\nA,B,\nC,D,\n", max_rows=1)

    assert err.status_code == 400


# --- row-level errors (422) ---------------------------------------------------


def test_reports_missing_required_fields_per_row() -> None:
    err = _reject("name,address,phone\nCity Hospital,1 Main St,\n,2 Oak Ave,\nGeneral,,\n")

    assert err.status_code == 422
    assert [(e.row, e.field) for e in err.errors] == [(2, "name"), (3, "address")]


def test_reports_all_row_errors_not_just_first() -> None:
    err = _reject("name,address,phone\n,A,\n,B,\n,C,\n")

    assert len(err.errors) == 3


def test_rejects_row_with_too_many_columns() -> None:
    err = _reject("name,address,phone\nCity Hospital,1 Main St,555,extra\n")

    assert err.status_code == 422
    assert err.errors[0].row == 1


def test_rejects_invalid_phone() -> None:
    err = _reject("name,address,phone\nCity Hospital,1 Main St,call me maybe\n")

    assert err.status_code == 422
    assert (err.errors[0].row, err.errors[0].field) == (1, "phone")


def test_rejects_overlong_field() -> None:
    err = _reject(f"name,address,phone\n{'x' * 256},1 Main St,\n")

    assert err.status_code == 422
    assert err.errors[0].field == "name"


def test_rejects_duplicate_rows_in_same_file() -> None:
    err = _reject("name,address,phone\nCity Hospital,1 Main St,\ncity hospital, 1 MAIN ST ,\n")

    assert err.status_code == 422
    assert err.errors[0].row == 2
    assert "duplicate" in err.errors[0].message.lower()
