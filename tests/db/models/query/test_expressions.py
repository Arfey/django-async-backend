"""
Port of Django's expressions/tests.py to our async backend.

Omitted categories:
- Tests using isolate_apps / dynamic schema.
- Tests using related-manager ops that our AsyncManager isn't wired into.
- Oracle / MySQL / SQLite specific tests (we target PostgreSQL).
"""

from __future__ import annotations

import datetime
import pickle
import re
import uuid
from copy import deepcopy
from decimal import Decimal
from unittest import mock

import pytest
from django.core.exceptions import FieldError
from django.db import DEFAULT_DB_ALIAS, DatabaseError, NotSupportedError
from django.db.models import (
    AutoField,
    Avg,
    BinaryField,
    BooleanField,
    Case,
    CharField,
    Count,
    DateField,
    DateTimeField,
    DecimalField,
    DurationField,
    Exists,
    Expression,
    ExpressionList,
    ExpressionWrapper,
    F,
    FloatField,
    Func,
    IntegerField,
    JSONField,
    Max,
    Min,
    OrderBy,
    OuterRef,
    PositiveIntegerField,
    Q,
    StdDev,
    Subquery,
    Sum,
    TimeField,
    UUIDField,
    Value,
    Variance,
    When,
)
from django.db.models.expressions import (
    Col,
    ColPairs,
    Combinable,
    CombinedExpression,
    NegatedExpression,
    OutputFieldIsNoneError,
    RawSQL,
    Ref,
)
from django.db.models.functions import (
    Coalesce,
    Concat,
    ExtractDay,
    Left,
    Length,
    Lower,
    Substr,
    TruncDate,
    Upper,
)
from expressions.models import (
    UUID,
    UUIDPK,
    Company,
    Employee,
    Experiment,
    JSONFieldModel,
    Manager,
    Number,
    RemoteEmployee,
    Result,
    SimulationRun,
    Text,
    Time,
)

from django_async_backend.db import async_connections

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _D:
    """Container for named fixture objects."""


def _features():
    return async_connections[DEFAULT_DB_ALIAS].features


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def basic_expressions_data(async_db):
    d = _D()
    d.example_inc = await Company.async_object.acreate(
        name="Example Inc.",
        num_employees=2300,
        num_chairs=5,
        ceo=await Employee.async_object.acreate(firstname="Joe", lastname="Smith", salary=10),
    )
    d.foobar_ltd = await Company.async_object.acreate(
        name="Foobar Ltd.",
        num_employees=3,
        num_chairs=4,
        based_in_eu=True,
        ceo=await Employee.async_object.acreate(firstname="Frank", lastname="Meyer", salary=20),
    )
    d.max = await Employee.async_object.acreate(firstname="Max", lastname="Mustermann", salary=30)
    d.gmbh = await Company.async_object.acreate(name="Test GmbH", num_employees=32, num_chairs=1, ceo=d.max)
    d.company_query = Company.async_object.values("name", "num_employees", "num_chairs").order_by(
        "name", "num_employees", "num_chairs"
    )
    return d


@pytest.fixture
async def iterable_lookup_data(async_db):
    d = _D()
    ceo = await Employee.async_object.acreate(firstname="Just", lastname="Doit", salary=30)
    d.c5020 = await Company.async_object.acreate(name="5020 Ltd", num_employees=50, num_chairs=20, ceo=ceo)
    d.c5040 = await Company.async_object.acreate(name="5040 Ltd", num_employees=50, num_chairs=40, ceo=ceo)
    d.c5050 = await Company.async_object.acreate(name="5050 Ltd", num_employees=50, num_chairs=50, ceo=ceo)
    d.c5060 = await Company.async_object.acreate(name="5060 Ltd", num_employees=50, num_chairs=60, ceo=ceo)
    d.c99300 = await Company.async_object.acreate(name="99300 Ltd", num_employees=99, num_chairs=300, ceo=ceo)
    return d


@pytest.fixture
async def numeric_data(async_db):
    await Number.async_object.acreate(integer=-1)
    await Number.async_object.acreate(integer=42)
    await Number.async_object.acreate(integer=1337)
    await Number.async_object.filter().aupdate(float=F("integer"))


@pytest.fixture
async def numeric_op_data(async_db):
    d = _D()
    d.n = await Number.async_object.acreate(integer=42, float=15.5)
    d.n1 = await Number.async_object.acreate(integer=-42, float=-15.5)
    return d


@pytest.fixture
async def time_delta_data(async_db):
    d = _D()
    d.sday = sday = datetime.date(2010, 6, 25)
    d.stime = stime = datetime.datetime(2010, 6, 25, 12, 15, 30, 747000)
    midnight = datetime.time(0)

    delta0 = datetime.timedelta(0)
    delta1 = datetime.timedelta(microseconds=253000)
    delta2 = datetime.timedelta(seconds=44)
    delta3 = datetime.timedelta(hours=21, minutes=8)
    delta4 = datetime.timedelta(days=10)
    delta5 = datetime.timedelta(days=90)

    d.deltas = []
    d.delays = []
    d.days_long = []

    # e0
    end = stime + delta0
    d.e0 = await Experiment.async_object.acreate(
        name="e0",
        assigned=sday,
        start=stime,
        end=end,
        completed=end.date(),
        estimated_time=delta0,
    )
    d.deltas.append(delta0)
    d.delays.append(d.e0.start - datetime.datetime.combine(d.e0.assigned, midnight))
    d.days_long.append(d.e0.completed - d.e0.assigned)

    # e1
    delay = datetime.timedelta(1)
    end = stime + delay + delta1
    e1 = await Experiment.async_object.acreate(
        name="e1",
        assigned=sday,
        start=stime + delay,
        end=end,
        completed=end.date(),
        estimated_time=delta1,
    )
    d.deltas.append(delta1)
    d.delays.append(e1.start - datetime.datetime.combine(e1.assigned, midnight))
    d.days_long.append(e1.completed - e1.assigned)

    # e2
    end = stime + delta2
    e2 = await Experiment.async_object.acreate(
        name="e2",
        assigned=sday - datetime.timedelta(3),
        start=stime,
        end=end,
        completed=end.date(),
        estimated_time=datetime.timedelta(hours=1),
    )
    d.deltas.append(delta2)
    d.delays.append(e2.start - datetime.datetime.combine(e2.assigned, midnight))
    d.days_long.append(e2.completed - e2.assigned)

    # e3
    delay = datetime.timedelta(4)
    end = stime + delay + delta3
    e3 = await Experiment.async_object.acreate(
        name="e3",
        assigned=sday,
        start=stime + delay,
        end=end,
        completed=end.date(),
        estimated_time=delta3,
    )
    d.deltas.append(delta3)
    d.delays.append(e3.start - datetime.datetime.combine(e3.assigned, midnight))
    d.days_long.append(e3.completed - e3.assigned)

    # e4
    end = stime + delta4
    e4 = await Experiment.async_object.acreate(
        name="e4",
        assigned=sday - datetime.timedelta(10),
        start=stime,
        end=end,
        completed=end.date(),
        estimated_time=delta4 - datetime.timedelta(1),
    )
    d.deltas.append(delta4)
    d.delays.append(e4.start - datetime.datetime.combine(e4.assigned, midnight))
    d.days_long.append(e4.completed - e4.assigned)

    # e5
    delay = datetime.timedelta(30)
    end = stime + delay + delta5
    e5 = await Experiment.async_object.acreate(
        name="e5",
        assigned=sday,
        start=stime + delay,
        end=end,
        completed=end.date(),
        estimated_time=delta5,
    )
    d.deltas.append(delta5)
    d.delays.append(e5.start - datetime.datetime.combine(e5.assigned, midnight))
    d.days_long.append(e5.completed - e5.assigned)

    d.expnames = [e.name async for e in Experiment.async_object.all()]
    return d


@pytest.fixture
async def negated_expression_data(async_db):
    d = _D()
    ceo = await Employee.async_object.acreate(firstname="Joe", lastname="Smith", salary=10)
    d.eu_company = await Company.async_object.acreate(
        name="Example Inc.",
        num_employees=2300,
        num_chairs=5,
        ceo=ceo,
        based_in_eu=True,
    )
    d.non_eu_company = await Company.async_object.acreate(
        name="Foobar Ltd.",
        num_employees=3,
        num_chairs=4,
        ceo=ceo,
        based_in_eu=False,
    )
    return d


@pytest.fixture
async def field_transform_data(async_db):
    d = _D()
    d.sday = sday = datetime.date(2010, 6, 25)
    d.stime = stime = datetime.datetime(2010, 6, 25, 12, 15, 30, 747000)
    d.ex1 = await Experiment.async_object.acreate(
        name="Experiment 1",
        assigned=sday,
        completed=sday + datetime.timedelta(2),
        estimated_time=datetime.timedelta(2),
        start=stime,
        end=stime + datetime.timedelta(2),
    )
    return d


# ---------------------------------------------------------------------------
# BasicExpressionsTests
# ---------------------------------------------------------------------------


async def test_annotate_values_aggregate(basic_expressions_data):
    companies = await (
        Company.async_object.annotate(
            salaries=F("ceo__salary"),
        )
        .values("num_employees", "salaries")
        .aaggregate(
            result=Sum(F("salaries") + F("num_employees"), output_field=IntegerField()),
        )
    )
    assert companies["result"] == 2395


async def test_decimal_division_literal_value(async_db):
    num = await Number.async_object.acreate(integer=2)
    obj = await Number.async_object.annotate(
        val=F("integer") / Value(Decimal("3.0"), output_field=DecimalField())
    ).aget(pk=num.pk)
    assert round(abs(obj.val - Decimal("0.6667")), 4) == 0


async def test_annotate_values_filter(basic_expressions_data):
    d = basic_expressions_data
    companies = Company.async_object.annotate(foo=RawSQL("%s", ["value"])).filter(foo="value").order_by("name")
    assert [c async for c in companies] == [d.example_inc, d.foobar_ltd, d.gmbh]


async def test_annotate_values_count(basic_expressions_data):
    companies = Company.async_object.annotate(foo=RawSQL("%s", ["value"]))
    assert await companies.acount() == 3


async def test_filtering_on_annotate_that_uses_q(basic_expressions_data):
    if not _features().supports_boolean_expr_in_select_clause:
        pytest.skip("DB does not support boolean expr in select clause")
    qs = Company.async_object.annotate(
        num_employees_check=ExpressionWrapper(Q(num_employees__gt=3), output_field=BooleanField())
    ).filter(num_employees_check=True)
    assert await qs.acount() == 2


async def test_filtering_on_q_that_is_boolean(basic_expressions_data):
    qs = Company.async_object.filter(ExpressionWrapper(Q(num_employees__gt=3), output_field=BooleanField()))
    assert await qs.acount() == 2


async def test_filtering_on_rawsql_that_is_boolean(basic_expressions_data):
    qs = Company.async_object.filter(
        RawSQL("num_employees > %s", (3,), output_field=BooleanField()),
    )
    assert await qs.acount() == 2


async def test_filter_inter_attribute(basic_expressions_data):
    d = basic_expressions_data
    qs = d.company_query.filter(num_employees__gt=F("num_chairs"))
    assert [r async for r in qs] == [
        {"num_chairs": 5, "name": "Example Inc.", "num_employees": 2300},
        {"num_chairs": 1, "name": "Test GmbH", "num_employees": 32},
    ]


async def test_update(basic_expressions_data):
    d = basic_expressions_data
    await d.company_query.aupdate(num_chairs=F("num_employees"))
    assert [r async for r in d.company_query] == [
        {"num_chairs": 2300, "name": "Example Inc.", "num_employees": 2300},
        {"num_chairs": 3, "name": "Foobar Ltd.", "num_employees": 3},
        {"num_chairs": 32, "name": "Test GmbH", "num_employees": 32},
    ]


async def _run_slicing_of_f_expressions(model):
    tests = [
        (F("name")[:], "Example Inc."),
        (F("name")[:7], "Example"),
        (F("name")[:6][:5], "Examp"),
        (F("name")[0], "E"),
        (F("name")[13], ""),
        (F("name")[8:], "Inc."),
        (F("name")[0:15], "Example Inc."),
        (F("name")[2:7], "ample"),
        (F("name")[1:][:3], "xam"),
        (F("name")[2:2], ""),
    ]
    for expression, expected in tests:
        obj = await model.async_object.aget(name="Example Inc.")
        try:
            obj.name = expression
            await obj.asave(update_fields=["name"])
            await obj.arefresh_from_db()
            assert obj.name == expected
        finally:
            obj.name = "Example Inc."
            await obj.asave(update_fields=["name"])


async def _test_slicing_of_f_expressions(model):
    tests = [
        (F("name")[:], "Example Inc."),
        (F("name")[:7], "Example"),
        (F("name")[:6][:5], "Examp"),
        (F("name")[0], "E"),
        (F("name")[13], ""),
        (F("name")[8:], "Inc."),
        (F("name")[0:15], "Example Inc."),
        (F("name")[2:7], "ample"),
        (F("name")[1:][:3], "xam"),
        (F("name")[2:2], ""),
    ]
    for expression, expected in tests:
        obj = await model.async_object.aget(name="Example Inc.")
        try:
            obj.name = expression
            await obj.asave(update_fields=["name"])
            await obj.arefresh_from_db()
            assert obj.name == expected
        finally:
            obj.name = "Example Inc."
            await obj.asave(update_fields=["name"])


async def test_slicing_of_f_expressions_charfield(basic_expressions_data):
    await _test_slicing_of_f_expressions(Company)


async def test_slicing_of_f_expressions_textfield(basic_expressions_data):
    await Text.async_object.abulk_create([Text(name=c.name) async for c in Company.async_object.all()])
    await _test_slicing_of_f_expressions(Text)


async def test_slicing_of_f_expressions_with_annotate(basic_expressions_data):
    qs = Company.async_object.annotate(
        first_three=F("name")[:3],
        after_three=F("name")[3:],
        random_four=F("name")[2:5],
        first_letter_slice=F("name")[:1],
        first_letter_index=F("name")[0],
    )
    tests = [
        ("first_three", ["Exa", "Foo", "Tes"]),
        ("after_three", ["mple Inc.", "bar Ltd.", "t GmbH"]),
        ("random_four", ["amp", "oba", "st "]),
        ("first_letter_slice", ["E", "F", "T"]),
        ("first_letter_index", ["E", "F", "T"]),
    ]
    for annotation, expected in tests:
        got = sorted([r async for r in qs.values_list(annotation, flat=True)])
        assert got == sorted(expected)


async def test_slicing_of_f_expression_with_annotated_expression(basic_expressions_data):
    qs = Company.async_object.annotate(
        new_name=Case(
            When(based_in_eu=True, then=Concat(Value("EU:"), F("name"))),
            default=F("name"),
        ),
        first_two=F("new_name")[:3],
    )
    got = sorted([r async for r in qs.values_list("first_two", flat=True)])
    assert got == sorted(["Exa", "EU:", "Tes"])


def test_slicing_of_f_expressions_with_negative_index():
    msg = "Negative indexing is not supported."
    indexes = [slice(0, -4), slice(-4, 0), slice(-4), -5]
    for i in indexes:
        with pytest.raises(ValueError, match=msg):
            F("name")[i]


def test_slicing_of_f_expressions_with_slice_stop_less_than_slice_start():
    with pytest.raises(ValueError, match="Slice stop must be greater than slice start."):
        F("name")[4:2]


def test_slicing_of_f_expressions_with_invalid_type():
    with pytest.raises(TypeError, match="Argument to slice must be either int or slice instance."):
        F("name")["error"]


def test_slicing_of_f_expressions_with_step():
    with pytest.raises(ValueError, match="Step argument is not supported."):
        F("name")[::4]


async def test_slicing_of_f_unsupported_field(basic_expressions_data):
    with pytest.raises(NotSupportedError, match="This field does not support slicing."):
        await Company.async_object.filter().aupdate(num_chairs=F("num_chairs")[:4])


async def test_slicing_of_outerref(basic_expressions_data):
    inner = Company.async_object.filter(name__startswith=OuterRef("ceo__firstname")[0])
    outer = Company.async_object.filter(Exists(inner)).values_list("name", flat=True)
    assert [r async for r in outer] == ["Foobar Ltd."]


async def test_arithmetic(basic_expressions_data):
    d = basic_expressions_data
    await d.company_query.aupdate(num_chairs=F("num_employees") + 2)
    assert [r async for r in d.company_query] == [
        {"num_chairs": 2302, "name": "Example Inc.", "num_employees": 2300},
        {"num_chairs": 5, "name": "Foobar Ltd.", "num_employees": 3},
        {"num_chairs": 34, "name": "Test GmbH", "num_employees": 32},
    ]


async def test_order_of_operations(basic_expressions_data):
    d = basic_expressions_data
    await d.company_query.aupdate(num_chairs=F("num_employees") + 2 * F("num_employees"))
    assert [r async for r in d.company_query] == [
        {"num_chairs": 6900, "name": "Example Inc.", "num_employees": 2300},
        {"num_chairs": 9, "name": "Foobar Ltd.", "num_employees": 3},
        {"num_chairs": 96, "name": "Test GmbH", "num_employees": 32},
    ]


async def test_parenthesis_priority(basic_expressions_data):
    d = basic_expressions_data
    await d.company_query.aupdate(num_chairs=(F("num_employees") + 2) * F("num_employees"))
    assert [r async for r in d.company_query] == [
        {"num_chairs": 5294600, "name": "Example Inc.", "num_employees": 2300},
        {"num_chairs": 15, "name": "Foobar Ltd.", "num_employees": 3},
        {"num_chairs": 1088, "name": "Test GmbH", "num_employees": 32},
    ]


async def test_update_with_fk(basic_expressions_data):
    assert await Company.async_object.filter().aupdate(point_of_contact=F("ceo")) == 3
    got = sorted([str(c.point_of_contact) async for c in Company.async_object.all().select_related("point_of_contact")])
    assert got == sorted(["Joe Smith", "Frank Meyer", "Max Mustermann"])


async def test_update_with_none(async_db):
    await Number.async_object.acreate(integer=1, float=1.0)
    await Number.async_object.acreate(integer=2)
    await Number.async_object.filter(float__isnull=False).aupdate(float=Value(None))
    got = [n.float async for n in Number.async_object.all()]
    assert got == [None, None]


async def test_update_jsonfield_case_when_key_is_null(async_db):
    if not _features().supports_json_field:
        pytest.skip("DB does not support JSON field")
    obj = await JSONFieldModel.async_object.acreate(data={"key": None})
    updated = await JSONFieldModel.async_object.filter().aupdate(
        data=Case(
            When(
                data__key=Value(None, JSONField()),
                then=Value({"key": "something"}, JSONField()),
            ),
        )
    )
    assert updated == 1
    await obj.arefresh_from_db()
    assert obj.data == {"key": "something"}


async def test_filter_with_join(basic_expressions_data):
    await Company.async_object.filter().aupdate(point_of_contact=F("ceo"))
    c = await Company.async_object.afirst()
    c.point_of_contact = await Employee.async_object.acreate(firstname="Guido", lastname="van Rossum")
    await c.asave()

    got = sorted([c.name async for c in Company.async_object.filter(ceo__firstname=F("point_of_contact__firstname"))])
    assert got == sorted(["Foobar Ltd.", "Test GmbH"])

    await Company.async_object.exclude(ceo__firstname=F("point_of_contact__firstname")).aupdate(name="foo")
    assert (await Company.async_object.exclude(ceo__firstname=F("point_of_contact__firstname")).aget()).name == "foo"

    msg = "Joined field references are not permitted in this query"
    with pytest.raises(FieldError, match=msg):
        await Company.async_object.exclude(ceo__firstname=F("point_of_contact__firstname")).aupdate(
            name=F("point_of_contact__lastname")
        )


async def test_object_update(basic_expressions_data):
    d = basic_expressions_data
    d.gmbh.num_employees = F("num_employees") + 4
    await d.gmbh.asave()
    assert d.gmbh.num_employees == 36


async def test_new_object_save(basic_expressions_data):
    d = basic_expressions_data
    test_co = Company(name=Lower(Value("UPPER")), num_employees=32, num_chairs=1, ceo=d.max)
    await test_co.asave()
    await test_co.arefresh_from_db()
    assert test_co.name == "upper"


async def test_new_object_create(basic_expressions_data):
    d = basic_expressions_data
    test_co = await Company.async_object.acreate(name=Lower(Value("UPPER")), num_employees=32, num_chairs=1, ceo=d.max)
    await test_co.arefresh_from_db()
    assert test_co.name == "upper"


async def test_object_create_with_aggregate(basic_expressions_data):
    msg = (
        "Aggregate functions are not allowed in this query "
        r"\(num_employees=Max\(Value\(1\)\)\)\."
    )
    with pytest.raises(FieldError, match=msg):
        await Company.async_object.acreate(
            name="Company",
            num_employees=Max(Value(1)),
            num_chairs=1,
            ceo=await Employee.async_object.acreate(firstname="Just", lastname="Doit", salary=30),
        )


async def test_object_update_fk(basic_expressions_data):
    d = basic_expressions_data
    test_gmbh = await Company.async_object.aget(pk=d.gmbh.pk)
    msg = r'F\(ceo\)": "Company.point_of_contact" must be a "Employee" instance.'
    with pytest.raises(ValueError, match=msg):
        test_gmbh.point_of_contact = F("ceo")

    test_gmbh.point_of_contact = d.gmbh.ceo
    await test_gmbh.asave()
    test_gmbh.name = F("ceo__lastname")
    with pytest.raises(FieldError, match="Joined field references are not permitted in this query"):
        await test_gmbh.asave()


async def test_update_inherited_field_value(basic_expressions_data):
    with pytest.raises(FieldError, match="Joined field references are not permitted in this query"):
        await RemoteEmployee.async_object.filter().aupdate(adjusted_salary=F("salary") * 5)


async def test_object_update_unsaved_objects(basic_expressions_data):
    d = basic_expressions_data
    acme = Company(name="The Acme Widget Co.", num_employees=12, num_chairs=5, ceo=d.max)
    acme.num_employees = F("num_employees") + 16
    msg = (
        r'Failed to insert expression "Col\(expressions_company, '
        r'expressions\.Company\.num_employees\) \+ Value\(16\)" on '
        r"expressions\.Company\.num_employees\. F\(\) expressions can only be "
        r"used to update, not to insert\."
    )
    with pytest.raises(ValueError, match=msg):
        await acme.asave()

    acme.num_employees = 12
    acme.name = Lower(F("name"))
    msg = (
        r'Failed to insert expression "Lower\(Col\(expressions_company, '
        r'expressions\.Company\.name\)\)" on expressions\.Company\.name\. F\(\) '
        r"expressions can only be used to update, not to insert\."
    )
    with pytest.raises(ValueError, match=msg):
        await acme.asave()


async def test_ticket_11722_iexact_lookup(async_db):
    await Employee.async_object.acreate(firstname="John", lastname="Doe")
    test = await Employee.async_object.acreate(firstname="Test", lastname="test")
    qs = Employee.async_object.filter(firstname__iexact=F("lastname"))
    assert [e async for e in qs] == [test]


async def test_ticket_16731_startswith_lookup(async_db):
    await Employee.async_object.acreate(firstname="John", lastname="Doe")
    e2 = await Employee.async_object.acreate(firstname="Jack", lastname="Jackson")
    e3 = await Employee.async_object.acreate(firstname="Jack", lastname="jackson")
    features = _features()
    expected = [e2, e3] if features.has_case_insensitive_like else [e2]
    got = [e async for e in Employee.async_object.filter(lastname__startswith=F("firstname"))]
    assert got == expected
    qs = Employee.async_object.filter(lastname__istartswith=F("firstname")).order_by("pk")
    assert [e async for e in qs] == [e2, e3]


async def test_ticket_18375_join_reuse(basic_expressions_data):
    qs = Employee.async_object.filter(company_ceo_set__num_chairs=F("company_ceo_set__num_employees"))
    assert str(qs.query).count("JOIN") == 1


async def test_ticket_18375_kwarg_ordering(basic_expressions_data):
    qs = Employee.async_object.filter(
        company_ceo_set__num_chairs=F("company_ceo_set__num_employees"),
        company_ceo_set__num_chairs__gte=1,
    )
    assert str(qs.query).count("JOIN") == 1


async def test_ticket_18375_kwarg_ordering_2(basic_expressions_data):
    qs = Employee.async_object.filter(
        company_ceo_set__num_employees=F("pk"),
        pk=F("company_ceo_set__num_employees"),
    )
    assert str(qs.query).count("JOIN") == 1


async def test_ticket_18375_chained_filters(basic_expressions_data):
    qs = Employee.async_object.filter(company_ceo_set__num_employees=F("pk")).filter(
        company_ceo_set__num_employees=F("company_ceo_set__num_employees")
    )
    assert str(qs.query).count("JOIN") == 2


async def test_order_by_exists(basic_expressions_data):
    d = basic_expressions_data
    mary = await Employee.async_object.acreate(firstname="Mary", lastname="Mustermann", salary=20)
    mustermanns_by_seniority = Employee.async_object.filter(lastname="Mustermann").order_by(
        Exists(Company.async_object.filter(ceo=OuterRef("pk"))).desc()
    )
    assert [e async for e in mustermanns_by_seniority] == [d.max, mary]


async def test_order_by_multiline_sql(basic_expressions_data):
    d = basic_expressions_data
    raw_order_by = (
        RawSQL(
            """
            CASE WHEN num_employees > 1000
                 THEN num_chairs
                 ELSE 0 END
            """,
            [],
        ).desc(),
        RawSQL(
            """
            CASE WHEN num_chairs > 1
                 THEN 1
                 ELSE 0 END
            """,
            [],
        ).asc(),
    )
    for qs in (Company.async_object.all(), Company.async_object.distinct()):
        got = [c async for c in qs.order_by(*raw_order_by)]
        assert got == [d.example_inc, d.gmbh, d.foobar_ltd]


async def test_outerref(basic_expressions_data):
    inner = Company.async_object.filter(point_of_contact=OuterRef("pk"))
    msg = "This queryset contains a reference to an outer query and may only be used in a subquery."
    with pytest.raises(ValueError, match=msg):
        await inner.aexists()

    outer = Employee.async_object.annotate(is_point_of_contact=Exists(inner))
    assert await outer.aexists() is True


def test_exist_single_field_output_field():
    queryset = Company.async_object.values("pk")
    assert isinstance(Exists(queryset).output_field, BooleanField)


async def test_subquery(basic_expressions_data):
    d = basic_expressions_data
    await Company.async_object.filter(name="Example Inc.").aupdate(
        point_of_contact=await Employee.async_object.aget(firstname="Joe", lastname="Smith"),
        ceo=d.max,
    )
    await Employee.async_object.acreate(firstname="Bob", lastname="Brown", salary=40)
    qs = (
        Employee.async_object.annotate(
            is_point_of_contact=Exists(Company.async_object.filter(point_of_contact=OuterRef("pk"))),
            is_not_point_of_contact=~Exists(Company.async_object.filter(point_of_contact=OuterRef("pk"))),
            is_ceo_of_small_company=Exists(Company.async_object.filter(num_employees__lt=200, ceo=OuterRef("pk"))),
            is_ceo_small_2=~~Exists(Company.async_object.filter(num_employees__lt=200, ceo=OuterRef("pk"))),
            largest_company=Subquery(
                Company.async_object.order_by("-num_employees")
                .filter(Q(ceo=OuterRef("pk")) | Q(point_of_contact=OuterRef("pk")))
                .values("name")[:1],
                output_field=CharField(),
            ),
        )
        .values(
            "firstname",
            "is_point_of_contact",
            "is_not_point_of_contact",
            "is_ceo_of_small_company",
            "is_ceo_small_2",
            "largest_company",
        )
        .order_by("firstname")
    )
    results = [r async for r in qs]
    assert results == [
        {
            "firstname": "Bob",
            "is_point_of_contact": False,
            "is_not_point_of_contact": True,
            "is_ceo_of_small_company": False,
            "is_ceo_small_2": False,
            "largest_company": None,
        },
        {
            "firstname": "Frank",
            "is_point_of_contact": False,
            "is_not_point_of_contact": True,
            "is_ceo_of_small_company": True,
            "is_ceo_small_2": True,
            "largest_company": "Foobar Ltd.",
        },
        {
            "firstname": "Joe",
            "is_point_of_contact": True,
            "is_not_point_of_contact": False,
            "is_ceo_of_small_company": False,
            "is_ceo_small_2": False,
            "largest_company": "Example Inc.",
        },
        {
            "firstname": "Max",
            "is_point_of_contact": False,
            "is_not_point_of_contact": True,
            "is_ceo_of_small_company": True,
            "is_ceo_small_2": True,
            "largest_company": "Example Inc.",
        },
    ]


async def test_subquery_eq(basic_expressions_data):
    qs = Employee.async_object.annotate(
        is_ceo=Exists(Company.async_object.filter(ceo=OuterRef("pk"))),
        is_point_of_contact=Exists(
            Company.async_object.filter(point_of_contact=OuterRef("pk")),
        ),
        small_company=Exists(
            queryset=Company.async_object.filter(num_employees__lt=200),
        ),
    ).filter(is_ceo=True, is_point_of_contact=False, small_company=True)
    assert qs.query.annotations["is_ceo"] != qs.query.annotations["is_point_of_contact"]
    assert qs.query.annotations["is_ceo"] != qs.query.annotations["small_company"]


async def test_in_subquery(basic_expressions_data):
    d = basic_expressions_data
    small_companies = Company.async_object.filter(num_employees__lt=200).values("pk")
    subquery_test = Company.async_object.filter(pk__in=Subquery(small_companies))
    got = sorted([c.pk async for c in subquery_test])
    assert got == sorted([d.foobar_ltd.pk, d.gmbh.pk])
    subquery_test2 = Company.async_object.filter(pk=Subquery(small_companies.filter(num_employees=3)[:1]))
    got2 = [c async for c in subquery_test2]
    assert got2 == [d.foobar_ltd]


async def test_uuid_pk_subquery(async_db):
    u = await UUIDPK.async_object.acreate()
    await UUID.async_object.acreate(uuid_fk=u)
    qs = UUIDPK.async_object.filter(id__in=Subquery(UUID.async_object.values("uuid_fk__id")))
    got = [o async for o in qs]
    assert got == [u]


async def test_nested_subquery(basic_expressions_data):
    inner = Company.async_object.filter(point_of_contact=OuterRef("pk"))
    outer = Employee.async_object.annotate(is_point_of_contact=Exists(inner))
    contrived = Employee.async_object.annotate(
        is_point_of_contact=Subquery(
            outer.filter(pk=OuterRef("pk")).values("is_point_of_contact"),
            output_field=BooleanField(),
        ),
    )
    got_contrived = sorted([tuple(r) async for r in contrived.values_list()])
    got_outer = sorted([tuple(r) async for r in outer.values_list()])
    assert got_contrived == got_outer


async def test_nested_subquery_join_outer_ref(basic_expressions_data):
    d = basic_expressions_data
    inner = Employee.async_object.filter(pk=OuterRef("ceo__pk")).values("pk")
    qs = Employee.async_object.annotate(
        ceo_company=Subquery(
            Company.async_object.filter(
                ceo__in=inner,
                ceo__pk=OuterRef("pk"),
            ).values("pk"),
        ),
    )
    got = [r async for r in qs.values_list("ceo_company", flat=True)]
    assert got == [d.example_inc.pk, d.foobar_ltd.pk, d.gmbh.pk]


async def test_nested_subquery_outer_ref_2(async_db):
    first = await Time.async_object.acreate(time="09:00")
    second = await Time.async_object.acreate(time="17:00")
    third = await Time.async_object.acreate(time="21:00")
    await SimulationRun.async_object.abulk_create(
        [
            SimulationRun(start=first, end=second, midpoint="12:00"),
            SimulationRun(start=first, end=third, midpoint="15:00"),
            SimulationRun(start=second, end=first, midpoint="00:00"),
        ]
    )
    inner = Time.async_object.filter(time=OuterRef(OuterRef("time")), pk=OuterRef("start")).values("time")
    middle = SimulationRun.async_object.annotate(other=Subquery(inner)).values("other")[:1]
    outer = Time.async_object.annotate(other=Subquery(middle, output_field=TimeField()))
    got = sorted([o.pk async for o in outer])
    assert got == sorted([first.pk, second.pk, third.pk])


async def test_nested_subquery_outer_ref_with_autofield(async_db):
    first = await Time.async_object.acreate(time="09:00")
    second = await Time.async_object.acreate(time="17:00")
    await SimulationRun.async_object.acreate(start=first, end=second, midpoint="12:00")
    inner = SimulationRun.async_object.filter(start=OuterRef(OuterRef("pk"))).values("start")
    middle = Time.async_object.annotate(other=Subquery(inner)).values("other")[:1]
    outer = Time.async_object.annotate(other=Subquery(middle, output_field=IntegerField()))
    got = sorted([o.pk async for o in outer])
    assert got == sorted([first.pk, second.pk])


async def test_annotations_within_subquery(basic_expressions_data):
    await Company.async_object.filter(num_employees__lt=50).aupdate(
        ceo=await Employee.async_object.aget(firstname="Frank")
    )
    inner = (
        Company.async_object.filter(ceo=OuterRef("pk"))
        .values("ceo")
        .annotate(total_employees=Sum("num_employees"))
        .values("total_employees")
    )
    outer = Employee.async_object.annotate(total_employees=Subquery(inner)).filter(salary__lte=Subquery(inner))
    got = [r async for r in outer.order_by("-total_employees").values("salary", "total_employees")]
    assert got == [
        {"salary": 10, "total_employees": 2300},
        {"salary": 20, "total_employees": 35},
    ]


async def test_subquery_references_joined_table_twice(basic_expressions_data):
    inner = Company.async_object.filter(
        num_chairs__gte=OuterRef("ceo__salary"),
        num_employees__gte=OuterRef("point_of_contact__salary"),
    )
    outer = Company.async_object.filter(pk__in=Subquery(inner.values("pk")))
    assert not await outer.aexists()


async def test_subquery_filter_by_aggregate(async_db):
    await Number.async_object.acreate(integer=1000, float=1.2)
    await Employee.async_object.acreate(salary=1000, firstname="", lastname="")
    qs = Number.async_object.annotate(
        min_valuable_count=Subquery(
            Employee.async_object.filter(salary=OuterRef("integer"))
            .annotate(cnt=Count("salary"))
            .filter(cnt__gt=0)
            .values("cnt")[:1]
        ),
    )
    assert (await qs.aget()).float == 1.2


async def test_aggregate_subquery_annotation(basic_expressions_data):
    from tests.fixtures.query_counter import async_capture_queries

    async with async_capture_queries() as queries:
        aggregate = await Company.async_object.annotate(
            ceo_salary=Subquery(
                Employee.async_object.filter(
                    id=OuterRef("ceo_id"),
                ).values("salary")
            ),
        ).aaggregate(
            ceo_salary_gt_20=Count("pk", filter=Q(ceo_salary__gt=20)),
        )
    assert aggregate == {"ceo_salary_gt_20": 1}
    assert len(queries) == 1
    sql = queries[0]["sql"]
    assert sql.count("SELECT") <= 3
    assert "GROUP BY" not in sql


async def test_object_create_with_f_expression_in_subquery(basic_expressions_data):
    d = basic_expressions_data
    await Company.async_object.acreate(name="Big company", num_employees=100000, num_chairs=1, ceo=d.max)
    biggest_company = await Company.async_object.acreate(
        name="Biggest company",
        num_chairs=1,
        ceo=d.max,
        num_employees=Subquery(
            Company.async_object.order_by("-num_employees")
            .annotate(max_num_employees=Max("num_employees"))
            .annotate(new_num_employees=F("max_num_employees") + 1)
            .values("new_num_employees")[:1]
        ),
    )
    await biggest_company.arefresh_from_db()
    assert biggest_company.num_employees == 100001


async def test_aggregate_rawsql_annotation(basic_expressions_data):
    from tests.fixtures.query_counter import async_capture_queries

    async with async_capture_queries() as queries:
        aggregate = await Company.async_object.annotate(
            salary=RawSQL("SUM(num_chairs) OVER (ORDER BY num_employees)", []),
        ).aaggregate(
            count=Count("pk"),
        )
        assert aggregate == {"count": 3}
    assert len(queries) == 1
    sql = queries[0]["sql"]
    assert "GROUP BY" not in sql


def test_explicit_output_field():
    class FuncA(Func):
        output_field = CharField()

    class FuncB(Func):
        pass

    expr = FuncB(FuncA())
    assert expr.output_field == FuncA.output_field


async def test_outerref_mixed_case_table_name(async_db):
    inner = Result.async_object.filter(result_time__gte=OuterRef("experiment__assigned"))
    outer = Result.async_object.filter(pk__in=Subquery(inner.values("pk")))
    assert not await outer.aexists()


async def test_outerref_with_operator(basic_expressions_data):
    inner = Company.async_object.filter(num_employees=OuterRef("ceo__salary") + 2)
    outer = Company.async_object.filter(pk__in=Subquery(inner.values("pk")))
    assert (await outer.aget()).name == "Test GmbH"


async def test_nested_outerref_with_function(basic_expressions_data):
    d = basic_expressions_data
    d.gmbh.point_of_contact = await Employee.async_object.aget(lastname="Meyer")
    await d.gmbh.asave()
    inner = Employee.async_object.filter(
        lastname__startswith=Left(OuterRef(OuterRef("lastname")), 1),
    )
    qs = Employee.async_object.annotate(
        ceo_company=Subquery(
            Company.async_object.filter(
                point_of_contact__in=inner,
                ceo__pk=OuterRef("pk"),
            ).values("name"),
        ),
    ).filter(ceo_company__isnull=False)
    assert (await qs.aget()).ceo_company == "Test GmbH"


async def test_annotation_with_outerref(basic_expressions_data):
    d = basic_expressions_data
    gmbh_salary = await Company.async_object.annotate(
        max_ceo_salary_raise=Subquery(
            Company.async_object.annotate(
                salary_raise=OuterRef("num_employees") + F("num_employees"),
            )
            .order_by("-salary_raise")
            .values("salary_raise")[:1],
        ),
    ).aget(pk=d.gmbh.pk)
    assert gmbh_salary.max_ceo_salary_raise == 2332


async def test_annotation_with_outerref_and_output_field(basic_expressions_data):
    d = basic_expressions_data
    gmbh_salary = await Company.async_object.annotate(
        max_ceo_salary_raise=Subquery(
            Company.async_object.annotate(
                salary_raise=OuterRef("num_employees") + F("num_employees"),
            )
            .order_by("-salary_raise")
            .values("salary_raise")[:1],
            output_field=DecimalField(),
        ),
    ).aget(pk=d.gmbh.pk)
    assert gmbh_salary.max_ceo_salary_raise == 2332.0
    assert isinstance(gmbh_salary.max_ceo_salary_raise, Decimal)


async def test_annotation_with_nested_outerref(basic_expressions_data):
    d = basic_expressions_data
    d.gmbh.point_of_contact = await Employee.async_object.aget(lastname="Meyer")
    await d.gmbh.asave()
    inner = Employee.async_object.annotate(
        outer_lastname=OuterRef(OuterRef("lastname")),
    ).filter(lastname__startswith=Left("outer_lastname", 1))
    qs = Employee.async_object.annotate(
        ceo_company=Subquery(
            Company.async_object.filter(
                point_of_contact__in=inner,
                ceo__pk=OuterRef("pk"),
            ).values("name"),
        ),
    ).filter(ceo_company__isnull=False)
    assert (await qs.aget()).ceo_company == "Test GmbH"


async def test_annotation_with_deeply_nested_outerref(basic_expressions_data):
    d = basic_expressions_data
    bob = await Employee.async_object.acreate(firstname="Bob", lastname="", based_in_eu=True)
    d.max.manager = await Manager.async_object.acreate(name="Rock", secretary=bob)
    await d.max.asave()
    qs = Employee.async_object.filter(
        Exists(
            Manager.async_object.filter(
                Exists(
                    Employee.async_object.filter(
                        pk=OuterRef("secretary__pk"),
                    )
                    .annotate(secretary_based_in_eu=OuterRef(OuterRef("based_in_eu")))
                    .filter(Exists(Company.async_object.filter(based_in_eu=OuterRef("secretary_based_in_eu"))))
                ),
                secretary__pk=OuterRef("pk"),
            )
        )
    )
    assert await qs.aget() == bob


def test_pickle_expression():
    expr = Value(1)
    expr.convert_value  # populate cached property
    assert pickle.loads(pickle.dumps(expr)) == expr  # noqa: S301


async def test_incorrect_field_in_F_expression(async_db):
    with pytest.raises(FieldError, match="Cannot resolve keyword 'nope' into field."):
        [e async for e in Employee.async_object.filter(firstname=F("nope"))]


async def test_incorrect_joined_field_in_F_expression(async_db):
    with pytest.raises(FieldError, match="Cannot resolve keyword 'nope' into field."):
        [c async for c in Company.async_object.filter(ceo__pk=F("point_of_contact__nope"))]


async def test_exists_in_filter(basic_expressions_data):
    inner = Company.async_object.filter(ceo=OuterRef("pk")).values("pk")
    qs1 = Employee.async_object.filter(Exists(inner))
    qs2 = Employee.async_object.annotate(found=Exists(inner)).filter(found=True)
    got1 = sorted([e.pk async for e in qs1])
    got2 = sorted([e.pk async for e in qs2])
    assert got1 == got2
    assert not await Employee.async_object.exclude(Exists(inner)).aexists()
    got3 = sorted([e.pk async for e in Employee.async_object.exclude(~Exists(inner))])
    assert got2 == got3


async def test_subquery_in_filter(basic_expressions_data):
    d = basic_expressions_data
    inner = Company.async_object.filter(ceo=OuterRef("pk")).values("based_in_eu")
    qs = Employee.async_object.filter(Subquery(inner))
    assert [e async for e in qs] == [d.foobar_ltd.ceo]


async def test_subquery_group_by_outerref_in_filter(basic_expressions_data):
    inner = (
        Company.async_object.annotate(employee=OuterRef("pk"))
        .values("employee")
        .annotate(min_num_chairs=Min("num_chairs"))
        .values("ceo")
    )
    assert await Employee.async_object.filter(pk__in=Subquery(inner)).aexists() is True


async def test_case_in_filter_if_boolean_output_field(basic_expressions_data):
    d = basic_expressions_data
    is_ceo = Company.async_object.filter(ceo=OuterRef("pk"))
    is_poc = Company.async_object.filter(point_of_contact=OuterRef("pk"))
    qs = Employee.async_object.filter(
        Case(
            When(Exists(is_ceo), then=True),
            When(Exists(is_poc), then=True),
            default=False,
            output_field=BooleanField(),
        ),
    )
    got = sorted([e.pk async for e in qs])
    assert got == sorted([d.example_inc.ceo.pk, d.foobar_ltd.ceo.pk, d.max.pk])


async def test_boolean_expression_combined(basic_expressions_data):
    d = basic_expressions_data
    is_ceo = Company.async_object.filter(ceo=OuterRef("pk"))
    is_poc = Company.async_object.filter(point_of_contact=OuterRef("pk"))
    d.gmbh.point_of_contact = d.max
    await d.gmbh.asave()

    def sorted_pks(qs_result):
        return sorted([e.pk for e in qs_result])

    got = [e async for e in Employee.async_object.filter(Exists(is_ceo) | Exists(is_poc))]
    assert sorted_pks(got) == sorted([d.example_inc.ceo.pk, d.foobar_ltd.ceo.pk, d.max.pk])

    got = [e async for e in Employee.async_object.filter(Exists(is_ceo) & Exists(is_poc))]
    assert sorted_pks(got) == [d.max.pk]

    got = [e async for e in Employee.async_object.filter(Exists(is_ceo) & Q(salary__gte=30))]
    assert sorted_pks(got) == [d.max.pk]

    got = [e async for e in Employee.async_object.filter(Exists(is_poc) | Q(salary__lt=15))]
    assert sorted_pks(got) == sorted([d.example_inc.ceo.pk, d.max.pk])

    got = [e async for e in Employee.async_object.filter(Q(salary__gte=30) & Exists(is_ceo))]
    assert sorted_pks(got) == [d.max.pk]

    got = [e async for e in Employee.async_object.filter(Q(salary__lt=15) | Exists(is_poc))]
    assert sorted_pks(got) == sorted([d.example_inc.ceo.pk, d.max.pk])


async def test_boolean_expression_combined_with_empty_Q(basic_expressions_data):
    d = basic_expressions_data
    is_poc = Company.async_object.filter(point_of_contact=OuterRef("pk"))
    d.gmbh.point_of_contact = d.max
    await d.gmbh.asave()
    tests = [
        Exists(is_poc) & Q(),
        Q() & Exists(is_poc),
        Exists(is_poc) | Q(),
        Q() | Exists(is_poc),
        Q(Exists(is_poc)) & Q(),
        Q() & Q(Exists(is_poc)),
        Q(Exists(is_poc)) | Q(),
        Q() | Q(Exists(is_poc)),
    ]
    for conditions in tests:
        got = [e async for e in Employee.async_object.filter(conditions)]
        assert sorted([e.pk for e in got]) == [d.max.pk]


async def test_boolean_expression_in_Q(basic_expressions_data):
    d = basic_expressions_data
    is_poc = Company.async_object.filter(point_of_contact=OuterRef("pk"))
    d.gmbh.point_of_contact = d.max
    await d.gmbh.asave()
    got = [e async for e in Employee.async_object.filter(Q(Exists(is_poc)))]
    assert sorted([e.pk for e in got]) == [d.max.pk]


# ---------------------------------------------------------------------------
# IterableLookupInnerExpressionsTests
# ---------------------------------------------------------------------------


async def test_in_lookup_allows_F_expressions_and_expressions_for_integers(iterable_lookup_data):
    d = iterable_lookup_data
    queryset = Company.async_object.filter(num_employees__in=([F("num_chairs") - 10]))
    assert [c async for c in queryset] == [d.c5060]
    got = sorted(
        [
            c.pk
            async for c in Company.async_object.filter(num_employees__in=([F("num_chairs") - 10, F("num_chairs") + 10]))
        ]
    )
    assert got == sorted([d.c5040.pk, d.c5060.pk])
    got = sorted(
        [
            c.pk
            async for c in Company.async_object.filter(
                num_employees__in=([F("num_chairs") - 10, F("num_chairs"), F("num_chairs") + 10])
            )
        ]
    )
    assert got == sorted([d.c5040.pk, d.c5050.pk, d.c5060.pk])


async def test_expressions_range_lookups_join_choice(async_db):
    midpoint = datetime.time(13, 0)
    t1 = await Time.async_object.acreate(time=datetime.time(12, 0))
    t2 = await Time.async_object.acreate(time=datetime.time(14, 0))
    s1 = await SimulationRun.async_object.acreate(start=t1, end=t2, midpoint=midpoint)
    await SimulationRun.async_object.acreate(start=t1, end=None, midpoint=midpoint)
    await SimulationRun.async_object.acreate(start=None, end=t2, midpoint=midpoint)
    await SimulationRun.async_object.acreate(start=None, end=None, midpoint=midpoint)

    queryset = SimulationRun.async_object.filter(midpoint__range=[F("start__time"), F("end__time")])
    assert [r async for r in queryset] == [s1]

    queryset = SimulationRun.async_object.exclude(midpoint__range=[F("start__time"), F("end__time")])
    assert [r async for r in queryset] == []


async def test_range_lookup_allows_F_expressions_and_expressions_for_integers(iterable_lookup_data):
    d = iterable_lookup_data
    got = sorted([c.pk async for c in Company.async_object.filter(num_employees__range=(F("num_chairs"), 100))])
    assert got == sorted([d.c5020.pk, d.c5040.pk, d.c5050.pk])

    got = sorted(
        [
            c.pk
            async for c in Company.async_object.filter(
                num_employees__range=(F("num_chairs") - 10, F("num_chairs") + 10)
            )
        ]
    )
    assert got == sorted([d.c5040.pk, d.c5050.pk, d.c5060.pk])

    got = sorted([c.pk async for c in Company.async_object.filter(num_employees__range=(F("num_chairs") - 10, 100))])
    assert got == sorted([d.c5020.pk, d.c5040.pk, d.c5050.pk, d.c5060.pk])

    got = sorted([c.pk async for c in Company.async_object.filter(num_employees__range=(1, 100))])
    assert got == sorted([d.c5020.pk, d.c5040.pk, d.c5050.pk, d.c5060.pk, d.c99300.pk])


async def test_range_lookup_namedtuple(iterable_lookup_data):
    from collections import namedtuple

    d = iterable_lookup_data
    EmployeeRange = namedtuple("EmployeeRange", ["minimum", "maximum"])
    qs = Company.async_object.filter(
        num_employees__range=EmployeeRange(minimum=51, maximum=100),
    )
    assert [c async for c in qs] == [d.c99300]


async def test_range_lookup_allows_F_expressions_and_expressions_for_dates(async_db):
    start = datetime.datetime(2016, 2, 3, 15, 0, 0)
    end = datetime.datetime(2016, 2, 5, 15, 0, 0)
    experiment_1 = await Experiment.async_object.acreate(
        name="Integrity testing",
        assigned=start.date(),
        start=start,
        end=end,
        completed=end.date(),
        estimated_time=end - start,
    )
    experiment_2 = await Experiment.async_object.acreate(
        name="Taste testing",
        assigned=start.date(),
        start=start,
        end=end,
        completed=end.date(),
        estimated_time=end - start,
    )
    r1 = await Result.async_object.acreate(
        experiment=experiment_1,
        result_time=datetime.datetime(2016, 2, 4, 15, 0, 0),
    )
    await Result.async_object.acreate(
        experiment=experiment_1,
        result_time=datetime.datetime(2016, 3, 10, 2, 0, 0),
    )
    await Result.async_object.acreate(
        experiment=experiment_2,
        result_time=datetime.datetime(2016, 1, 8, 5, 0, 0),
    )
    tests = [
        ([F("experiment__start"), F("experiment__end")], "result_time__range"),
        (
            [F("experiment__start__date"), F("experiment__end__date")],
            "result_time__date__range",
        ),
    ]
    for within_experiment_time, lookup in tests:
        queryset = Result.async_object.filter(**{lookup: within_experiment_time})
        got = [r async for r in queryset]
        assert got == [r1]


async def test_relabeled_clone_rhs(async_db):
    await Number.async_object.abulk_create([Number(integer=1), Number(integer=2)])
    got = await Number.async_object.filter(
        Exists(Number.async_object.exclude(pk=OuterRef("pk")).filter(integer__range=(F("integer"), F("integer"))))
    ).aexists()
    assert got is True


# ---------------------------------------------------------------------------
# FTests (pure Python)
# ---------------------------------------------------------------------------


def test_F_deepcopy():
    f = F("foo")
    g = deepcopy(f)
    assert f.name == g.name


def test_F_deconstruct():
    f = F("name")
    path, args, kwargs = f.deconstruct()
    assert path == "django.db.models.F"
    assert args == (f.name,)
    assert kwargs == {}


def test_F_equal():
    f = F("name")
    same_f = F("name")
    other_f = F("username")
    assert f == same_f
    assert f != other_f


def test_F_hash():
    d = {F("name"): "Bob"}
    assert F("name") in d
    assert d[F("name")] == "Bob"


def test_F_not_equal_Value():
    f = F("name")
    value = Value("name")
    assert f != value
    assert value != f


def test_F_contains():
    with pytest.raises(TypeError, match="argument of type 'F' is not iterable"):
        "" in F("name")  # noqa: B015


def test_F_replace_expressions_transform():
    replacements = {F("timestamp"): Value(None)}
    transform_ref = F("timestamp__date")
    assert transform_ref.replace_expressions(replacements) is transform_ref
    invalid_transform_ref = F("timestamp__invalid")
    assert invalid_transform_ref.replace_expressions(replacements) is invalid_transform_ref
    replacements = {F("timestamp"): Value(datetime.datetime(2025, 3, 1, 14, 10))}
    assert F("timestamp__date").replace_expressions(replacements) == TruncDate(
        Value(datetime.datetime(2025, 3, 1, 14, 10))
    )
    assert F("timestamp__date__day").replace_expressions(replacements) == ExtractDay(
        TruncDate(Value(datetime.datetime(2025, 3, 1, 14, 10)))
    )
    invalid_nested_transform_ref = F("timestamp__date__invalid")
    assert invalid_nested_transform_ref.replace_expressions(replacements) is invalid_nested_transform_ref
    mock_replacements = mock.Mock()
    mock_replacements.get.return_value = None
    field_ref = F("name")
    assert field_ref.replace_expressions(mock_replacements) is field_ref
    mock_replacements.get.assert_called_once_with(field_ref)


# ---------------------------------------------------------------------------
# ExpressionsTests
# ---------------------------------------------------------------------------


async def test_F_reuse(async_db):
    f = F("id")
    n = await Number.async_object.acreate(integer=-1)
    c = await Company.async_object.acreate(
        name="Example Inc.",
        num_employees=2300,
        num_chairs=5,
        ceo=await Employee.async_object.acreate(firstname="Joe", lastname="Smith"),
    )
    c_qs = Company.async_object.filter(id=f)
    assert await c_qs.aget() == c
    n_qs = Number.async_object.filter(id=f)
    assert await n_qs.aget() == n
    assert await c_qs.aget() == c


async def test_patterns_escape(async_db):
    await Employee.async_object.abulk_create(
        [
            Employee(firstname="Johnny", lastname="%John"),
            Employee(firstname="Jean-Claude", lastname="Claud_"),
            Employee(firstname="Jean-Claude", lastname="Claude%"),
            Employee(firstname="Johnny", lastname="Joh\\n"),
            Employee(firstname="Johnny", lastname="_ohn"),
            Employee(firstname="Johnny", lastname="^Joh"),
            Employee(firstname="Johnny", lastname="Johnny$"),
            Employee(firstname="Johnny", lastname="Joh."),
            Employee(firstname="Johnny", lastname="[J]ohnny"),
            Employee(firstname="Johnny", lastname="(J)ohnny"),
            Employee(firstname="Johnny", lastname="J*ohnny"),
            Employee(firstname="Johnny", lastname="J+ohnny"),
            Employee(firstname="Johnny", lastname="J?ohnny"),
            Employee(firstname="Johnny", lastname="J{1}ohnny"),
            Employee(firstname="Johnny", lastname="J|ohnny"),
            Employee(firstname="Johnny", lastname="J-ohnny"),
        ]
    )
    claude = await Employee.async_object.acreate(firstname="Jean-Claude", lastname="Claude")
    john = await Employee.async_object.acreate(firstname="Johnny", lastname="John")
    john_sign = await Employee.async_object.acreate(firstname="%Joh\\nny", lastname="%Joh\\n")

    got = sorted([e.pk async for e in Employee.async_object.filter(firstname__contains=F("lastname"))])
    assert got == sorted([john_sign.pk, john.pk, claude.pk])
    got = sorted([e.pk async for e in Employee.async_object.filter(firstname__startswith=F("lastname"))])
    assert got == sorted([john_sign.pk, john.pk])
    got = [e async for e in Employee.async_object.filter(firstname__endswith=F("lastname"))]
    assert got == [claude]


async def test_insensitive_patterns_escape(async_db):
    await Employee.async_object.abulk_create(
        [
            Employee(firstname="Johnny", lastname="%john"),
            Employee(firstname="Jean-Claude", lastname="claud_"),
            Employee(firstname="Jean-Claude", lastname="claude%"),
            Employee(firstname="Johnny", lastname="joh\\n"),
            Employee(firstname="Johnny", lastname="_ohn"),
        ]
    )
    claude = await Employee.async_object.acreate(firstname="Jean-Claude", lastname="claude")
    john = await Employee.async_object.acreate(firstname="Johnny", lastname="john")
    john_sign = await Employee.async_object.acreate(firstname="%Joh\\nny", lastname="%joh\\n")

    got = sorted([e.pk async for e in Employee.async_object.filter(firstname__icontains=F("lastname"))])
    assert got == sorted([john_sign.pk, john.pk, claude.pk])
    got = sorted([e.pk async for e in Employee.async_object.filter(firstname__istartswith=F("lastname"))])
    assert got == sorted([john_sign.pk, john.pk])
    got = [e async for e in Employee.async_object.filter(firstname__iendswith=F("lastname"))]
    assert got == [claude]


# ---------------------------------------------------------------------------
# SimpleExpressionTests (pure Python)
# ---------------------------------------------------------------------------


def test_Expression_equal():
    assert Expression() == Expression()
    assert Expression(IntegerField()) == Expression(output_field=IntegerField())
    assert Expression(IntegerField()) == mock.ANY
    assert Expression(IntegerField()) != Expression(CharField())

    class InitCaptureExpression(Expression):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

    assert InitCaptureExpression(IntegerField()) != InitCaptureExpression(output_field=IntegerField())
    assert InitCaptureExpression(IntegerField()) == InitCaptureExpression(IntegerField())
    assert InitCaptureExpression(output_field=IntegerField()) == InitCaptureExpression(output_field=IntegerField())


def test_Expression_hash():
    assert hash(Expression()) == hash(Expression())
    assert hash(Expression(IntegerField())) == hash(Expression(output_field=IntegerField()))
    assert hash(Expression(IntegerField())) != hash(Expression(CharField()))


def test_get_expression_for_validation_only_one_source_expression():
    expression = Expression()
    expression.constraint_validation_compatible = False
    msg = "Expressions with constraint_validation_compatible set to False must have only one source expression."
    with pytest.raises(ValueError, match=msg):
        expression.get_expression_for_validation()


def test_replace_expressions_falsey():
    class AssignableExpression(Expression):
        def __init__(self, *source_expressions):
            super().__init__()
            self.set_source_expressions(list(source_expressions))

        def get_source_expressions(self):
            return self.source_expressions

        def set_source_expressions(self, exprs):
            self.source_expressions = exprs

    expression = AssignableExpression()
    falsey = Q()
    expression.set_source_expressions([falsey])
    replaced = expression.replace_expressions({"replacement": Expression()})
    assert replaced.get_source_expressions() == [falsey]


# ---------------------------------------------------------------------------
# ExpressionsNumericTests
# ---------------------------------------------------------------------------


async def test_fill_with_value_from_same_object(numeric_data):
    got = sorted([(n.integer, round(n.float)) async for n in Number.async_object.all()])
    assert got == sorted([(-1, -1), (42, 42), (1337, 1337)])


async def test_increment_value(numeric_data):
    updated = await Number.async_object.filter(integer__gt=0).aupdate(integer=F("integer") + 1)
    assert updated == 2
    got = sorted([(n.integer, round(n.float)) async for n in Number.async_object.all()])
    assert got == sorted([(-1, -1), (43, 42), (1338, 1337)])


async def test_filter_not_equals_other_field(numeric_data):
    updated = await Number.async_object.filter(integer__gt=0).aupdate(integer=F("integer") + 1)
    assert updated == 2
    got = sorted([(n.integer, round(n.float)) async for n in Number.async_object.exclude(float=F("integer"))])
    assert got == sorted([(43, 42), (1338, 1337)])


async def test_filter_decimal_expression(async_db):
    obj = await Number.async_object.acreate(integer=0, float=1, decimal_value=Decimal(1))
    qs = Number.async_object.annotate(
        x=ExpressionWrapper(Value(1), output_field=DecimalField()),
    ).filter(Q(x=1, integer=0) & Q(x=Decimal(1)))
    assert [n async for n in qs] == [obj]


async def test_complex_expressions(async_db):
    n = await Number.async_object.acreate(integer=10, float=123.45)
    updated = await Number.async_object.filter(pk=n.pk).aupdate(float=F("integer") + F("float") * 2)
    assert updated == 1
    refreshed = await Number.async_object.aget(pk=n.pk)
    assert refreshed.integer == 10
    assert abs(refreshed.float - 256.900) < 0.001


async def test_decimal_expression(async_db):
    n = await Number.async_object.acreate(integer=1, decimal_value=Decimal("0.5"))
    n.decimal_value = F("decimal_value") - Decimal("0.4")
    await n.asave()
    assert n.decimal_value == Decimal("0.1")


# ---------------------------------------------------------------------------
# ExpressionOperatorTests
# ---------------------------------------------------------------------------


async def test_lefthand_addition(numeric_op_data):
    d = numeric_op_data
    await Number.async_object.filter(pk=d.n.pk).aupdate(integer=F("integer") + 15, float=F("float") + 42.7)
    n = await Number.async_object.aget(pk=d.n.pk)
    assert n.integer == 57
    assert abs(n.float - 58.200) < 0.001


async def test_lefthand_subtraction(numeric_op_data):
    d = numeric_op_data
    await Number.async_object.filter(pk=d.n.pk).aupdate(integer=F("integer") - 15, float=F("float") - 42.7)
    n = await Number.async_object.aget(pk=d.n.pk)
    assert n.integer == 27
    assert abs(n.float - (-27.200)) < 0.001


async def test_lefthand_multiplication(numeric_op_data):
    d = numeric_op_data
    await Number.async_object.filter(pk=d.n.pk).aupdate(integer=F("integer") * 15, float=F("float") * 42.7)
    n = await Number.async_object.aget(pk=d.n.pk)
    assert n.integer == 630
    assert abs(n.float - 661.850) < 0.001


async def test_lefthand_division(numeric_op_data):
    d = numeric_op_data
    await Number.async_object.filter(pk=d.n.pk).aupdate(integer=F("integer") / 2, float=F("float") / 42.7)
    n = await Number.async_object.aget(pk=d.n.pk)
    assert n.integer == 21
    assert abs(n.float - 0.363) < 0.001


async def test_lefthand_modulo(numeric_op_data):
    d = numeric_op_data
    await Number.async_object.filter(pk=d.n.pk).aupdate(integer=F("integer") % 20)
    assert (await Number.async_object.aget(pk=d.n.pk)).integer == 2


async def test_lefthand_modulo_null(async_db):
    await Employee.async_object.acreate(firstname="John", lastname="Doe", salary=None)
    qs = Employee.async_object.annotate(modsalary=F("salary") % 20)
    assert (await qs.aget()).salary is None


async def test_lefthand_bitwise_and(numeric_op_data):
    d = numeric_op_data
    await Number.async_object.filter(pk=d.n.pk).aupdate(integer=F("integer").bitand(56))
    await Number.async_object.filter(pk=d.n1.pk).aupdate(integer=F("integer").bitand(-56))
    assert (await Number.async_object.aget(pk=d.n.pk)).integer == 40
    assert (await Number.async_object.aget(pk=d.n1.pk)).integer == -64


async def test_lefthand_bitwise_left_shift_operator(numeric_op_data):
    d = numeric_op_data
    await Number.async_object.filter().aupdate(integer=F("integer").bitleftshift(2))
    assert (await Number.async_object.aget(pk=d.n.pk)).integer == 168
    assert (await Number.async_object.aget(pk=d.n1.pk)).integer == -168


async def test_lefthand_bitwise_right_shift_operator(numeric_op_data):
    d = numeric_op_data
    await Number.async_object.filter().aupdate(integer=F("integer").bitrightshift(2))
    assert (await Number.async_object.aget(pk=d.n.pk)).integer == 10
    assert (await Number.async_object.aget(pk=d.n1.pk)).integer == -11


async def test_lefthand_bitwise_or(numeric_op_data):
    d = numeric_op_data
    await Number.async_object.filter().aupdate(integer=F("integer").bitor(48))
    assert (await Number.async_object.aget(pk=d.n.pk)).integer == 58
    assert (await Number.async_object.aget(pk=d.n1.pk)).integer == -10


async def test_lefthand_transformed_field_bitwise_or(async_db):
    from django.db.models.functions import Length
    from django.test.utils import register_lookup

    await Employee.async_object.acreate(firstname="Max", lastname="Mustermann")
    with register_lookup(CharField, Length):
        qs = Employee.async_object.annotate(bitor=F("lastname__length").bitor(48))
        result = await qs.aget()
    assert result.bitor == 58


async def test_lefthand_power(numeric_op_data):
    d = numeric_op_data
    await Number.async_object.filter(pk=d.n.pk).aupdate(integer=F("integer") ** 2, float=F("float") ** 1.5)
    n = await Number.async_object.aget(pk=d.n.pk)
    assert n.integer == 1764
    assert abs(n.float - 61.02) < 0.01


async def test_lefthand_bitwise_xor(numeric_op_data):
    d = numeric_op_data
    await Number.async_object.filter().aupdate(integer=F("integer").bitxor(48))
    assert (await Number.async_object.aget(pk=d.n.pk)).integer == 26
    assert (await Number.async_object.aget(pk=d.n1.pk)).integer == -26


async def test_lefthand_bitwise_xor_null(async_db):
    employee = await Employee.async_object.acreate(firstname="John", lastname="Doe")
    await Employee.async_object.filter().aupdate(salary=F("salary").bitxor(48))
    await employee.arefresh_from_db()
    assert employee.salary is None


async def test_lefthand_bitwise_xor_right_null(async_db):
    employee = await Employee.async_object.acreate(firstname="John", lastname="Doe", salary=48)
    await Employee.async_object.filter().aupdate(salary=F("salary").bitxor(None))
    await employee.arefresh_from_db()
    assert employee.salary is None


async def test_right_hand_addition(numeric_op_data):
    d = numeric_op_data
    await Number.async_object.filter(pk=d.n.pk).aupdate(integer=15 + F("integer"), float=42.7 + F("float"))
    n = await Number.async_object.aget(pk=d.n.pk)
    assert n.integer == 57
    assert abs(n.float - 58.200) < 0.001


async def test_right_hand_subtraction(numeric_op_data):
    d = numeric_op_data
    await Number.async_object.filter(pk=d.n.pk).aupdate(integer=15 - F("integer"), float=42.7 - F("float"))
    n = await Number.async_object.aget(pk=d.n.pk)
    assert n.integer == -27
    assert abs(n.float - 27.200) < 0.001


async def test_right_hand_multiplication(numeric_op_data):
    d = numeric_op_data
    await Number.async_object.filter(pk=d.n.pk).aupdate(integer=15 * F("integer"), float=42.7 * F("float"))
    n = await Number.async_object.aget(pk=d.n.pk)
    assert n.integer == 630
    assert abs(n.float - 661.850) < 0.001


async def test_right_hand_division(numeric_op_data):
    d = numeric_op_data
    await Number.async_object.filter(pk=d.n.pk).aupdate(integer=640 / F("integer"), float=42.7 / F("float"))
    n = await Number.async_object.aget(pk=d.n.pk)
    assert n.integer == 15
    assert abs(n.float - 2.755) < 0.001


async def test_right_hand_modulo(numeric_op_data):
    d = numeric_op_data
    await Number.async_object.filter(pk=d.n.pk).aupdate(integer=69 % F("integer"))
    assert (await Number.async_object.aget(pk=d.n.pk)).integer == 27


async def test_righthand_power(numeric_op_data):
    d = numeric_op_data
    await Number.async_object.filter(pk=d.n.pk).aupdate(integer=2 ** F("integer"), float=1.5 ** F("float"))
    n = await Number.async_object.aget(pk=d.n.pk)
    assert n.integer == 4398046511104
    assert abs(n.float - 536.308) < 0.001


# ---------------------------------------------------------------------------
# FTimeDeltaTests
# ---------------------------------------------------------------------------


async def test_multiple_query_compilation(time_delta_data):
    queryset = Experiment.async_object.filter(end__lt=F("start") + datetime.timedelta(hours=1))
    q1 = str(queryset.query)
    q2 = str(queryset.query)
    assert q1 == q2


async def test_query_clone(time_delta_data):
    qs = Experiment.async_object.filter(end__lt=F("start") + datetime.timedelta(hours=1))
    qs2 = qs.all()
    [_ async for _ in qs]
    [_ async for _ in qs2]


async def test_delta_add(time_delta_data):
    d = time_delta_data
    for i, delta in enumerate(d.deltas):
        test_set = [e.name async for e in Experiment.async_object.filter(end__lt=F("start") + delta)]
        assert test_set == d.expnames[:i]

        test_set = [e.name async for e in Experiment.async_object.filter(end__lt=delta + F("start"))]
        assert test_set == d.expnames[:i]

        test_set = [e.name async for e in Experiment.async_object.filter(end__lte=F("start") + delta)]
        assert test_set == d.expnames[: i + 1]


async def test_delta_subtract(time_delta_data):
    d = time_delta_data
    for i, delta in enumerate(d.deltas):
        test_set = [e.name async for e in Experiment.async_object.filter(start__gt=F("end") - delta)]
        assert test_set == d.expnames[:i]

        test_set = [e.name async for e in Experiment.async_object.filter(start__gte=F("end") - delta)]
        assert test_set == d.expnames[: i + 1]


async def test_delta_exclude(time_delta_data):
    d = time_delta_data
    for i, delta in enumerate(d.deltas):
        test_set = [e.name async for e in Experiment.async_object.exclude(end__lt=F("start") + delta)]
        assert test_set == d.expnames[i:]

        test_set = [e.name async for e in Experiment.async_object.exclude(end__lte=F("start") + delta)]
        assert test_set == d.expnames[i + 1 :]


async def test_delta_date_comparison(time_delta_data):
    d = time_delta_data
    for i, days in enumerate(d.days_long):
        test_set = [e.name async for e in Experiment.async_object.filter(completed__lt=F("assigned") + days)]
        assert test_set == d.expnames[:i]

        test_set = [e.name async for e in Experiment.async_object.filter(completed__lte=F("assigned") + days)]
        assert test_set == d.expnames[: i + 1]


async def test_datetime_and_durationfield_addition_with_filter(time_delta_data):
    test_set = Experiment.async_object.filter(end=F("start") + F("estimated_time"))
    assert await test_set.acount() > 0
    got_names = [e.name async for e in test_set]
    expected_names = [e.name async for e in Experiment.async_object.all() if e.end == e.start + e.estimated_time]
    assert got_names == expected_names


async def test_datetime_and_duration_field_addition_with_annotate_and_no_output_field(time_delta_data):
    test_set = Experiment.async_object.annotate(estimated_end=F("start") + F("estimated_time"))
    got = [(e.estimated_end, e.start + e.estimated_time) async for e in test_set]
    for actual, expected in got:
        assert actual == expected


async def test_datetime_subtraction_with_annotate_and_no_output_field(time_delta_data):
    if not _features().supports_temporal_subtraction:
        pytest.skip("DB does not support temporal subtraction")
    test_set = Experiment.async_object.annotate(calculated_duration=F("end") - F("start"))
    got = [(e.calculated_duration, e.end - e.start) async for e in test_set]
    for actual, expected in got:
        assert actual == expected


async def test_mixed_comparisons1(time_delta_data):
    d = time_delta_data
    for i, delay in enumerate(d.delays):
        test_set = [e.name async for e in Experiment.async_object.filter(assigned__gt=F("start") - delay)]
        assert test_set == d.expnames[:i]

        test_set = [e.name async for e in Experiment.async_object.filter(assigned__gte=F("start") - delay)]
        assert test_set == d.expnames[: i + 1]


async def test_mixed_comparisons2(time_delta_data):
    d = time_delta_data
    for i, raw_delay in enumerate(d.delays):
        delay = datetime.timedelta(raw_delay.days)
        test_set = [e.name async for e in Experiment.async_object.filter(start__lt=F("assigned") + delay)]
        assert test_set == d.expnames[:i]

        test_set = [
            e.name
            async for e in Experiment.async_object.filter(start__lte=F("assigned") + delay + datetime.timedelta(1))
        ]
        assert test_set == d.expnames[: i + 1]


async def test_delta_update(time_delta_data):
    d = time_delta_data
    for delta in d.deltas:
        exps = [e async for e in Experiment.async_object.all()]
        expected_durations = [e.duration() for e in exps]
        expected_starts = [e.start + delta for e in exps]
        expected_ends = [e.end + delta for e in exps]

        await Experiment.async_object.filter().aupdate(start=F("start") + delta, end=F("end") + delta)
        exps = [e async for e in Experiment.async_object.all()]
        new_starts = [e.start for e in exps]
        new_ends = [e.end for e in exps]
        new_durations = [e.duration() for e in exps]
        assert expected_starts == new_starts
        assert expected_ends == new_ends
        assert expected_durations == new_durations


async def test_invalid_operator(time_delta_data):
    with pytest.raises(DatabaseError):
        [_ async for _ in Experiment.async_object.filter(start=F("start") * datetime.timedelta(0))]


async def test_durationfield_add(time_delta_data):
    zeros = [e.name async for e in Experiment.async_object.filter(start=F("start") + F("estimated_time"))]
    assert zeros == ["e0"]

    end_less = [e.name async for e in Experiment.async_object.filter(end__lt=F("start") + F("estimated_time"))]
    assert end_less == ["e2"]

    delta_math = [
        e.name
        async for e in Experiment.async_object.filter(
            end__gte=F("start") + F("estimated_time") + datetime.timedelta(hours=1)
        )
    ]
    assert delta_math == ["e4"]

    queryset = Experiment.async_object.annotate(
        shifted=ExpressionWrapper(
            F("start") + Value(None, output_field=DurationField()),
            output_field=DateTimeField(),
        )
    )
    assert (await queryset.afirst()).shifted is None


async def test_durationfield_multiply_divide(time_delta_data):
    await Experiment.async_object.filter().aupdate(scalar=2)
    tests = [
        (Decimal(2), 2),
        (F("scalar"), 2),
        (2, 2),
        (3.2, 3.2),
    ]
    for expr, scalar in tests:
        qs = Experiment.async_object.annotate(
            multiplied=ExpressionWrapper(
                expr * F("estimated_time"),
                output_field=DurationField(),
            ),
            divided=ExpressionWrapper(
                F("estimated_time") / expr,
                output_field=DurationField(),
            ),
        )
        async for experiment in qs:
            assert experiment.multiplied == experiment.estimated_time * scalar
            assert experiment.divided == experiment.estimated_time / scalar


async def test_duration_expressions(time_delta_data):
    d = time_delta_data
    for delta in d.deltas:
        qs = Experiment.async_object.annotate(duration=F("estimated_time") + delta)
        async for obj in qs:
            assert obj.duration == obj.estimated_time + delta


async def test_date_subtraction(time_delta_data):
    if not _features().supports_temporal_subtraction:
        pytest.skip("DB does not support temporal subtraction")
    queryset = Experiment.async_object.annotate(
        completion_duration=F("completed") - F("assigned"),
    )
    at_least_5_days = {e.name async for e in queryset.filter(completion_duration__gte=datetime.timedelta(days=5))}
    assert at_least_5_days == {"e3", "e4", "e5"}

    at_least_120_days = {e.name async for e in queryset.filter(completion_duration__gte=datetime.timedelta(days=120))}
    assert at_least_120_days == {"e5"}

    less_than_5_days = {e.name async for e in queryset.filter(completion_duration__lt=datetime.timedelta(days=5))}
    assert less_than_5_days == {"e0", "e1", "e2"}

    queryset = Experiment.async_object.annotate(
        difference=F("completed") - Value(None, output_field=DateField()),
    )
    assert (await queryset.afirst()).difference is None

    queryset = Experiment.async_object.annotate(
        shifted=ExpressionWrapper(
            F("completed") - Value(None, output_field=DurationField()),
            output_field=DateField(),
        )
    )
    assert (await queryset.afirst()).shifted is None


async def test_date_subquery_subtraction(time_delta_data):
    if not _features().supports_temporal_subtraction:
        pytest.skip("DB does not support temporal subtraction")
    subquery = Experiment.async_object.filter(pk=OuterRef("pk")).values("completed")
    queryset = Experiment.async_object.annotate(
        difference=subquery - F("completed"),
    ).filter(difference=datetime.timedelta())
    assert await queryset.aexists()


async def test_date_case_subtraction(time_delta_data):
    if not _features().supports_temporal_subtraction:
        pytest.skip("DB does not support temporal subtraction")
    d = time_delta_data
    queryset = Experiment.async_object.annotate(
        date_case=Case(
            When(Q(name="e0"), then=F("completed")),
            output_field=DateField(),
        ),
        completed_value=Value(
            d.e0.completed,
            output_field=DateField(),
        ),
        difference=F("date_case") - F("completed_value"),
    ).filter(difference=datetime.timedelta())
    assert await queryset.aget() == d.e0


async def test_time_subtraction(async_db):
    if not _features().supports_temporal_subtraction:
        pytest.skip("DB does not support temporal subtraction")
    await Time.async_object.acreate(time=datetime.time(12, 30, 15, 2345))
    queryset = Time.async_object.annotate(
        difference=F("time") - Value(datetime.time(11, 15, 0)),
    )
    assert (await queryset.aget()).difference == datetime.timedelta(hours=1, minutes=15, seconds=15, microseconds=2345)

    queryset = Time.async_object.annotate(
        difference=F("time") - Value(None, output_field=TimeField()),
    )
    assert (await queryset.afirst()).difference is None

    queryset = Time.async_object.annotate(
        shifted=ExpressionWrapper(
            F("time") - Value(None, output_field=DurationField()),
            output_field=TimeField(),
        )
    )
    assert (await queryset.afirst()).shifted is None


async def test_time_subquery_subtraction(async_db):
    if not _features().supports_temporal_subtraction:
        pytest.skip("DB does not support temporal subtraction")
    await Time.async_object.acreate(time=datetime.time(12, 30, 15, 2345))
    subquery = Time.async_object.filter(pk=OuterRef("pk")).values("time")
    queryset = Time.async_object.annotate(
        difference=subquery - F("time"),
    ).filter(difference=datetime.timedelta())
    assert await queryset.aexists()


async def test_datetime_subtraction(time_delta_data):
    if not _features().supports_temporal_subtraction:
        pytest.skip("DB does not support temporal subtraction")
    under_estimate = [e.name async for e in Experiment.async_object.filter(estimated_time__gt=F("end") - F("start"))]
    assert under_estimate == ["e2"]

    over_estimate = [e.name async for e in Experiment.async_object.filter(estimated_time__lt=F("end") - F("start"))]
    assert over_estimate == ["e4"]

    queryset = Experiment.async_object.annotate(
        difference=F("start") - Value(None, output_field=DateTimeField()),
    )
    assert (await queryset.afirst()).difference is None

    queryset = Experiment.async_object.annotate(
        shifted=ExpressionWrapper(
            F("start") - Value(None, output_field=DurationField()),
            output_field=DateTimeField(),
        )
    )
    assert (await queryset.afirst()).shifted is None


async def test_datetime_subquery_subtraction(time_delta_data):
    if not _features().supports_temporal_subtraction:
        pytest.skip("DB does not support temporal subtraction")
    subquery = Experiment.async_object.filter(pk=OuterRef("pk")).values("start")
    queryset = Experiment.async_object.annotate(
        difference=subquery - F("start"),
    ).filter(difference=datetime.timedelta())
    assert await queryset.aexists()


async def test_datetime_subtraction_microseconds(time_delta_data):
    if not _features().supports_temporal_subtraction:
        pytest.skip("DB does not support temporal subtraction")
    delta = datetime.timedelta(microseconds=8999999999999999)
    await Experiment.async_object.filter().aupdate(end=F("start") + delta)
    qs = Experiment.async_object.annotate(delta=F("end") - F("start"))
    async for e in qs:
        assert e.delta == delta


async def test_duration_with_datetime(time_delta_data):
    d = time_delta_data
    over_estimate = (
        Experiment.async_object.exclude(name="e1").filter(completed__gt=d.stime + F("estimated_time")).order_by("name")
    )
    assert [e.name async for e in over_estimate] == ["e3", "e4", "e5"]


async def test_duration_with_datetime_microseconds(time_delta_data):
    delta = datetime.timedelta(microseconds=8999999999999999)
    qs = Experiment.async_object.annotate(
        dt=ExpressionWrapper(
            F("start") + delta,
            output_field=DateTimeField(),
        )
    )
    async for e in qs:
        assert e.dt == e.start + delta


async def test_date_minus_duration(time_delta_data):
    more_than_4_days = Experiment.async_object.filter(assigned__lt=F("completed") - Value(datetime.timedelta(days=4)))
    got = [e.name async for e in more_than_4_days]
    assert got == ["e3", "e4", "e5"]


async def test_negative_timedelta_update(time_delta_data):
    experiments = (
        Experiment.async_object.filter(name="e0")
        .annotate(start_sub_seconds=F("start") + datetime.timedelta(seconds=-30))
        .annotate(start_sub_minutes=F("start_sub_seconds") + datetime.timedelta(minutes=-30))
        .annotate(start_sub_hours=F("start_sub_minutes") + datetime.timedelta(hours=-2))
        .annotate(new_start=F("start_sub_hours") + datetime.timedelta(days=-2))
    )
    expected_start = datetime.datetime(2010, 6, 23, 9, 45, 0)
    experiments = experiments.annotate(new_start=F("new_start") + datetime.timedelta(microseconds=-30))
    expected_start += datetime.timedelta(microseconds=+746970)
    await experiments.aupdate(start=F("new_start"))
    e0 = await Experiment.async_object.aget(name="e0")
    assert e0.start == expected_start


# ---------------------------------------------------------------------------
# ValueTests
# ---------------------------------------------------------------------------


async def test_update_TimeField_using_Value(async_db):
    await Time.async_object.acreate()
    await Time.async_object.filter().aupdate(time=Value(datetime.time(1), output_field=TimeField()))
    assert (await Time.async_object.aget()).time == datetime.time(1)


async def test_update_UUIDField_using_Value(async_db):
    await UUID.async_object.acreate()
    await UUID.async_object.filter().aupdate(
        uuid=Value(uuid.UUID("12345678901234567890123456789012"), output_field=UUIDField())
    )
    assert (await UUID.async_object.aget()).uuid == uuid.UUID("12345678901234567890123456789012")


def test_Value_deconstruct():
    value = Value("name")
    path, args, kwargs = value.deconstruct()
    assert path == "django.db.models.Value"
    assert args == (value.value,)
    assert kwargs == {}


def test_Value_deconstruct_output_field():
    value = Value("name", output_field=CharField())
    path, args, kwargs = value.deconstruct()
    assert path == "django.db.models.Value"
    assert args == (value.value,)
    assert len(kwargs) == 1
    assert kwargs["output_field"].deconstruct() == CharField().deconstruct()


def test_Value_repr():
    tests = [
        (None, "Value(None)"),
        ("str", "Value('str')"),
        (True, "Value(True)"),
        (42, "Value(42)"),
        (datetime.datetime(2019, 5, 15), "Value(datetime.datetime(2019, 5, 15, 0, 0))"),
        (Decimal("3.14"), "Value(Decimal('3.14'))"),
    ]
    for value, expected in tests:
        assert repr(Value(value)) == expected


def test_Value_equal():
    value = Value("name")
    assert value == Value("name")
    assert value != Value("username")


def test_Value_hash():
    d = {Value("name"): "Bob"}
    assert Value("name") in d
    assert d[Value("name")] == "Bob"


def test_Value_equal_output_field():
    value = Value("name", output_field=CharField())
    same_value = Value("name", output_field=CharField())
    other_value = Value("name", output_field=TimeField())
    no_output_field = Value("name")
    assert value == same_value
    assert value != other_value
    assert value != no_output_field


async def test_compile_unresolved(async_db):
    from django_async_backend.db import async_connections as _ac

    conn = _ac[DEFAULT_DB_ALIAS]
    # Use Django's sync connection object for the compiler (the query/compiler
    # path is purely Python, no DB I/O).
    compiler = Time.async_object.all().query.get_compiler(connection=conn)
    value = Value("foo")
    sql, params = value.as_sql(compiler, conn)
    assert sql == "%s"
    assert tuple(params) == ("foo",)
    value = Value("foo", output_field=CharField())
    sql, params = value.as_sql(compiler, conn)
    assert sql == "%s"
    assert tuple(params) == ("foo",)


async def test_output_field_decimalfield(async_db):
    await Time.async_object.acreate()
    time = await Time.async_object.annotate(one=Value(1, output_field=DecimalField())).afirst()
    assert time.one == 1


async def test_output_field_is_none_error(async_db):
    with pytest.raises(OutputFieldIsNoneError):
        await Employee.async_object.annotate(custom_expression=Value(None)).afirst()


def test_output_field_or_none_property_not_cached():
    expression = Value(None, output_field=None)
    assert expression._output_field_or_none is None
    expression.output_field = BooleanField()
    assert isinstance(expression._output_field_or_none, BooleanField)


def test_resolve_output_field():
    value_types = [
        ("str", CharField),
        (True, BooleanField),
        (42, IntegerField),
        (3.14, FloatField),
        (datetime.date(2019, 5, 15), DateField),
        (datetime.datetime(2019, 5, 15), DateTimeField),
        (datetime.time(3, 16), TimeField),
        (datetime.timedelta(1), DurationField),
        (Decimal("3.14"), DecimalField),
        (b"", BinaryField),
        (uuid.uuid4(), UUIDField),
    ]
    for value, output_field_type in value_types:
        expr = Value(value)
        assert isinstance(expr.output_field, output_field_type)


def test_resolve_output_field_failure():
    with pytest.raises(FieldError, match="Cannot resolve expression type, unknown output_field"):
        Value(object()).output_field


def test_output_field_does_not_create_broken_validators():
    value_types = [
        "str",
        True,
        42,
        3.14,
        datetime.date(2019, 5, 15),
        datetime.datetime(2019, 5, 15),
        datetime.time(3, 16),
        datetime.timedelta(1),
        Decimal("3.14"),
        b"",
        uuid.uuid4(),
    ]
    for value in value_types:
        field = Value(value)._resolve_output_field()
        field.clean(value, model_instance=None)


# ---------------------------------------------------------------------------
# ExistsTests
# ---------------------------------------------------------------------------


async def test_exists_optimizations(async_db):
    from tests.fixtures.query_counter import async_capture_queries

    now = datetime.datetime.now(tz=datetime.UTC)
    await Experiment.async_object.acreate(
        name="test",
        assigned=now.date(),
        completed=now.date(),
        estimated_time=datetime.timedelta(hours=1),
        start=now,
        end=now,
    )
    connection = async_connections[DEFAULT_DB_ALIAS]
    async with async_capture_queries() as queries:
        results = [
            r
            async for r in Experiment.async_object.values(
                exists=Exists(
                    Experiment.async_object.order_by("pk"),
                ),
            ).order_by()
        ]
    assert len(results) == 1
    assert results[0]["exists"] is True
    assert len(queries) == 1
    sql = queries[0]["sql"]
    assert connection.ops.quote_name(Experiment._meta.pk.column) not in sql
    assert "LIMIT" in sql
    assert "ORDER BY" not in sql


async def test_negated_empty_exists(async_db):
    manager = await Manager.async_object.acreate(name="m")
    qs = Manager.async_object.filter(~Exists(Manager.async_object.none()) & Q(pk=manager.pk))
    assert [m async for m in qs] == [manager]


async def test_select_negated_empty_exists(async_db):
    manager = await Manager.async_object.acreate(name="m")
    qs = Manager.async_object.annotate(not_exists=~Exists(Manager.async_object.none())).filter(pk=manager.pk)
    assert [m async for m in qs] == [manager]
    assert (await qs.aget()).not_exists is True


async def test_filter_by_empty_exists(async_db):
    manager = await Manager.async_object.acreate(name="m")
    qs = Manager.async_object.annotate(exists=Exists(Manager.async_object.none())).filter(pk=manager.pk, exists=False)
    assert [m async for m in qs] == [manager]
    assert (await qs.aget()).exists is False


async def test_annotate_by_empty_custom_exists(async_db):
    class CustomExists(Exists):
        template = Subquery.template

    manager = await Manager.async_object.acreate(name="m")
    qs = Manager.async_object.annotate(exists=CustomExists(Manager.async_object.none()))
    assert [m async for m in qs] == [manager]
    assert (await qs.aget()).exists is False


# ---------------------------------------------------------------------------
# FieldTransformTests
# ---------------------------------------------------------------------------


async def test_month_aggregation(field_transform_data):
    result = await Experiment.async_object.aaggregate(month_count=Count("assigned__month"))
    assert result == {"month_count": 1}


async def test_transform_in_values(field_transform_data):
    got = [r async for r in Experiment.async_object.values("assigned__month")]
    assert got == [{"assigned__month": 6}]


async def test_multiple_transforms_in_values(field_transform_data):
    got = [r async for r in Experiment.async_object.values("end__date__month")]
    assert got == [{"end__date__month": 6}]


# ---------------------------------------------------------------------------
# ReprTests (pure Python)
# ---------------------------------------------------------------------------


def test_repr_expressions():
    assert repr(Case(When(a=1))) == "<Case: CASE WHEN <Q: (AND: ('a', 1))> THEN Value(None), ELSE Value(None)>"
    assert (
        repr(When(Q(age__gte=18), then=Value("legal")))
        == "<When: WHEN <Q: (AND: ('age__gte', 18))> THEN Value('legal')>"
    )
    assert repr(Col("alias", "field")) == "Col(alias, field)"
    assert (
        repr(ColPairs("alias", ["t1", "t2"], ["s1", "s2"], "f")) == "ColPairs('alias', ['t1', 't2'], ['s1', 's2'], 'f')"
    )
    assert repr(F("published")) == "F(published)"
    assert repr(F("cost") + F("tax")) == "<CombinedExpression: F(cost) + F(tax)>"
    assert repr(ExpressionWrapper(F("cost") + F("tax"), IntegerField())) == "ExpressionWrapper(F(cost) + F(tax))"
    assert repr(Func("published", function="TO_CHAR")) == "Func(F(published), function=TO_CHAR)"
    assert repr(F("published")[0:2]) == "Sliced(F(published), slice(0, 2, None))"
    assert repr(OuterRef("name")[1:5]) == "Sliced(OuterRef(name), slice(1, 5, None))"
    assert repr(OrderBy(Value(1))) == "OrderBy(Value(1), descending=False)"
    assert repr(RawSQL("table.col", [])) == "RawSQL(table.col, [])"
    assert repr(Ref("sum_cost", Sum("cost"))) == "Ref(sum_cost, Sum(F(cost)))"
    assert repr(Value(1)) == "Value(1)"
    assert repr(ExpressionList(F("col"), F("anothercol"))) == "ExpressionList(F(col), F(anothercol))"
    assert (
        repr(ExpressionList(OrderBy(F("col"), descending=False))) == "ExpressionList(OrderBy(F(col), descending=False))"
    )


def test_repr_functions():
    assert repr(Coalesce("a", "b")) == "Coalesce(F(a), F(b))"
    assert repr(Concat("a", "b")) == "Concat(ConcatPair(F(a), F(b)))"
    assert repr(Length("a")) == "Length(F(a))"
    assert repr(Lower("a")) == "Lower(F(a))"
    assert repr(Substr("a", 1, 3)) == "Substr(F(a), Value(1), Value(3))"
    assert repr(Upper("a")) == "Upper(F(a))"


def test_repr_aggregates():
    assert repr(Avg("a")) == "Avg(F(a))"
    assert repr(Count("a")) == "Count(F(a))"
    assert repr(Count("*")) == "Count('*')"
    assert repr(Max("a")) == "Max(F(a))"
    assert repr(Min("a")) == "Min(F(a))"
    assert repr(StdDev("a")) == "StdDev(F(a), sample=False)"
    assert repr(Sum("a")) == "Sum(F(a))"
    assert repr(Variance("a", sample=True)) == "Variance(F(a), sample=True)"


def test_repr_distinct_aggregates():
    assert repr(Count("a", distinct=True)) == "Count(F(a), distinct=True)"
    assert repr(Count("*", distinct=True)) == "Count('*', distinct=True)"


def test_repr_filtered_aggregates():
    flt = Q(a=1)
    assert repr(Avg("a", filter=flt)) == "Avg(F(a), filter=(AND: ('a', 1)))"
    assert repr(Count("a", filter=flt)) == "Count(F(a), filter=(AND: ('a', 1)))"
    assert repr(Max("a", filter=flt)) == "Max(F(a), filter=(AND: ('a', 1)))"
    assert repr(Min("a", filter=flt)) == "Min(F(a), filter=(AND: ('a', 1)))"
    assert repr(StdDev("a", filter=flt)) == "StdDev(F(a), filter=(AND: ('a', 1)), sample=False)"
    assert repr(Sum("a", filter=flt)) == "Sum(F(a), filter=(AND: ('a', 1)))"
    assert repr(Variance("a", sample=True, filter=flt)) == "Variance(F(a), filter=(AND: ('a', 1)), sample=True)"
    assert repr(Count("a", filter=flt, distinct=True)) == "Count(F(a), distinct=True, filter=(AND: ('a', 1)))"


# ---------------------------------------------------------------------------
# CombinableTests (pure Python)
# ---------------------------------------------------------------------------


BITWISE_MSG = re.escape("Use .bitand(), .bitor(), and .bitxor() for bitwise logical operations.")


def test_Combinable_negation():
    c = Combinable()
    assert -c == c * -1


def test_Combinable_and():
    with pytest.raises(NotImplementedError, match=BITWISE_MSG):
        Combinable() & Combinable()


def test_Combinable_or():
    with pytest.raises(NotImplementedError, match=BITWISE_MSG):
        Combinable() | Combinable()


def test_Combinable_xor():
    with pytest.raises(NotImplementedError, match=BITWISE_MSG):
        Combinable() ^ Combinable()


def test_Combinable_reversed_and():
    with pytest.raises(NotImplementedError, match=BITWISE_MSG):
        object() & Combinable()


def test_Combinable_reversed_or():
    with pytest.raises(NotImplementedError, match=BITWISE_MSG):
        object() | Combinable()


def test_Combinable_reversed_xor():
    with pytest.raises(NotImplementedError, match=BITWISE_MSG):
        object() ^ Combinable()


# ---------------------------------------------------------------------------
# CombinedExpressionTests (pure Python)
# ---------------------------------------------------------------------------


def test_resolve_output_field_positive_integer():
    connectors = [
        Combinable.ADD,
        Combinable.MUL,
        Combinable.DIV,
        Combinable.MOD,
        Combinable.POW,
    ]
    for connector in connectors:
        expr = CombinedExpression(
            Expression(PositiveIntegerField()),
            connector,
            Expression(PositiveIntegerField()),
        )
        assert isinstance(expr.output_field, PositiveIntegerField)


def test_resolve_output_field_number():
    tests = [
        (IntegerField, AutoField, IntegerField),
        (AutoField, IntegerField, IntegerField),
        (IntegerField, DecimalField, DecimalField),
        (DecimalField, IntegerField, DecimalField),
        (IntegerField, FloatField, FloatField),
        (FloatField, IntegerField, FloatField),
    ]
    connectors = [
        Combinable.ADD,
        Combinable.SUB,
        Combinable.MUL,
        Combinable.DIV,
        Combinable.MOD,
    ]
    for lhs, rhs, combined in tests:
        for connector in connectors:
            expr = CombinedExpression(Expression(lhs()), connector, Expression(rhs()))
            assert isinstance(expr.output_field, combined)


def test_resolve_output_field_with_null():
    def null():
        return Value(None)

    tests = [
        (AutoField, Combinable.ADD, null),
        (DecimalField, Combinable.ADD, null),
        (FloatField, Combinable.ADD, null),
        (IntegerField, Combinable.ADD, null),
        (IntegerField, Combinable.SUB, null),
        (null, Combinable.ADD, IntegerField),
        (DateField, Combinable.ADD, null),
        (DateTimeField, Combinable.ADD, null),
        (DurationField, Combinable.ADD, null),
        (TimeField, Combinable.ADD, null),
        (TimeField, Combinable.SUB, null),
        (null, Combinable.ADD, DateTimeField),
        (DateField, Combinable.SUB, null),
    ]
    for lhs, connector, rhs in tests:
        expr = CombinedExpression(Expression(lhs()), connector, Expression(rhs()))
        with pytest.raises(FieldError, match="Cannot infer type"):
            expr.output_field


def test_resolve_output_field_numbers_with_null():
    test_values = [
        (3.14159, None, FloatField),
        (None, 3.14159, FloatField),
        (None, 42, IntegerField),
        (42, None, IntegerField),
        (None, Decimal("3.14"), DecimalField),
        (Decimal("3.14"), None, DecimalField),
    ]
    connectors = [
        Combinable.ADD,
        Combinable.SUB,
        Combinable.MUL,
        Combinable.DIV,
        Combinable.MOD,
        Combinable.POW,
    ]
    for lhs, rhs, expected_output_field in test_values:
        for connector in connectors:
            expr = CombinedExpression(Value(lhs), connector, Value(rhs))
            assert isinstance(expr.output_field, expected_output_field)


def test_resolve_output_field_dates():
    tests = [
        (DateField, Combinable.ADD, DateField, FieldError),
        (DateTimeField, Combinable.ADD, DateTimeField, FieldError),
        (TimeField, Combinable.ADD, TimeField, FieldError),
        (DurationField, Combinable.ADD, DurationField, DurationField),
        (DateField, Combinable.ADD, DurationField, DateTimeField),
        (DateTimeField, Combinable.ADD, DurationField, DateTimeField),
        (TimeField, Combinable.ADD, DurationField, TimeField),
        (DurationField, Combinable.ADD, DateField, DateTimeField),
        (DurationField, Combinable.ADD, DateTimeField, DateTimeField),
        (DurationField, Combinable.ADD, TimeField, TimeField),
        (DateField, Combinable.SUB, DateField, DurationField),
        (DateTimeField, Combinable.SUB, DateTimeField, DurationField),
        (TimeField, Combinable.SUB, TimeField, DurationField),
        (DurationField, Combinable.SUB, DurationField, DurationField),
        (DateField, Combinable.SUB, DurationField, DateTimeField),
        (DateTimeField, Combinable.SUB, DurationField, DateTimeField),
        (TimeField, Combinable.SUB, DurationField, TimeField),
        (DurationField, Combinable.SUB, DateField, FieldError),
        (DurationField, Combinable.SUB, DateTimeField, FieldError),
    ]
    for lhs, connector, rhs, combined in tests:
        expr = CombinedExpression(Expression(lhs()), connector, Expression(rhs()))
        if isinstance(combined, type) and issubclass(combined, Exception):
            with pytest.raises(combined, match="Cannot infer type"):
                expr.output_field
        else:
            assert isinstance(expr.output_field, combined)


async def test_mixed_char_date_with_annotate(async_db):
    queryset = Experiment.async_object.annotate(nonsense=F("name") + F("assigned"))
    msg = (
        "Cannot infer type of '\\+' expression involving these types: CharField, DateField. You must set output_field."
    )
    with pytest.raises(FieldError, match=msg):
        [_ async for _ in queryset]


# ---------------------------------------------------------------------------
# ExpressionWrapperTests (pure Python)
# ---------------------------------------------------------------------------


def test_empty_group_by():
    expr = ExpressionWrapper(Value(3), output_field=IntegerField())
    assert expr.get_group_by_cols() == []


def test_non_empty_group_by():
    value = Value("f")
    value.output_field = None
    expr = ExpressionWrapper(Lower(value), output_field=IntegerField())
    group_by_cols = expr.get_group_by_cols()
    assert group_by_cols == [expr.expression]
    assert group_by_cols[0].output_field == expr.output_field


# ---------------------------------------------------------------------------
# NegatedExpressionTests
# ---------------------------------------------------------------------------


def test_negated_invert():
    f = F("field")
    assert ~f == NegatedExpression(f)
    assert ~~f is not f
    assert ~~f == f


async def test_negated_filter(negated_expression_data):
    d = negated_expression_data
    assert [c async for c in Company.async_object.filter(~F("based_in_eu"))] == [d.non_eu_company]

    qs = Company.async_object.annotate(eu_required=~Value(False))
    assert [c async for c in qs.filter(based_in_eu=F("eu_required")).order_by("eu_required")] == [d.eu_company]
    assert [c async for c in qs.filter(based_in_eu=~~F("eu_required"))] == [d.eu_company]
    assert [c async for c in qs.filter(based_in_eu=~F("eu_required"))] == [d.non_eu_company]
    assert [c async for c in qs.filter(based_in_eu=~F("based_in_eu"))] == []


async def test_negated_values(negated_expression_data):
    got = [
        r
        async for r in Company.async_object.annotate(negated=~F("based_in_eu"))
        .values_list("name", "negated")
        .order_by("name")
    ]
    assert got == [("Example Inc.", False), ("Foobar Ltd.", True)]


# ---------------------------------------------------------------------------
# OrderByTests (pure Python)
# ---------------------------------------------------------------------------


def test_OrderBy_equal():
    assert OrderBy(F("field"), nulls_last=True) == OrderBy(F("field"), nulls_last=True)
    assert OrderBy(F("field"), nulls_last=True) != OrderBy(F("field"))


def test_OrderBy_hash():
    assert hash(OrderBy(F("field"), nulls_last=True)) == hash(OrderBy(F("field"), nulls_last=True))
    assert hash(OrderBy(F("field"), nulls_last=True)) != hash(OrderBy(F("field")))


def test_OrderBy_nulls_false():
    msg = "nulls_first and nulls_last values must be True or None."
    with pytest.raises(ValueError, match=msg):
        OrderBy(F("field"), nulls_first=False)
    with pytest.raises(ValueError, match=msg):
        OrderBy(F("field"), nulls_last=False)
    with pytest.raises(ValueError, match=msg):
        F("field").asc(nulls_first=False)
    with pytest.raises(ValueError, match=msg):
        F("field").desc(nulls_last=False)
