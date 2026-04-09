import json

from test_app.models import TestModel


async def test_aexplain(async_db):
    explain_output = await TestModel.async_object.aexplain()

    assert "Seq Scan on test_model  (cost=" in explain_output
    assert "Planning Time" not in explain_output
    assert "Execution Time" not in explain_output


async def test_aexplain_with_format(async_db):
    explain_output = json.loads(await TestModel.async_object.aexplain(format="JSON"))
    assert isinstance(explain_output, list)
    assert any(plan.get("Plan", {}).get("Node Type") == "Seq Scan" for plan in explain_output)


async def test_aexplain_with_options(async_db):
    explain_output = await TestModel.async_object.aexplain(analyze=True, verbose=True)
    assert "Seq Scan" in explain_output
    assert "test_model" in explain_output
    assert "Planning Time" in explain_output
    assert "Execution Time" in explain_output
