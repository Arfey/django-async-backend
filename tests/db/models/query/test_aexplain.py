import json
from django_async_backend.test import AsyncioTestCase
from test_app.models import TestModel
from django.test import TestCase


class TestMock(TestCase):
    def test_mock(self):
        pass


class TestAExplain(AsyncioTestCase):
    async def test_aexplain(self):
        explain_output = await TestModel.async_object.aexplain()

        self.assertIn("Seq Scan on test_model  (cost=", explain_output, "Explain output should contain 'Seq Scan'")
        self.assertNotIn("Planning Time", explain_output, "'Planning Time' should not be present in explain output")
        self.assertNotIn("Execution Time", explain_output, "'Execution Time' should not be present in explain output")

    async def test_aexplain_with_format(self):
        explain_output = json.loads(await TestModel.async_object.aexplain(format="JSON"))
        self.assertTrue(
            isinstance(explain_output, list),
            "Explain output should be a list"
        )
        self.assertTrue(
            any(plan.get("Plan", {}).get("Node Type") == "Seq Scan" for plan in explain_output),
            "Explain output should contain a 'Seq Scan' plan"
        )

    async def test_aexplain_with_options(self):
        explain_output = await TestModel.async_object.aexplain(analyze=True, verbose=True)
        self.assertIn("Seq Scan", explain_output, "Explain output should contain 'Seq Scan'")
        self.assertIn("test_model", explain_output, "Explain output should reference the correct table")
        self.assertIn("Planning Time", explain_output, "Explain output should contain 'Planning Time'")
        self.assertIn("Execution Time", explain_output, "Explain output should contain 'Execution Time'")
