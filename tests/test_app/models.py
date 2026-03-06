from django.db import models


class TestModel(models.Model):
    name = models.CharField(max_length=255, unique=True)
    value = models.IntegerField(null=True)

    class Meta:
        db_table = "test_model"
