import uuid

from django.db import models

from django_async_backend.db.models.base import AsyncModel
from django_async_backend.db.models.manager import AsyncManager


class Manager(AsyncModel, models.Model):
    name = models.CharField(max_length=50)
    secretary = models.ForeignKey("Employee", models.CASCADE, null=True, related_name="managers")

    async_object = AsyncManager()


class Employee(AsyncModel, models.Model):
    firstname = models.CharField(max_length=50)
    lastname = models.CharField(max_length=50)
    salary = models.IntegerField(blank=True, null=True)
    manager = models.ForeignKey(Manager, models.CASCADE, null=True)
    based_in_eu = models.BooleanField(default=False)

    async_object = AsyncManager()

    def __str__(self):
        return "%s %s" % (self.firstname, self.lastname)


class RemoteEmployee(Employee):
    adjusted_salary = models.IntegerField()

    async_object = AsyncManager()


class Company(AsyncModel, models.Model):
    name = models.CharField(max_length=100)
    num_employees = models.PositiveIntegerField()
    num_chairs = models.PositiveIntegerField()
    ceo = models.ForeignKey(
        Employee,
        models.CASCADE,
        related_name="company_ceo_set",
    )
    point_of_contact = models.ForeignKey(
        Employee,
        models.SET_NULL,
        related_name="company_point_of_contact_set",
        null=True,
    )
    based_in_eu = models.BooleanField(default=False)

    async_object = AsyncManager()

    def __str__(self):
        return self.name


class Number(AsyncModel, models.Model):
    integer = models.BigIntegerField(db_column="the_integer")
    float = models.FloatField(null=True, db_column="the_float")
    decimal_value = models.DecimalField(max_digits=20, decimal_places=17, null=True)

    async_object = AsyncManager()


class Experiment(AsyncModel, models.Model):
    name = models.CharField(max_length=24)
    assigned = models.DateField()
    completed = models.DateField()
    estimated_time = models.DurationField()
    start = models.DateTimeField()
    end = models.DateTimeField()
    scalar = models.IntegerField(null=True)

    async_object = AsyncManager()

    class Meta:
        db_table = "expressions_ExPeRiMeNt"
        ordering = ("name",)

    def duration(self):
        return self.end - self.start


class Result(AsyncModel, models.Model):
    experiment = models.ForeignKey(Experiment, models.CASCADE)
    result_time = models.DateTimeField()

    async_object = AsyncManager()


class Time(AsyncModel, models.Model):
    time = models.TimeField(null=True)

    async_object = AsyncManager()


class SimulationRun(AsyncModel, models.Model):
    start = models.ForeignKey(Time, models.CASCADE, null=True, related_name="+")
    end = models.ForeignKey(Time, models.CASCADE, null=True, related_name="+")
    midpoint = models.TimeField()

    async_object = AsyncManager()


class UUIDPK(AsyncModel, models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4)

    async_object = AsyncManager()


class UUID(AsyncModel, models.Model):
    uuid = models.UUIDField(null=True)
    uuid_fk = models.ForeignKey(UUIDPK, models.CASCADE, null=True)

    async_object = AsyncManager()


class Text(AsyncModel, models.Model):
    name = models.TextField()

    async_object = AsyncManager()


class JSONFieldModel(AsyncModel, models.Model):
    data = models.JSONField(null=True)

    async_object = AsyncManager()

    class Meta:
        required_db_features = {"supports_json_field"}
