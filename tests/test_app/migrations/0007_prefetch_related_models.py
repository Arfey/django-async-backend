import django.db.models.deletion
from django.db import (
    migrations,
    models,
)

import django_async_backend.db.models.base


class Migration(migrations.Migration):
    dependencies = [
        ("test_app", "0006_totalorderingcompositepkmodel"),
    ]

    operations = [
        migrations.CreateModel(
            name="SelectRelatedAuthorModel",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("name", models.CharField(max_length=255, unique=True)),
            ],
            options={"db_table": "select_related_author_model"},
            bases=(
                django_async_backend.db.models.base.AsyncModelMixin,
                models.Model,
            ),
        ),
        migrations.CreateModel(
            name="SelectRelatedPublisherModel",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("name", models.CharField(max_length=255, unique=True)),
            ],
            options={"db_table": "select_related_publisher_model"},
            bases=(
                django_async_backend.db.models.base.AsyncModelMixin,
                models.Model,
            ),
        ),
        migrations.CreateModel(
            name="SelectRelatedProfileModel",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("bio", models.CharField(max_length=255)),
                (
                    "author",
                    models.OneToOneField(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="profile",
                        to="test_app.selectrelatedauthormodel",
                    ),
                ),
            ],
            options={"db_table": "select_related_profile_model"},
            bases=(
                django_async_backend.db.models.base.AsyncModelMixin,
                models.Model,
            ),
        ),
        migrations.CreateModel(
            name="SelectRelatedBookModel",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("title", models.CharField(max_length=255, unique=True)),
                (
                    "author",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="books",
                        to="test_app.selectrelatedauthormodel",
                    ),
                ),
                (
                    "publisher",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="books",
                        to="test_app.selectrelatedpublishermodel",
                    ),
                ),
            ],
            options={"db_table": "select_related_book_model"},
            bases=(
                django_async_backend.db.models.base.AsyncModelMixin,
                models.Model,
            ),
        ),
    ]
