from django.db import models
from django.db.models.lookups import IsNull

from django_async_backend.db.models.base import AsyncModel
from django_async_backend.db.models.manager import AsyncManager


class Alarm(AsyncModel, models.Model):
    desc = models.CharField(max_length=100)
    time = models.TimeField()

    async_object = AsyncManager()

    def __str__(self):
        return "%s (%s)" % (self.time, self.desc)


class Author(AsyncModel, models.Model):
    name = models.CharField(max_length=100)
    alias = models.CharField(max_length=50, null=True, blank=True)  # noqa: DJ001
    bio = models.TextField(null=True)  # noqa: DJ001

    async_object = AsyncManager()

    class Meta:
        ordering = ("name",)


class Article(AsyncModel, models.Model):
    headline = models.CharField(max_length=100)
    pub_date = models.DateTimeField()
    author = models.ForeignKey(Author, models.SET_NULL, blank=True, null=True)
    slug = models.SlugField(unique=True, blank=True, null=True)

    async_object = AsyncManager()

    class Meta:
        ordering = ("-pub_date", "headline")

    def __str__(self):
        return self.headline


class Tag(AsyncModel, models.Model):
    articles = models.ManyToManyField(Article)
    name = models.CharField(max_length=100)

    async_object = AsyncManager()

    class Meta:
        ordering = ("name",)


class NulledTextField(models.TextField):
    def get_prep_value(self, value):
        return None if value == "" else value


@NulledTextField.register_lookup
class NulledTransform(models.Transform):
    lookup_name = "nulled"
    template = "NULL"


@NulledTextField.register_lookup
class IsNullWithNoneAsRHS(IsNull):
    lookup_name = "isnull_none_rhs"
    can_use_none_as_rhs = True


class Season(AsyncModel, models.Model):
    year = models.PositiveSmallIntegerField()
    gt = models.IntegerField(null=True, blank=True)
    nulled_text_field = NulledTextField(null=True)

    async_object = AsyncManager()

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["year"], name="season_year_unique"),
        ]

    def __str__(self):
        return str(self.year)


class Game(AsyncModel, models.Model):
    season = models.ForeignKey(Season, models.CASCADE, related_name="games")
    home = models.CharField(max_length=100)
    away = models.CharField(max_length=100)

    async_object = AsyncManager()


class Player(AsyncModel, models.Model):
    name = models.CharField(max_length=100)
    games = models.ManyToManyField(Game, related_name="players")

    async_object = AsyncManager()


class Product(AsyncModel, models.Model):
    name = models.CharField(max_length=80)
    qty_target = models.DecimalField(max_digits=6, decimal_places=2)

    async_object = AsyncManager()


class Stock(AsyncModel, models.Model):
    product = models.ForeignKey(Product, models.CASCADE)
    short = models.BooleanField(default=False)
    qty_available = models.DecimalField(max_digits=6, decimal_places=2)

    async_object = AsyncManager()


class Freebie(AsyncModel, models.Model):
    gift_product = models.ForeignKey(Product, models.CASCADE)
    stock_id = models.IntegerField(blank=True, null=True)

    stock = models.ForeignObject(
        Stock,
        from_fields=["stock_id", "gift_product"],
        to_fields=["id", "product"],
        on_delete=models.CASCADE,
    )

    async_object = AsyncManager()
