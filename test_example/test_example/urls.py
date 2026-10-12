from django.urls import path

from test_example.views import (
    index,
    stream,
)

urlpatterns = [
    path("", index),
    path("stream", stream),
]
