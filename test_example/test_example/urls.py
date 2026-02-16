from django.urls import path

from test_example.views import index

urlpatterns = [
    path("", index),
]
