#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from importlib import metadata

sys.path.insert(0, os.path.abspath("../../"))
sys.path.insert(0, os.path.abspath("./"))

from theme_config import *  # noqa: E402,F401,F403

master_doc = "index"
project = "django-async-backend"
copyright = "2025, Arfey"
author = "Arfey"
description = (
    "True async Django ORM and PostgreSQL backend with connection "
    "pooling and async transactions"
)

try:
    release = metadata.version("django-async-backend")
except metadata.PackageNotFoundError:  # pragma: no cover - docs-only path
    release = "unknown"

version = release

html_static_path = ["./_static"]
html_css_files = ["custom.css"]
html_baseurl = "https://django-async-backend.readthedocs.io/"
sitemap_url_scheme = "en/stable/{link}"
html_title = (
    f"{project} <small><b style='color: var(--color-brand-primary)'>"
    f"{{{release}}}</b></small>"
)

extensions = [
    "myst_parser",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.extlinks",
    "sphinx.ext.intersphinx",
    "sphinx_copybutton",
    "sphinx_design",
    "sphinx_issues",
    "sphinx_sitemap",
    "sphinxext.opengraph",
]

source_suffix = {
    ".md": "markdown",
    ".rst": "restructuredtext",
}

myst_enable_extensions = [
    "attrs_inline",
    "colon_fence",
    "deflist",
    "fieldlist",
    "linkify",
    "substitution",
]
# Generate anchors for ## and ### headings so `file.md#section` links work.
myst_heading_anchors = 3

# Cross-document links use MyST anchors (`../handbook/orm.md#managers`)
# rather than autosectionlabel refs, but keeping labels namespaced by
# document avoids duplicate-label warnings between pages.
autosectionlabel_prefix_document = True
autosectionlabel_maxdepth = 3

extlinks = {
    # `pypi` is intentionally absent: sphinx_issues already registers that
    # role, and redefining it here emits an "already registered" warning.
    "djangodoc": ("https://docs.djangoproject.com/en/stable/%s", "%s"),
}

issues_github_path = "Arfey/django-async-backend"

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "django": (
        "https://docs.djangoproject.com/en/stable/",
        "https://docs.djangoproject.com/en/stable/_objects/",
    ),
    "psycopg": ("https://www.psycopg.org/psycopg3/docs/", None),
}

htmlhelp_basename = "djangoasyncbackenddoc"
latex_elements: dict[str, str] = {}
latex_documents = [
    (
        master_doc,
        "django-async-backend.tex",
        "django-async-backend Documentation",
        author,
        "manual",
    ),
]
man_pages = [
    (
        master_doc,
        "django-async-backend",
        "django-async-backend Documentation",
        [author],
        1,
    )
]
