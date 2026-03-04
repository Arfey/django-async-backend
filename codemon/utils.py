import os
import pathlib

import libcst as cst
import requests
import yaml
from pydantic import (
    BaseModel,
    model_validator,
)

parent = pathlib.Path(__file__).parent


class CallFun(BaseModel):
    attr: str | None = None
    value: str | None = None

    @model_validator(mode="after")
    def validate_at_least_one_provided(self):
        if self.attr is None and self.value is None:
            raise ValueError(
                'At least one of "attr" or "value" must be provided.'
            )
        return self


class RenameAttr(BaseModel):
    attr: str | None = None
    value: str | None = None
    name: str | None = None


class RenameFun(BaseModel):
    func: RenameAttr | None = None


class ContextManagers(BaseModel):
    asname: str
    to_async: bool = False


class ForStatement(BaseModel):
    target: str
    to_async: bool = False


class Call(BaseModel):
    to_await: bool = False
    func: CallFun | None = None
    name: str | None = None
    rename: RenameFun | None = None
    replace_raw: str | None = None

    @model_validator(mode="after")
    def validate_either_attr_or_name(self):
        if (self.func is None and self.name is None) or (
            self.func is not None and self.name is not None
        ):
            raise ValueError(
                'Exactly one of "fun" or "name" must be set, not both or neither.'
            )
        return self


class ReturnBlock(BaseModel):
    replace_raw: str | None = None
    call: Call | None = None


class AssignTarget(BaseModel):
    name: str


class Assign(BaseModel):
    remove: bool = False
    target: AssignTarget


class Method(BaseModel):
    remove: bool = False
    add_raw_top: list[str] = None
    to_async: bool = False
    rename: str | None = None
    calls: list[Call] | None = None
    for_statements: list[ForStatement] | None = None
    context_managers: list[ContextManagers] | None = None
    return_blocks: list[ReturnBlock] | None = None


class Class(BaseModel):
    rename: str | None = None
    methods: dict[str, Method]
    assigns: list[Assign] | None = None


class Function(BaseModel):
    rename: str | None = None
    add_raw_top: list[str] = None
    to_async: bool = False
    calls: list[Call] | None = None
    for_statements: list[ForStatement] | None = None
    return_blocks: list[ReturnBlock] | None = None
    remove: bool = False


class ImportAlias(BaseModel):
    name: str | None = None
    remove: bool = False


class Module(BaseModel):
    classes: dict[str, Class] | None = None
    new_imports: list[str] | None = None
    import_aliases: list[ImportAlias] | None = None
    add_raw_bottom: list[str] = None
    functions: dict[str, Function] | None = None


class Config(BaseModel):
    pathname: str
    module: Module


def load_file(*, config: Config, version: str):
    response = requests.get(
        f"https://raw.githubusercontent.com/django/django/refs/tags/{version}/django/{config.pathname}"
    )
    response.raise_for_status()

    target = parent.parent / "django_async_backend" / config.pathname

    os.makedirs(os.path.dirname(target), exist_ok=True)

    with open(target, mode="w") as file:
        file.write(response.text)


def get_ast(config: Config):
    with open(
        parent.parent / "django_async_backend" / config.pathname, "r"
    ) as file:
        source = file.read()

    module = cst.parse_module(source)
    wrapper = cst.metadata.MetadataWrapper(module)

    return wrapper


def write_ast(ast, config, version):
    with open(
        parent.parent / "django_async_backend" / config.pathname, "w"
    ) as f:
        f.write(
            f"# This file was generated automatically. Do not modify it manually. (based on django {version})\n"
        )
        f.write(ast.code)


def get_configs():
    return [
        os.path.abspath(f)
        for f in (parent / "config").glob("*")
        if os.path.isfile(f)
    ]


def load_config(path) -> Config:
    with open(path, mode="r") as f:
        data = yaml.safe_load(f)

    return Config.model_validate(data)
