from unittest import TestCase

from django_async_backend.utils.decorators import method_decorators


def test_decorator(func):
    def wrapper(*args, **kwargs):
        res = func(*args, **kwargs)
        return ("decorated", res)

    return wrapper


class MethodDecoratorsTests(TestCase):

    def test_decorate_single_method(self):
        class DummyView:
            def foo(self):
                return "foo"

            def bar(self, *args, **kwargs):
                return (args, kwargs)

        DecoratedDummyView = method_decorators(test_decorator, ["foo"])(
            DummyView
        )
        view = DecoratedDummyView()

        self.assertIsInstance(view, DummyView)
        self.assertEqual(view.foo(), ("decorated", "foo"))
        self.assertEqual(view.bar(1, some=2), ((1,), {"some": 2}))

    def test_decorate_list_of_methods(self):
        class DummyView:
            def foo(self):
                return "foo"

            def bar(self, *args, **kwargs):
                return (args, kwargs)

        DecoratedDummyView = method_decorators(test_decorator, ["foo", "bar"])(
            DummyView
        )
        view = DecoratedDummyView()

        self.assertIsInstance(view, DummyView)
        self.assertEqual(view.foo(), ("decorated", "foo"))
        self.assertEqual(
            view.bar(1, some=2), ("decorated", ((1,), {"some": 2}))
        )

    def test_without_decoration(self):
        class EmptyView:
            pass

        DecoratedView = method_decorators(test_decorator, [])(EmptyView)
        self.assertIsInstance(DecoratedView(), EmptyView)
