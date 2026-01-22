from django.utils.decorators import method_decorator


def method_decorators(decorator, names):
    """
    Applies a given decorator to multiple methods of a class.

    Example:
        @method_decorators(login_required, ['get', 'post'])
        class MyView(View):
            ...
    """

    def inner_decorator(cls):
        for name in names:
            cls = method_decorator(decorator, name)(cls)

        return cls

    return inner_decorator
