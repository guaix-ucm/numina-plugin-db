

_event_names = [
    'on_ingest_raw_fits'
]


class EventManager(object):
    def __init__(self, name):
        self.name = name
        self.events = []

    def register(self, callable):
        self.events.append(callable)

    def __call__(self, *args, **kwargs):
        result = []
        for callable in self.events:
            res = callable(*args, **kwargs)
            result.append(res)
        return result


def _create_managers():
    managers = {}
    for event in _event_names:
        managers[event] = EventManager(event)
    return managers


_managers = _create_managers()


def manage(name, callable):
    global _managers
    manager = _managers[name]
    manager.register(callable)
    return callable


def call_event(name, *args, **kwargs):
    global _managers
    manager = _managers[name]
    return manager(*args, **kwargs)


class on_event(object):
    def __init__(self, name):
        self.name = name

    def __call__(self, fn):
        manage(self.name, fn)
        return fn

