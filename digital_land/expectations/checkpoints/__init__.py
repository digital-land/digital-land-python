checkpoints = {}


def checkpoint(name):
    def decorator(klass):
        if name in checkpoints.keys():
            checkpoints[name].append(klass)
        else:
            checkpoints[name] = [klass]
        return klass

    return decorator
