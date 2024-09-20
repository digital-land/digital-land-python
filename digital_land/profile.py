import cProfile
import pstats
from functools import wraps

# https://docs.python.org/3/library/profile.html
# https://docs.python.org/3/library/profile.html#the-stats-class

def profile(sort_by='tottime', prof_file=None, lines_to_print=None):
    def inner(func):
        @wraps(func)
        def wrapper_profile(*args, **kwargs):
            _prof_file = prof_file or func.__name__ + '.prof'
            prof = cProfile.Profile()
            prof.enable()
            retval = func(*args, **kwargs)
            prof.disable()
            prof.dump_stats(_prof_file)

            with open(_prof_file, 'w') as f:
                ps = pstats.Stats(prof, stream=f)
                if isinstance(sort_by, (tuple, list)):
                    ps.sort_stats(*sort_by)
                else:
                    ps.sort_stats(sort_by)
                ps.print_stats(lines_to_print)
            return retval

        return wrapper_profile

    return inner