# encapsulate the GOV.UK register model, with digital land specification extensions
# we expect to merge this back into https://pypi.org/project/openregister/


class Register:
    register = "unknown"
    key = None
    fieldnames = []
    store = None

    def __init__(self, register=None, key=None, fieldnames=[], store=None):
        if register:
            self.register = register
        if key:
            self.key = key
        if not self.key:
            self.key = self.register
        if store:
            self.store = store

    def __getitem__(self, value):
        return self.store.get_entries(self.register, self.key, value)
