class AnsibleError(Exception):
    pass


class AnsibleInventoryNotFoundError(AnsibleError):
    def __init__(self, inventory):
        self.inventory = inventory
        self.msg = "Unable to continue, inventory file not found %s" % self.inventory

    def __str__(self):
        return self.msg
