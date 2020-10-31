def sort_dict(d):
    """
    This helper sorts a dict by key. It's useful when have a hard-coded expected variable and we want to
    execute a function that returns a dict, but we're not sure how the keys in the dict returned by that function will
    be sorted. By running this sort_dict on both values, we can ensure that comparing them via assertEqual won't
    fail simply because their keys are in different orders.

    :param d: dict
    :return: dict sorted by key
    """
    return dict(sorted(d.items()))
