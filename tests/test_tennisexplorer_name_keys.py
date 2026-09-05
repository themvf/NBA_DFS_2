from ingest.tennis_results_tennisexplorer import _keys_full_name


def test_full_name_keys_support_surname_first_names():
    assert ("wu", "y") in _keys_full_name("Wu Yibing")


def test_full_name_keys_ignore_suffix_as_only_surname():
    assert ("damm", "m") in _keys_full_name("Martin Damm Jr.")


def test_full_name_keys_support_multi_given_and_compound_surnames():
    keys = _keys_full_name("Maria Camila Osorio Serrano")
    assert ("osorio", "c") in keys
    assert ("osorioserrano", "m") in keys
