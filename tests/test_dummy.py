import clmunch.utils


def test_dummy() -> None:
    assert True


def test_unique_substrings() -> None:
    assert clmunch.utils.unique_substrings(["a", "b", "c"]) == ["a", "b", "c"]
    assert clmunch.utils.unique_substrings(["a", "b", "ab"]) == ["a", "b", "ab"]
    assert clmunch.utils.unique_substrings(["a", "b", "ba"]) == ["a", "b", "ba"]
    assert clmunch.utils.unique_substrings(["a", "b", "ba", "ab"]) == ["a", "b", "ba", "ab"]

    assert clmunch.utils.unique_substrings(["aa", "ba"]) == ["a", "b"]
    assert clmunch.utils.unique_substrings(["a", "aa"]) == ["a", "aa"]
    assert clmunch.utils.unique_substrings(["aa", "a"]) == ["aa", "a"]
    assert clmunch.utils.unique_substrings(["a123", "b123", "c123"]) == ["a", "b", "c"]
