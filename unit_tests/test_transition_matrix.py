import myspark.transition_matrix as t


def test_pairs():
    xs = [1, 2, 3, 4, 5]
    pairs = t.pairs(xs)
    assert len(pairs) == len(xs) - 1
    expected = [[1, 2], [2, 3], [3, 4], [4, 5]]
    for p in pairs:
        assert p in expected
