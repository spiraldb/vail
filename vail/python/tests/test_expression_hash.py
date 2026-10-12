import vail as hl


def test_expression_hash_is_object_identity():
    for make in (lambda: hl.array([1, 2]), lambda: hl.struct(x=1), lambda: hl.literal(1)):
        left, right = make(), make()
        assert hash(left) == object.__hash__(left)
        assert hash(right) == object.__hash__(right)
        entries = {left: 'left', right: 'right'}
        assert len(entries) == 2
        assert entries[left] == 'left'
        assert entries[right] == 'right'
        entries[left] = 'updated'
        assert len(entries) == 2
        assert entries[left] == 'updated'
        assert hl.eval(left == right) is True


def test_hashing_does_not_execute_expression():
    expression = hl.case().when(False, 1).or_error('must not execute')
    entries = {expression: 7}
    assert hash(expression) == object.__hash__(expression)
    assert entries[expression] == 7
