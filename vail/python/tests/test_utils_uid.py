from concurrent.futures import ThreadPoolExecutor

import vail as hl
from vail.utils import Env


def test_get_uid_spelling_and_shared_counter():
    first = Env.get_uid()
    number = int(first.removeprefix('__uid_'))
    assert Env.get_uid('field') == f'__uid_field{number + 1}'
    assert Env.get_uid('') == f'__uid_{number + 2}'
    assert Env.get_uid() == f'__uid_{number + 3}'


def test_get_uid_is_unique_across_threads():
    with ThreadPoolExecutor(max_workers=4) as executor:
        names = list(executor.map(lambda _: Env.get_uid(), range(1000)))
    assert len(set(names)) == 1000


def test_get_uid_names_work_as_table_fields():
    name = Env.get_uid('field')
    table = hl.range_table(2).annotate(**{name: hl.struct()})
    assert [row[name] for row in table.collect()] == [hl.Struct(), hl.Struct()]
