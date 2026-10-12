import hail as hl
import json
hl.init(master='local[1]', quiet=True, log='/private/tmp/vail-string-oracle-hail.log')
cases = {
 'length_supplementary': hl.str('a😀b').length(),
 'reverse_supplementary': hl.str('a😀b').reverse(),
 'join_missing': hl.str(',').join(hl.literal(['a', None, 'b'], hl.tarray(hl.tstr))),
 'repeat_missing_zero': hl.missing(hl.tstr) * 0,
 'repeat_negative': hl.str('a') * -1,
 'find_empty': hl.str('abc').find(''),
 'find_supplementary': hl.str('😀abc').find('b'),
 'int_spaces': hl.parse_int32(' 1 '),
 'int_plus': hl.parse_int32('+01'),
 'int_unicode': hl.parse_int32('١'),
 'float_hex': hl.parse_float64('0x1.8p1'),
 'float_suffix': hl.parse_float64('1.5f'),
 'float_space': hl.parse_float64(' 1.5 '),
 'float_signed_nan': hl.parse_float64('-nan'),
 'split_empty': hl.str('').split(','),
 'split_trailing': hl.str('a,,').split(','),
 'lower': hl.str('ΟΣ İ').lower(),
}
output = {}
for name, expr in cases.items():
    try: output[name] = hl.eval(expr)
    except Exception as exc: output[name] = {'error': type(exc).__name__}
print(json.dumps(output, ensure_ascii=True, allow_nan=True))
hl.stop()
