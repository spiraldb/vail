use arrow_array::{Array, ArrayRef, ListArray, RecordBatch, StringArray};
use std::sync::Arc;
use vail_execution::expressions::{Expr, project_batch};

fn parse(
    lines: Vec<Option<&str>>,
    quote: Option<&str>,
    markers: &[&str],
    expected_fields: usize,
) -> Result<Vec<Option<Vec<Option<String>>>>, String> {
    let batch =
        RecordBatch::try_from_iter([("line", Arc::new(StringArray::from(lines)) as ArrayRef)])
            .unwrap();
    let expression = Expr::ParseCsvLine {
        value: Arc::new(Expr::Column {
            name: "line".into(),
        }),
        delimiter: ",".into(),
        quote: quote.map(str::to_owned),
        missing: markers.iter().map(|v| v.to_string()).collect(),
        expected_fields,
    };
    let result =
        project_batch(&batch, &[("fields".into(), expression)]).map_err(|e| e.to_string())?;
    let lists = result
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    Ok((0..lists.len())
        .map(|row| {
            if lists.is_null(row) {
                None
            } else {
                let values = lists.value(row);
                Some(
                    values
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .iter()
                        .map(|v| v.map(str::to_owned))
                        .collect(),
                )
            }
        })
        .collect())
}

#[test]
fn csv_projection_preserves_quotes_missing_rows_fields_and_empty_batches() {
    assert_eq!(
        parse(
            vec![Some("a,\"b,c\","), None, Some("x,NA,NA")],
            Some("\""),
            &["", "NA", "NA"],
            3
        )
        .unwrap(),
        vec![
            Some(vec![Some("a".into()), Some("b,c".into()), None]),
            None,
            Some(vec![Some("x".into()), None, None])
        ]
    );
    assert_eq!(
        parse(vec![Some("a,\"b\"")], None, &[], 2).unwrap(),
        vec![Some(vec![Some("a".into()), Some("\"b\"".into())])]
    );
    assert!(parse(vec![], Some("\""), &[], 3).unwrap().is_empty());
}

#[test]
fn csv_projection_rejects_malformed_quotes_extra_records_and_wrong_counts() {
    for line in ["a,\"b", "a,\"b\"\"c\"", "a\"b,c", "a,b\nc,d"] {
        assert!(
            parse(vec![Some(line)], Some("\""), &[], 0).is_err(),
            "{line:?}"
        );
    }
    assert!(
        parse(vec![Some("a,b")], Some("\""), &[], 3)
            .unwrap_err()
            .contains("field count")
    );
    assert!(
        parse(vec![Some("")], Some("\""), &[], 1)
            .unwrap_err()
            .contains("empty CSV")
    );
}

#[test]
fn unrestricted_csv_empty_lines_are_single_fields_with_configured_missingness() {
    for line in ["", "\n", "\r\n"] {
        assert_eq!(
            parse(vec![Some(line)], Some("\""), &[], 0).unwrap(),
            vec![Some(vec![Some("".into())])]
        );
        assert_eq!(
            parse(vec![Some(line)], Some("\""), &[""], 0).unwrap(),
            vec![Some(vec![None])]
        );
    }
}
