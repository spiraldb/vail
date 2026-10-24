//! Read-only encoding diagnostic; never used by query execution or tests.
use futures::TryStreamExt;
use vortex::{VortexSessionDefault, session::VortexSession};
use vortex::file::OpenOptionsSessionExt;
use vortex::expr::{get_item, root};
use vortex::array::{VortexSessionExecute, ArrayRef};
use vortex::array::arrays::{ListViewArray, listview::ListViewArraySlotsExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let session = VortexSession::default();
    let path = std::env::args().nth(1).expect("data Vortex file path");
    let file = session.open_options().open_path(path).await?;
    eprintln!("file dtype: {}", file.dtype());
    let column = std::env::args().nth(2).unwrap_or("__cells".into());
    let mut stream = file.scan()?.with_projection(get_item(column, root()).bind(file.dtype())?)
        .with_row_range(0..1).into_array_stream()?;
    let Some(array) = stream.try_next().await? else { return Ok(()) };
    eprintln!("raw list:\n{}", array.display_tree_encodings_only());
    let expr = vortex::array::scalar_fn::fns::list_field::list_field(
        root(), array.dtype(), &["value".into(), "DP".into()],
    )?;
    let mut ctx = session.create_execution_ctx();
    let list = array.apply(&expr)?.execute::<ListViewArray>(&mut ctx)?;
    let mut child = list.elements().clone();
    eprintln!("extracted element:\n{}", child.display_tree_encodings_only());
    for step in 0..5 {
        let next: ArrayRef = child.execute(&mut ctx)?;
        eprintln!("step {step}:\n{}", next.display_tree_encodings_only());
        child = next;
    }
    Ok(())
}
