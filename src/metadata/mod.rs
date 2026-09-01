mod decode;
mod model;

pub use model::ObjectMetadata;

pub(crate) use decode::{MetadataDecodeFailure, decode_record};
pub(crate) use model::ObjectWorkItem;
