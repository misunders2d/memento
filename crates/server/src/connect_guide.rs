//! Existing static private bootstrap text; never interpolate request Host/Origin.
pub fn connect_guide() -> &'static str {
    include_str!("connect_guide.txt")
}
