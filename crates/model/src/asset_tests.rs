use super::*;
use std::{
    fs,
    os::unix::fs::DirBuilderExt,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    root: PathBuf,
    model: PathBuf,
    graphs: PathBuf,
    graph_sha: [String; 3],
    weight_sha: [String; 3],
    weights: [Vec<u8>; 3],
}
impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "memento-asset-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::DirBuilder::new().mode(0o700).create(&root).unwrap();
        let model = root.join("weights");
        let graphs = root.join("graphs");
        fs::create_dir_all(model.join("onnx")).unwrap();
        fs::create_dir(&graphs).unwrap();
        let graph_sha = std::array::from_fn(|i| {
            let bytes = format!("Synthetic graph {i}").into_bytes();
            fs::write(graphs.join(format!("{}.onnx", GRAPH_FILES[i].0)), &bytes).unwrap();
            format!("{:x}", Sha256::digest(bytes))
        });
        let weights = std::array::from_fn(|i| {
            (0..768)
                .flat_map(|n| ((i * 768 + n) as f32 / 768.0).to_le_bytes())
                .collect::<Vec<_>>()
        });
        let weight_sha = std::array::from_fn(|i| {
            fs::write(
                model
                    .join("onnx")
                    .join(format!("{}.onnx_data", GRAPH_FILES[i].0)),
                &weights[i],
            )
            .unwrap();
            format!("{:x}", Sha256::digest(&weights[i]))
        });
        Self {
            root,
            model,
            graphs,
            graph_sha,
            weight_sha,
            weights,
        }
    }
    fn load(&self) -> Result<Assets> {
        let specs = std::array::from_fn(|i| {
            (
                GRAPH_FILES[i].0,
                self.graph_sha[i].as_str(),
                self.weight_sha[i].as_str(),
                self.weights[i].len() as u64,
            )
        });
        Assets::load_files(&self.model, self.graphs.clone(), &specs)
    }
    fn path(&self, index: usize) -> PathBuf {
        if index < 3 {
            self.graphs.join(format!("{}.onnx", GRAPH_FILES[index].0))
        } else {
            self.model
                .join("onnx")
                .join(format!("{}.onnx_data", GRAPH_FILES[index - 3].0))
        }
    }
    fn live_maps(&self) -> usize {
        fs::read_to_string("/proc/self/maps")
            .unwrap()
            .lines()
            .filter(|line| line.contains(self.root.to_str().unwrap()))
            .count()
    }
    fn open_files(&self) -> usize {
        fs::read_dir("/proc/self/fd")
            .unwrap()
            .filter_map(|e| fs::read_link(e.unwrap().path()).ok())
            .filter(|p| p.starts_with(&self.root))
            .count()
    }
    fn clean(&self) {
        assert_eq!(self.live_maps(), 0);
        assert_eq!(self.open_files(), 0);
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).unwrap();
    }
}

#[test]
fn parallel_assets_preserve_text_vision_audio_order_bytes_and_mapping_lifetime() {
    let fixture = Fixture::new();
    let assets = fixture.load().unwrap();
    assert_eq!(assets.graph_dir, fixture.graphs);
    assert_eq!(assets.mappings.len(), 3);
    for (mapping, expected) in assets.mappings.iter().zip(&fixture.weights) {
        assert_eq!(&mapping.map[..], expected);
    }
    assert_eq!(fixture.live_maps(), 3);
    assert_eq!(fixture.open_files(), 3);
    drop(assets);
    fixture.clean();
}

#[test]
fn every_missing_graph_or_weight_fails_and_joins_without_leaked_maps_or_files() {
    for index in 0..6 {
        let fixture = Fixture::new();
        fs::remove_file(fixture.path(index)).unwrap();
        assert!(
            matches!(fixture.load(),Err(Error::Io(e)) if e.kind()==std::io::ErrorKind::NotFound)
        );
        fixture.clean();
    }
}

#[test]
fn every_same_length_corrupt_graph_or_weight_fails_checksum_and_cleans_up() {
    for index in 0..6 {
        let fixture = Fixture::new();
        let path = fixture.path(index);
        let mut bytes = fs::read(&path).unwrap();
        bytes[0] ^= 0xff;
        fs::write(path, bytes).unwrap();
        assert!(
            matches!(fixture.load(),Err(Error::InvalidAssets(s)) if s=="asset checksum mismatch")
        );
        fixture.clean();
    }
}

#[test]
fn every_wrong_weight_length_fails_before_mapping_and_cleans_up() {
    for index in 3..6 {
        let fixture = Fixture::new();
        fs::write(fixture.path(index), [0_u8; 4]).unwrap();
        assert!(
            matches!(fixture.load(),Err(Error::InvalidAssets(s)) if s=="weight file size mismatch")
        );
        fixture.clean();
    }
}
